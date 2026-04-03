"""Excel parser for private credit loan tapes.

Follows the same pattern as deal_parser.py: flexible column mapping,
row validation, UploadIssue logging, per-fund replace semantics.
"""

import math
import re
import uuid
from collections import defaultdict
from datetime import date

import pandas as pd

from models import (
    CreditLoan,
    CreditLoanSnapshot,
    Firm,
    TeamFirmAccess,
    UploadIssue,
    db,
)

# ---------------------------------------------------------------------------
# Column mapping — flexible header recognition
# ---------------------------------------------------------------------------

CREDIT_COLUMN_MAP = {
    # Company / identification
    "company": "company_name",
    "company name": "company_name",
    "borrower": "company_name",
    "borrower name": "company_name",
    "portfolio company": "company_name",
    "issuer": "company_name",
    # Fund
    "fund": "fund_name",
    "fund name": "fund_name",
    "pcof fund": "fund_name",
    "pcof": "fund_name",
    "vehicle": "fund_name",
    "vintage": "vintage_year",
    "vintage year": "vintage_year",
    "fund vintage": "vintage_year",
    # Dates
    "close date": "close_date",
    "deal close date": "close_date",
    "investment date": "close_date",
    "origination date": "close_date",
    "exit date": "exit_date",
    "payoff date": "exit_date",
    "repayment date": "exit_date",
    "as of date": "as_of_date",
    "as-of date": "as_of_date",
    "report date": "as_of_date",
    "as of": "as_of_date",
    # Status
    "status": "status",
    "current status": "status",
    "realization status": "status",
    "default status": "default_status",
    "credit status": "default_status",
    "performance status": "default_status",
    # Structure & terms
    "instrument": "instrument",
    "instrument type": "instrument",
    "loan type": "instrument",
    "tranche": "tranche",
    "lien position": "tranche",
    "security": "security_type",
    "security type": "security_type",
    "collateral type": "security_type",
    "issue size": "issue_size",
    "facility size": "issue_size",
    "total commitment": "issue_size",
    "hold size": "hold_size",
    "pcof hold": "hold_size",
    "pcof hold size": "hold_size",
    "our commitment": "hold_size",
    "par amount": "hold_size",
    "coupon": "coupon_rate",
    "coupon rate": "coupon_rate",
    "interest rate": "coupon_rate",
    "all-in rate": "coupon_rate",
    "spread": "spread_bps",
    "spread bps": "spread_bps",
    "credit spread": "spread_bps",
    "floor": "floor_rate",
    "rate floor": "floor_rate",
    "floor rate": "floor_rate",
    "oid": "fee_oid",
    "original issue discount": "fee_oid",
    "upfront fee": "fee_upfront",
    "exit fee": "fee_exit",
    "maturity": "maturity_date",
    "maturity date": "maturity_date",
    "fixed or floating": "fixed_or_floating",
    "rate type": "fixed_or_floating",
    "fixed/floating": "fixed_or_floating",
    "reference rate": "reference_rate",
    "base rate": "reference_rate",
    "index": "reference_rate",
    "pik": "pik_toggle",
    "pik toggle": "pik_toggle",
    "pik rate": "pik_rate",
    "call protection": "call_protection_months",
    "non-call period": "call_protection_months",
    "make whole": "make_whole_premium",
    "make-whole premium": "make_whole_premium",
    "amortization": "amortization_type",
    "amortization type": "amortization_type",
    "repayment type": "amortization_type",
    "payment frequency": "payment_frequency",
    # Credit metrics
    "entry ltv": "entry_ltv",
    "ltv at entry": "entry_ltv",
    "initial ltv": "entry_ltv",
    "current ltv": "current_ltv",
    "ltv": "current_ltv",
    "entry revenue": "entry_revenue",
    "revenue at entry": "entry_revenue",
    "entry ebitda": "entry_ebitda",
    "ebitda at entry": "entry_ebitda",
    "current revenue": "current_revenue",
    "current ebitda": "current_ebitda",
    "interest coverage": "interest_coverage_ratio",
    "interest coverage ratio": "interest_coverage_ratio",
    "icr": "interest_coverage_ratio",
    "dscr": "dscr",
    "debt service coverage": "dscr",
    "debt service coverage ratio": "dscr",
    "credit rating": "internal_credit_rating",
    "internal rating": "internal_credit_rating",
    "rating": "internal_credit_rating",
    "covenant type": "covenant_type",
    "covenants": "covenant_type",
    "covenant compliant": "covenant_compliant",
    "covenant compliance": "covenant_compliant",
    "in compliance": "covenant_compliant",
    # Performance
    "gross irr": "gross_irr",
    "irr": "gross_irr",
    "moic": "moic",
    "realized value": "realized_value",
    "realized": "realized_value",
    "unrealized value": "unrealized_value",
    "unrealized": "unrealized_value",
    "cumulative interest income": "cumulative_interest_income",
    "interest income": "cumulative_interest_income",
    "total interest": "cumulative_interest_income",
    "cumulative fee income": "cumulative_fee_income",
    "fee income": "cumulative_fee_income",
    "total fees": "cumulative_fee_income",
    "fair value": "fair_value",
    "mark to market": "fair_value",
    "market value": "fair_value",
    "ytm": "yield_to_maturity",
    "yield to maturity": "yield_to_maturity",
    "recovery rate": "recovery_rate",
    "original par": "original_par",
    "original face": "original_par",
    "current outstanding": "current_outstanding",
    "outstanding balance": "current_outstanding",
    "current par": "current_outstanding",
    "accrued interest": "accrued_interest",
    # Classification
    "sector": "sector",
    "industry": "sector",
    "geography": "geography",
    "region": "geography",
    "country": "geography",
    "sponsor": "sponsor",
    "pe sponsor": "sponsor",
    "financial sponsor": "sponsor",
    # Currency
    "currency": "currency",
    "loan currency": "currency",
    # Firm (for multi-firm uploads)
    "firm": "firm_name",
    "firm name": "firm_name",
    "manager": "firm_name",
}

# Columns that trigger credit detection (need 2+ present)
CREDIT_TRIGGER_COLUMNS = {"coupon_rate", "spread_bps", "entry_ltv", "current_ltv", "maturity_date", "tranche", "pik_toggle"}

# Columns that trigger equity detection
EQUITY_TRIGGER_COLUMNS = {"equity_invested", "entry_enterprise_value", "ownership_pct"}

REQUIRED_FIELDS = {"company_name", "fund_name", "close_date"}

DATE_FIELDS = {"close_date", "exit_date", "as_of_date", "maturity_date"}
FLOAT_FIELDS = {
    "issue_size", "hold_size", "coupon_rate", "floor_rate",
    "fee_oid", "fee_upfront", "fee_exit", "make_whole_premium",
    "entry_ltv", "current_ltv", "entry_revenue", "entry_ebitda",
    "current_revenue", "current_ebitda", "interest_coverage_ratio", "dscr",
    "gross_irr", "moic", "realized_value", "unrealized_value",
    "cumulative_interest_income", "cumulative_fee_income",
    "fair_value", "yield_to_maturity", "recovery_rate",
    "original_par", "current_outstanding", "accrued_interest",
    "pik_rate",
}
INT_FIELDS = {"vintage_year", "spread_bps", "call_protection_months", "internal_credit_rating"}
BOOL_FIELDS = {"pik_toggle", "covenant_compliant"}


def _clean_str(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    return str(val).strip() or None


def _clean_float(val):
    if val is None:
        return None
    if isinstance(val, str):
        val = re.sub(r"[,$%xX]", "", val.strip())
        if not val:
            return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _clean_int(val):
    f = _clean_float(val)
    if f is None:
        return None
    return int(round(f))


def _clean_bool(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("true", "yes", "y", "1"):
        return True
    if s in ("false", "no", "n", "0"):
        return False
    return None


def _clean_date(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, date):
        return val
    if hasattr(val, "date"):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def _normalize_ltv(val):
    """LTV auto-normalization: values > 1.0 are assumed to be percentages."""
    if val is None:
        return None
    if val > 1.0:
        return val / 100.0
    if val < 0:
        return None
    return val


# ---------------------------------------------------------------------------
# Auto-detect: is this file credit or equity?
# ---------------------------------------------------------------------------


def detect_asset_class(headers):
    """Detect whether an Excel file contains credit or equity data.

    Returns 'credit', 'equity', 'ambiguous', or 'unknown'.
    """
    normalized = set()
    for h in headers:
        key = str(h).strip().lower()
        mapped = CREDIT_COLUMN_MAP.get(key)
        if mapped:
            normalized.add(mapped)

    credit_count = len(normalized & CREDIT_TRIGGER_COLUMNS)
    equity_count = len(normalized & EQUITY_TRIGGER_COLUMNS)

    if credit_count >= 2 and equity_count == 0:
        return "credit"
    if equity_count >= 1 and credit_count < 2:
        return "equity"
    if credit_count >= 2 and equity_count >= 1:
        return "ambiguous"
    return "unknown"


# ---------------------------------------------------------------------------
# Main parse function
# ---------------------------------------------------------------------------


def parse_credit_loan_tape(
    file_stream,
    firm_name=None,
    firm_id=None,
    team_id=None,
    issue_report_id=None,
):
    """Parse an Excel file containing a credit loan tape.

    Returns dict: {loans: int, funds: set, warnings: int, batch_id: str, issues: list}
    """
    batch_id = str(uuid.uuid4())[:12]
    if issue_report_id is None:
        issue_report_id = batch_id

    issues = []

    def _log_issue(row_num, severity, message, payload=None):
        issues.append({"row": row_num, "severity": severity, "message": message})
        try:
            db.session.add(UploadIssue(
                issue_report_id=issue_report_id,
                row_number=row_num,
                severity=severity,
                message=message,
                payload=str(payload)[:500] if payload else None,
                firm_id=firm_id,
                team_id=team_id,
            ))
        except Exception:
            pass

    # Read Excel
    try:
        xls = pd.ExcelFile(file_stream)
    except Exception as e:
        raise ValueError(f"Could not read Excel file: {e}")

    # Find the main loans sheet
    sheet_name = None
    for name in xls.sheet_names:
        nl = name.lower().strip()
        if nl in ("loans", "credit loans", "loan tape", "positions", "portfolio"):
            sheet_name = name
            break
    if sheet_name is None:
        sheet_name = xls.sheet_names[0]

    df = pd.read_excel(xls, sheet_name=sheet_name)
    if df.empty:
        raise ValueError("Loan sheet is empty")

    # Map columns
    col_map = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key in CREDIT_COLUMN_MAP:
            col_map[col] = CREDIT_COLUMN_MAP[key]

    df = df.rename(columns=col_map)

    # Check required columns
    missing = REQUIRED_FIELDS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    # Resolve firm
    resolved_firm_id = firm_id
    resolved_firm_name = firm_name
    if resolved_firm_id is None and "firm_name" in df.columns:
        first_firm = _clean_str(df["firm_name"].iloc[0]) if len(df) > 0 else None
        if first_firm:
            resolved_firm_name = first_firm

    if resolved_firm_id is None and resolved_firm_name:
        firm = Firm.query.filter_by(name=resolved_firm_name).first()
        if firm:
            resolved_firm_id = firm.id
        else:
            firm = Firm(name=resolved_firm_name, base_currency="USD")
            db.session.add(firm)
            db.session.flush()
            resolved_firm_id = firm.id
            if team_id:
                db.session.add(TeamFirmAccess(team_id=team_id, firm_id=resolved_firm_id))

    # Per-fund replace: delete existing loans for funds in this upload
    fund_names_in_upload = set()
    for _, row in df.iterrows():
        fn = _clean_str(row.get("fund_name"))
        if fn:
            fund_names_in_upload.add(fn)

    if fund_names_in_upload and resolved_firm_id:
        # Count existing loans for reconciliation warning
        for fn in fund_names_in_upload:
            existing_count = CreditLoan.query.filter_by(
                firm_id=resolved_firm_id, fund_name=fn
            ).count()
            new_count = sum(1 for _, r in df.iterrows() if _clean_str(r.get("fund_name")) == fn)
            if existing_count > 0 and new_count != existing_count:
                _log_issue(0, "warning",
                           f"Fund '{fn}': loan count changed from {existing_count} to {new_count}")

        # Delete existing loans for these funds
        for fn in fund_names_in_upload:
            CreditLoan.query.filter_by(firm_id=resolved_firm_id, fund_name=fn).delete()

    # Parse rows
    loan_count = 0
    fund_set = set()
    warning_count = 0

    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row (1-indexed header + data)

        company = _clean_str(row.get("company_name"))
        fund = _clean_str(row.get("fund_name"))
        close = _clean_date(row.get("close_date"))

        if not company:
            _log_issue(row_num, "error", "Missing company name, skipping row")
            warning_count += 1
            continue
        if not fund:
            _log_issue(row_num, "error", "Missing fund name, skipping row")
            warning_count += 1
            continue
        if not close:
            _log_issue(row_num, "error", "Missing or invalid close date, skipping row")
            warning_count += 1
            continue

        # Clean all fields
        loan = CreditLoan(
            company_name=company,
            fund_name=fund,
            vintage_year=_clean_int(row.get("vintage_year")),
            close_date=close,
            exit_date=_clean_date(row.get("exit_date")),
            status=_clean_str(row.get("status")) or "Unrealized",
            as_of_date=_clean_date(row.get("as_of_date")),
            instrument=_clean_str(row.get("instrument")),
            tranche=_clean_str(row.get("tranche")),
            security_type=_clean_str(row.get("security_type")),
            issue_size=_clean_float(row.get("issue_size")),
            hold_size=_clean_float(row.get("hold_size")),
            coupon_rate=_clean_float(row.get("coupon_rate")),
            spread_bps=_clean_int(row.get("spread_bps")),
            floor_rate=_clean_float(row.get("floor_rate")),
            fee_oid=_clean_float(row.get("fee_oid")),
            fee_upfront=_clean_float(row.get("fee_upfront")),
            fee_exit=_clean_float(row.get("fee_exit")),
            maturity_date=_clean_date(row.get("maturity_date")),
            fixed_or_floating=_clean_str(row.get("fixed_or_floating")),
            reference_rate=_clean_str(row.get("reference_rate")),
            pik_toggle=_clean_bool(row.get("pik_toggle")) or False,
            pik_rate=_clean_float(row.get("pik_rate")),
            call_protection_months=_clean_int(row.get("call_protection_months")),
            make_whole_premium=_clean_float(row.get("make_whole_premium")),
            amortization_type=_clean_str(row.get("amortization_type")),
            payment_frequency=_clean_str(row.get("payment_frequency")),
            entry_ltv=_normalize_ltv(_clean_float(row.get("entry_ltv"))),
            current_ltv=_normalize_ltv(_clean_float(row.get("current_ltv"))),
            entry_revenue=_clean_float(row.get("entry_revenue")),
            entry_ebitda=_clean_float(row.get("entry_ebitda")),
            current_revenue=_clean_float(row.get("current_revenue")),
            current_ebitda=_clean_float(row.get("current_ebitda")),
            interest_coverage_ratio=_clean_float(row.get("interest_coverage_ratio")),
            dscr=_clean_float(row.get("dscr")),
            internal_credit_rating=_clean_int(row.get("internal_credit_rating")),
            default_status=_clean_str(row.get("default_status")) or "Performing",
            covenant_type=_clean_str(row.get("covenant_type")),
            covenant_compliant=_clean_bool(row.get("covenant_compliant")),
            gross_irr=_clean_float(row.get("gross_irr")),
            moic=_clean_float(row.get("moic")),
            realized_value=_clean_float(row.get("realized_value")),
            unrealized_value=_clean_float(row.get("unrealized_value")),
            cumulative_interest_income=_clean_float(row.get("cumulative_interest_income")),
            cumulative_fee_income=_clean_float(row.get("cumulative_fee_income")),
            fair_value=_clean_float(row.get("fair_value")),
            yield_to_maturity=_clean_float(row.get("yield_to_maturity")),
            recovery_rate=_clean_float(row.get("recovery_rate")),
            original_par=_clean_float(row.get("original_par")),
            current_outstanding=_clean_float(row.get("current_outstanding")),
            accrued_interest=_clean_float(row.get("accrued_interest")),
            sector=_clean_str(row.get("sector")),
            geography=_clean_str(row.get("geography")),
            sponsor=_clean_str(row.get("sponsor")),
            currency=_clean_str(row.get("currency")) or "USD",
            firm_id=resolved_firm_id,
            team_id=team_id,
            upload_batch=batch_id,
        )

        # Validate LTV
        if loan.entry_ltv is not None and loan.entry_ltv > 1.0:
            _log_issue(row_num, "warning", f"Entry LTV {loan.entry_ltv} auto-normalized from percentage")
            loan.entry_ltv = loan.entry_ltv / 100.0
        if loan.current_ltv is not None and loan.current_ltv > 1.0:
            _log_issue(row_num, "warning", f"Current LTV {loan.current_ltv} auto-normalized from percentage")
            loan.current_ltv = loan.current_ltv / 100.0

        # PIK consistency
        if loan.pik_toggle and loan.pik_rate is None:
            _log_issue(row_num, "warning", "PIK is enabled but no PIK rate provided")
            warning_count += 1

        # Covenant consistency
        if loan.covenant_type and loan.covenant_type.lower() != "none" and loan.covenant_compliant is None:
            _log_issue(row_num, "warning", "Covenant type set but compliance not specified")
            warning_count += 1

        # Credit rating range
        if loan.internal_credit_rating is not None and not (1 <= loan.internal_credit_rating <= 5):
            _log_issue(row_num, "warning", f"Credit rating {loan.internal_credit_rating} outside 1-5 range")
            warning_count += 1

        db.session.add(loan)
        loan_count += 1
        fund_set.add(fund)

    # Parse optional Snapshots sheet
    snapshot_count = 0
    for sname in xls.sheet_names:
        if sname.lower().strip() in ("snapshots", "quarterly", "quarterly snapshots"):
            snapshot_count = _parse_snapshot_sheet(
                xls, sname, resolved_firm_id, team_id, batch_id, _log_issue
            )
            break

    db.session.commit()

    return {
        "loans": loan_count,
        "funds": fund_set,
        "snapshots": snapshot_count,
        "warnings": warning_count + len([i for i in issues if i["severity"] == "warning"]),
        "errors": len([i for i in issues if i["severity"] == "error"]),
        "batch_id": batch_id,
        "firm_id": resolved_firm_id,
        "issues": issues,
    }


def _parse_snapshot_sheet(xls, sheet_name, firm_id, team_id, batch_id, log_issue):
    """Parse the optional Snapshots sheet for CreditLoanSnapshot data."""
    df = pd.read_excel(xls, sheet_name=sheet_name)
    if df.empty:
        return 0

    # Map columns — snapshot sheets have a subset of credit columns plus snapshot_date
    snapshot_col_map = {
        "company": "company_name",
        "company name": "company_name",
        "borrower": "company_name",
        "snapshot date": "snapshot_date",
        "quarter end": "snapshot_date",
        "reporting date": "snapshot_date",
        "date": "snapshot_date",
        "current ltv": "current_ltv",
        "ltv": "current_ltv",
        "fair value": "fair_value",
        "market value": "fair_value",
        "current revenue": "current_revenue",
        "current ebitda": "current_ebitda",
        "interest coverage": "interest_coverage_ratio",
        "icr": "interest_coverage_ratio",
        "dscr": "dscr",
        "default status": "default_status",
        "credit status": "default_status",
        "credit rating": "internal_credit_rating",
        "rating": "internal_credit_rating",
        "covenant compliant": "covenant_compliant",
        "current outstanding": "current_outstanding",
        "accrued interest": "accrued_interest",
    }

    col_map = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key in snapshot_col_map:
            col_map[col] = snapshot_col_map[key]
    df = df.rename(columns=col_map)

    count = 0
    for idx, row in df.iterrows():
        row_num = idx + 2
        company = _clean_str(row.get("company_name"))
        snap_date = _clean_date(row.get("snapshot_date"))

        if not company or not snap_date:
            log_issue(row_num, "warning", "Snapshot row missing company or date, skipping")
            continue

        # Find the matching CreditLoan
        loan = CreditLoan.query.filter_by(
            firm_id=firm_id, company_name=company
        ).first()
        if not loan:
            log_issue(row_num, "warning", f"No matching loan for snapshot company '{company}'")
            continue

        snapshot = CreditLoanSnapshot(
            credit_loan_id=loan.id,
            snapshot_date=snap_date,
            current_ltv=_normalize_ltv(_clean_float(row.get("current_ltv"))),
            fair_value=_clean_float(row.get("fair_value")),
            current_revenue=_clean_float(row.get("current_revenue")),
            current_ebitda=_clean_float(row.get("current_ebitda")),
            interest_coverage_ratio=_clean_float(row.get("interest_coverage_ratio")),
            dscr=_clean_float(row.get("dscr")),
            default_status=_clean_str(row.get("default_status")),
            internal_credit_rating=_clean_int(row.get("internal_credit_rating")),
            covenant_compliant=_clean_bool(row.get("covenant_compliant")),
            current_outstanding=_clean_float(row.get("current_outstanding")),
            accrued_interest=_clean_float(row.get("accrued_interest")),
            firm_id=firm_id,
            team_id=team_id,
            upload_batch=batch_id,
        )
        db.session.add(snapshot)
        count += 1

    return count

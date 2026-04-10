"""Excel parser for private credit loan tapes.

Follows the same pattern as deal_parser.py: flexible column mapping,
row validation, UploadIssue logging, per-fund replace semantics.
"""

import math
import re
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta

import pandas as pd

from models import (
    CreditFundPerformance,
    CreditLoan,
    CreditLoanSnapshot,
    Firm,
    TeamFirmAccess,
    UploadIssue,
    db,
)
from services.deal_parser import _resolve_or_create_firm

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
    "fund size": "fund_size",
    "committed fund size": "fund_size",
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
    "spread (bps)": "spread_bps",
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
    "call protection (months)": "call_protection_months",
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
    # NEW: Real LP field mappings
    # Company details
    "count of investments": "investment_count",
    "# investments": "investment_count",
    "business description": "business_description",
    "description": "business_description",
    "public": "is_public",
    "publicly traded": "is_public",
    "location": "location",
    "sourcing channel": "sourcing_channel",
    "sourcing": "sourcing_channel",
    # Fund aliases
    "fund(s)": "fund_name",
    "funds": "fund_name",
    # Entry date aliases (map to close_date for backward compat)
    "entry date": "close_date",
    "funding date": "close_date",
    # Capital structure
    "committed": "committed_amount",
    "commitment": "committed_amount",
    "entry loan amount": "entry_loan_amount",
    "entry loan": "entry_loan_amount",
    "current invested capital": "current_invested_capital",
    "invested capital": "current_invested_capital",
    # Valuation (new)
    "realized proceeds": "realized_proceeds",
    "unrealized loan value": "unrealized_loan_value",
    "unrealized loan": "unrealized_loan_value",
    "unrealized warrant/equity value": "unrealized_warrant_equity_value",
    "unrealized warrant equity value": "unrealized_warrant_equity_value",
    "warrant equity value": "unrealized_warrant_equity_value",
    "total value": "total_value",
    "estimated gross irr at entry": "estimated_irr_at_entry",
    "estimated irr": "estimated_irr_at_entry",
    "gross moic": "moic",
    # Loan economics (new)
    "cash margin": "cash_margin",
    "cash margin / coupon": "cash_margin",
    "pik margin": "pik_margin",
    "pik margin / coupon": "pik_margin",
    "pik coupon": "pik_margin",
    "closing fee": "closing_fee",
    "loan term": "loan_term",
    "term (years)": "term_years",
    "term years": "term_years",
    "term": "term_years",
    "loan term (years)": "term_years",
    "prepayment protection": "prepayment_protection",
    # Warrants (entirely new)
    "equity investment": "equity_investment",
    "warrants at entry": "warrants_at_entry",
    "# warrants at entry": "warrants_at_entry",
    "warrant strike price at entry": "warrant_strike_entry",
    "warrants current": "warrants_current",
    "# warrants (current)": "warrants_current",
    "# warrants current": "warrants_current",
    "warrant strike price (current)": "warrant_strike_current",
    "warrant strike price current": "warrant_strike_current",
    "warrant term": "warrant_term",
    # Revenue
    "ttm revenue (entry)": "ttm_revenue_entry",
    "ttm revenue entry": "ttm_revenue_entry",
    "ttm revenue (current)": "ttm_revenue_current",
    "ttm revenue current": "ttm_revenue_current",
    # Collateral & coverage
    "entry collateral": "entry_collateral",
    "entry collateral $": "entry_collateral",
    "collateral at entry": "entry_collateral",
    "current collateral": "current_collateral",
    "exit collateral": "current_collateral",
    "exit collateral $": "current_collateral",
    "collateral value": "current_collateral",
    "entry coverage ratio": "entry_coverage_ratio",
    "coverage ratio at entry": "entry_coverage_ratio",
    "current coverage ratio": "current_coverage_ratio",
    "exit coverage ratio": "current_coverage_ratio",
    "coverage ratio": "current_coverage_ratio",
    "entry equity cushion": "entry_equity_cushion",
    "entry equity cushion %": "entry_equity_cushion",
    "equity cushion at entry": "entry_equity_cushion",
    "current equity cushion": "current_equity_cushion",
    "exit equity cushion": "current_equity_cushion",
    "exit equity cushion %": "current_equity_cushion",
    "equity cushion": "current_equity_cushion",
    # Renamed warrant aliases
    "warrant strike (entry)": "warrant_strike_entry",
    "warrant strike (current)": "warrant_strike_current",
    "warrants (current)": "warrants_current",
}

# Columns that trigger credit detection (need 2+ present)
CREDIT_TRIGGER_COLUMNS = {
    "coupon_rate", "spread_bps", "entry_ltv", "current_ltv", "maturity_date",
    "tranche", "pik_toggle",
    # New fields that also identify credit data
    "cash_margin", "pik_margin", "closing_fee", "warrants_at_entry",
    "committed_amount", "entry_loan_amount", "unrealized_loan_value",
}

# Columns that trigger equity detection
EQUITY_TRIGGER_COLUMNS = {"equity_invested", "entry_enterprise_value", "ownership_pct"}

REQUIRED_FIELDS = {"company_name", "fund_name"}

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
    "committed_amount", "entry_loan_amount", "current_invested_capital",
    "realized_proceeds", "unrealized_loan_value", "unrealized_warrant_equity_value",
    "total_value", "estimated_irr_at_entry",
    "cash_margin", "pik_margin", "closing_fee",
    "equity_investment", "warrant_strike_entry", "warrant_strike_current",
    "ttm_revenue_entry", "ttm_revenue_current",
    "term_years",
    "entry_collateral", "current_collateral",
    "entry_coverage_ratio", "current_coverage_ratio",
    "entry_equity_cushion", "current_equity_cushion",
}
INT_FIELDS = {
    "vintage_year", "spread_bps", "call_protection_months", "internal_credit_rating",
    "investment_count", "warrants_at_entry", "warrants_current",
}
BOOL_FIELDS = {"pik_toggle", "covenant_compliant", "is_public"}

MIN_VALID_DATE = date(1900, 1, 1)
MAX_VALID_DATE = date(2200, 12, 31)
EXCEL_EPOCH = datetime(1899, 12, 30)


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
        return _normalize_date(val)
    if hasattr(val, "date"):
        return _normalize_date(val.date())
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return _parse_numeric_date(float(val))
    if isinstance(val, str):
        return _parse_string_date(val)
    try:
        return _normalize_date(pd.to_datetime(val).date())
    except Exception:
        return None


def _normalize_date(value):
    if value is None:
        return None
    if value < MIN_VALID_DATE or value > MAX_VALID_DATE:
        return None
    return value


def _parse_excel_serial_date(value):
    if not math.isfinite(value):
        return None
    if value < 20000 or value > 150000:
        return None
    try:
        return _normalize_date((EXCEL_EPOCH + timedelta(days=value)).date())
    except OverflowError:
        return None


def _parse_numeric_date(value):
    if not math.isfinite(value):
        return None

    # Support YYYYMMDD integers that sometimes appear in exported tapes.
    if float(value).is_integer():
        integer_value = int(value)
        if 19000101 <= integer_value <= 22001231:
            try:
                return _normalize_date(datetime.strptime(str(integer_value), "%Y%m%d").date())
            except ValueError:
                pass

    return _parse_excel_serial_date(value)


def _parse_string_date(value):
    text = value.strip()
    if not text:
        return None

    if re.fullmatch(r"\d{8}", text):
        try:
            return _normalize_date(datetime.strptime(text, "%Y%m%d").date())
        except ValueError:
            return None

    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        try:
            return _parse_numeric_date(float(text))
        except ValueError:
            return None

    # Short-circuit obviously invalid five-digit year strings like 48113-11-21.
    if re.fullmatch(r"\d{5,}[-/]\d{1,2}[-/]\d{1,2}", text):
        return None

    try:
        return _normalize_date(pd.to_datetime(text).date())
    except Exception:
        return None


def _parse_term_to_years(val):
    """Parse a loan term value into years (float).

    Accepts:
      - Numeric: always treated as years (e.g., 5 = 5 years)
      - Text: "5 years", "60 months", "5Y", "3-5 years", "18M", "18mo"
    Returns float years or None.
    """
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None

    # Already numeric — always treated as years
    if isinstance(val, (int, float)):
        f = float(val)
        if f <= 0:
            return None
        return f

    s = str(val).strip().lower()
    if not s:
        return None

    # "5 years" or "5yr" or "5y"
    m = re.match(r'^(\d+(?:\.\d+)?)\s*(?:years?|yr?s?)$', s)
    if m:
        return float(m.group(1))

    # "60 months" or "60m" or "18mo" — convert to years
    m = re.match(r'^(\d+(?:\.\d+)?)\s*(?:months?|mos?|m)$', s)
    if m:
        return float(m.group(1)) / 12.0

    # "3-5 years" (range, use midpoint)
    m = re.match(r'^(\d+)-(\d+)\s*(?:years?|yr?s?)$', s)
    if m:
        return (float(m.group(1)) + float(m.group(2))) / 2

    # "3-5" bare range (assume years)
    m = re.match(r'^(\d+)-(\d+)$', s)
    if m:
        return (float(m.group(1)) + float(m.group(2))) / 2

    # Try plain numeric as fallback — always years
    try:
        f = float(re.sub(r'[,$%]', '', s))
        if f <= 0:
            return None
        return f
    except (ValueError, TypeError):
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
    resolved_firm_id = firm_id
    resolved_firm_name = firm_name

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

    def _scoped_credit_loan_query():
        query = CreditLoan.query.filter(CreditLoan.firm_id == resolved_firm_id)
        if team_id is None:
            return query.filter(CreditLoan.team_id.is_(None))
        return query.filter(CreditLoan.team_id == team_id)

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
    if resolved_firm_id is None and "firm_name" in df.columns:
        first_firm = _clean_str(df["firm_name"].iloc[0]) if len(df) > 0 else None
        if first_firm:
            resolved_firm_name = first_firm

    if resolved_firm_id is None and resolved_firm_name:
        # Reuse the deal_parser helper so slug + uniqueness + base_currency are handled
        # in one place. Previously created Firm() inline without a slug, which crashed
        # on the NOT NULL constraint for firms.slug.
        existing = Firm.query.filter_by(name=resolved_firm_name).first()
        is_new = existing is None
        firm = _resolve_or_create_firm(resolved_firm_name, "USD")
        if firm is not None:
            resolved_firm_id = firm.id
            if is_new and team_id:
                db.session.add(TeamFirmAccess(team_id=team_id, firm_id=resolved_firm_id))

    # Parse rows
    parsed_loans = []
    fund_row_counts = defaultdict(int)
    fund_set = set()

    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row (1-indexed header + data)

        company = _clean_str(row.get("company_name"))
        fund = _clean_str(row.get("fund_name"))

        if not company:
            _log_issue(row_num, "error", "Missing company name, skipping row")
            continue
        if not fund:
            _log_issue(row_num, "error", "Missing fund name, skipping row")
            continue

        close_date = _clean_date(row.get("close_date"))
        if close_date is None:
            _log_issue(row_num, "error", "Missing or invalid entry date, skipping row")
            continue

        # Clean all fields
        loan = CreditLoan(
            company_name=company,
            fund_name=fund,
            vintage_year=_clean_int(row.get("vintage_year")),
            fund_size=_clean_float(row.get("fund_size")),
            close_date=close_date,
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
            # --- NEW FIELDS ---
            investment_count=_clean_int(row.get("investment_count")),
            business_description=_clean_str(row.get("business_description")),
            is_public=_clean_bool(row.get("is_public")),
            sourcing_channel=_clean_str(row.get("sourcing_channel")),
            location=_clean_str(row.get("location")),
            committed_amount=_clean_float(row.get("committed_amount")),
            entry_loan_amount=_clean_float(row.get("entry_loan_amount")),
            current_invested_capital=_clean_float(row.get("current_invested_capital")),
            realized_proceeds=_clean_float(row.get("realized_proceeds")),
            unrealized_loan_value=_clean_float(row.get("unrealized_loan_value")),
            unrealized_warrant_equity_value=_clean_float(row.get("unrealized_warrant_equity_value")),
            total_value=_clean_float(row.get("total_value")),
            estimated_irr_at_entry=_clean_float(row.get("estimated_irr_at_entry")),
            cash_margin=_clean_float(row.get("cash_margin")),
            pik_margin=_clean_float(row.get("pik_margin")),
            closing_fee=_clean_float(row.get("closing_fee")),
            prepayment_protection=_clean_str(row.get("prepayment_protection")),
            loan_term=_clean_str(row.get("loan_term")),
            equity_investment=_clean_float(row.get("equity_investment")),
            warrants_at_entry=_clean_int(row.get("warrants_at_entry")),
            warrant_strike_entry=_clean_float(row.get("warrant_strike_entry")),
            warrants_current=_clean_int(row.get("warrants_current")),
            warrant_strike_current=_clean_float(row.get("warrant_strike_current")),
            warrant_term=_clean_str(row.get("warrant_term")),
            ttm_revenue_entry=_clean_float(row.get("ttm_revenue_entry")),
            ttm_revenue_current=_clean_float(row.get("ttm_revenue_current")),
            term_years=_clean_float(row.get("term_years")),
            entry_collateral=_clean_float(row.get("entry_collateral")),
            current_collateral=_clean_float(row.get("current_collateral")),
            entry_coverage_ratio=_clean_float(row.get("entry_coverage_ratio")),
            current_coverage_ratio=_clean_float(row.get("current_coverage_ratio")),
            entry_equity_cushion=_clean_float(row.get("entry_equity_cushion")),
            current_equity_cushion=_clean_float(row.get("current_equity_cushion")),
            sector=_clean_str(row.get("sector")),
            geography=_clean_str(row.get("geography")),
            sponsor=_clean_str(row.get("sponsor")),
            currency=_clean_str(row.get("currency")) or "USD",
            firm_id=resolved_firm_id,
            team_id=team_id,
            upload_batch=batch_id,
        )

        # Cross-populate old/new field pairs for backward compatibility
        if loan.entry_loan_amount is not None and loan.hold_size is None:
            loan.hold_size = loan.entry_loan_amount
        elif loan.hold_size is not None and loan.entry_loan_amount is None:
            loan.entry_loan_amount = loan.hold_size

        if loan.location is not None and loan.geography is None:
            loan.geography = loan.location
        elif loan.geography is not None and loan.location is None:
            loan.location = loan.geography

        if loan.cash_margin is not None and loan.coupon_rate is None:
            loan.coupon_rate = loan.cash_margin
        elif loan.coupon_rate is not None and loan.cash_margin is None:
            loan.cash_margin = loan.coupon_rate

        if loan.pik_margin is not None and loan.pik_rate is None:
            loan.pik_rate = loan.pik_margin

        if loan.closing_fee is not None and loan.fee_oid is None:
            loan.fee_oid = loan.closing_fee

        if loan.realized_proceeds is not None and loan.realized_value is None:
            loan.realized_value = loan.realized_proceeds

        if loan.unrealized_loan_value is not None and loan.unrealized_value is None:
            loan.unrealized_value = loan.unrealized_loan_value

        # Auto-compute term_years from loan_term string if not provided directly
        if loan.term_years is None and loan.loan_term is not None:
            loan.term_years = _parse_term_to_years(loan.loan_term)

        # Compute realization status from realized/unrealized values.
        # Overrides whatever the user uploaded — the data tells the truth.
        realized = (loan.realized_value or 0.0) + (loan.realized_proceeds or 0.0)
        unrealized = (loan.unrealized_value or 0.0) + (loan.unrealized_loan_value or 0.0) + (loan.unrealized_warrant_equity_value or 0.0)
        ds = (loan.default_status or "").lower()

        if ds in ("default", "restructured"):
            loan.status = "Fully Realized"
        elif realized > 0 and unrealized <= 0 and loan.exit_date is not None:
            loan.status = "Fully Realized"
        elif realized > 0 and unrealized > 0:
            loan.status = "Partially Realized"
        elif realized > 0 and unrealized <= 0:
            # Has realized proceeds but no exit date, still treat as fully realized
            loan.status = "Fully Realized"
        else:
            loan.status = "Unrealized"

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

        # Covenant consistency
        if loan.covenant_type and loan.covenant_type.lower() != "none" and loan.covenant_compliant is None:
            _log_issue(row_num, "warning", "Covenant type set but compliance not specified")

        # Credit rating range
        if loan.internal_credit_rating is not None and not (1 <= loan.internal_credit_rating <= 5):
            _log_issue(row_num, "warning", f"Credit rating {loan.internal_credit_rating} outside 1-5 range")

        parsed_loans.append(loan)
        fund_row_counts[fund] += 1
        fund_set.add(fund)

    if not parsed_loans:
        raise ValueError("No valid credit loans found. Each row must include Company Name, Fund Name, and Entry Date.")

    # Per-fund replace: only replace funds that have at least one valid row in
    # this upload, and scope replacement to the uploading team.
    if resolved_firm_id:
        for fn, new_count in fund_row_counts.items():
            existing_query = _scoped_credit_loan_query().filter(CreditLoan.fund_name == fn)
            existing_count = existing_query.count()
            if existing_count > 0 and new_count != existing_count:
                _log_issue(
                    0,
                    "warning",
                    f"Fund '{fn}': loan count changed from {existing_count} to {new_count}",
                )

            existing_ids = [
                row[0]
                for row in existing_query.with_entities(CreditLoan.id).all()
            ]
            if existing_ids:
                CreditLoanSnapshot.query.filter(
                    CreditLoanSnapshot.credit_loan_id.in_(existing_ids)
                ).delete(synchronize_session=False)
                existing_query.delete(synchronize_session=False)

    for loan in parsed_loans:
        db.session.add(loan)
    db.session.flush()
    loan_count = len(parsed_loans)

    # Parse optional Snapshots sheet
    snapshot_count = 0
    for sname in xls.sheet_names:
        if sname.lower().strip() in ("snapshots", "quarterly", "quarterly snapshots"):
            snapshot_count = _parse_snapshot_sheet(
                xls, sname, resolved_firm_id, team_id, batch_id, _log_issue
            )
            break

    # Parse optional Fund Performance sheet (net returns by fund)
    fund_perf_count = 0
    for sname in xls.sheet_names:
        if sname.lower().strip() in (
            "fund performance",
            "funds",
            "fund returns",
            "net returns",
            "fund net returns",
        ):
            fund_perf_count = _parse_fund_performance_sheet(
                xls, sname, resolved_firm_id, team_id, batch_id, _log_issue
            )
            break

    db.session.commit()

    return {
        "loans": loan_count,
        "funds": fund_set,
        "snapshots": snapshot_count,
        "fund_performance": fund_perf_count,
        "warnings": len([i for i in issues if i["severity"] == "warning"]),
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
        "fund": "fund_name",
        "fund name": "fund_name",
        "vehicle": "fund_name",
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
        "current invested capital": "current_invested_capital",
        "invested capital": "current_invested_capital",
        "unrealized loan value": "unrealized_loan_value",
        "unrealized loan": "unrealized_loan_value",
        "unrealized warrant/equity value": "unrealized_warrant_equity_value",
        "total value": "total_value",
        "ttm revenue (current)": "ttm_revenue_current",
        "ttm revenue current": "ttm_revenue_current",
        "gross irr": "gross_irr",
        "irr": "gross_irr",
        "moic": "moic",
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
        fund_name = _clean_str(row.get("fund_name"))
        snap_date = _clean_date(row.get("snapshot_date"))

        if not company or not snap_date:
            log_issue(row_num, "warning", "Snapshot row missing company or date, skipping")
            continue

        # Match only against loans created in this upload so shared firms or
        # repeated company names in other teams cannot hijack the snapshot.
        loan_query = CreditLoan.query.filter(
            CreditLoan.firm_id == firm_id,
            CreditLoan.upload_batch == batch_id,
            CreditLoan.company_name == company,
        )
        if team_id is None:
            loan_query = loan_query.filter(CreditLoan.team_id.is_(None))
        else:
            loan_query = loan_query.filter(CreditLoan.team_id == team_id)
        if fund_name:
            loan_query = loan_query.filter(CreditLoan.fund_name == fund_name)

        matches = loan_query.all()
        if not matches:
            detail = f" for fund '{fund_name}'" if fund_name else ""
            log_issue(row_num, "warning", f"No matching loan for snapshot company '{company}'{detail}")
            continue
        if len(matches) > 1:
            log_issue(
                row_num,
                "warning",
                f"Multiple matching loans found for snapshot company '{company}'. Include Fund Name on the Snapshots sheet.",
            )
            continue
        loan = matches[0]

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
            current_invested_capital=_clean_float(row.get("current_invested_capital")),
            unrealized_loan_value=_clean_float(row.get("unrealized_loan_value")),
            unrealized_warrant_equity_value=_clean_float(row.get("unrealized_warrant_equity_value")),
            total_value=_clean_float(row.get("total_value")),
            ttm_revenue_current=_clean_float(row.get("ttm_revenue_current")),
            gross_irr=_clean_float(row.get("gross_irr")),
            moic=_clean_float(row.get("moic")),
            firm_id=firm_id,
            team_id=team_id,
            upload_batch=batch_id,
        )
        db.session.add(snapshot)
        count += 1

    return count


# ---------------------------------------------------------------------------
# Fund Performance sheet parser (net returns by fund)
# ---------------------------------------------------------------------------


def _normalize_pct(value):
    """Accept percentages written as either 0.15 (decimal) or 15 (integer %).

    Values strictly greater than 1.5 are interpreted as percentages and
    divided by 100. The 1.5 cutoff lets legitimate 1.2x / 1.5x multiples
    stay intact — this helper is only meant for rate/IRR fields, so upstream
    callers should not pass MOIC through here.
    """
    if value is None:
        return None
    if value > 1.5:
        return value / 100.0
    return value


def _parse_fund_performance_sheet(xls, sheet_name, firm_id, team_id, batch_id, log_issue):
    """Parse the optional Fund Performance sheet for CreditFundPerformance rows.

    Expected columns (any combination of the aliases below is accepted):
    Fund Name, Vintage Year, Fund Size, Net IRR, Net MOIC, DPI, RVPI, TVPI,
    Called Capital, Distributed Capital, NAV, Report Date, Currency.

    Per-firm replace semantics: existing fund_performance rows for the firm
    are deleted before inserting. This mirrors the per-fund replace logic
    used for loans so repeat uploads don't duplicate.
    """
    df = pd.read_excel(xls, sheet_name=sheet_name)
    if df.empty:
        return 0

    fund_perf_col_map = {
        "fund": "fund_name",
        "fund name": "fund_name",
        "vehicle": "fund_name",
        "vintage": "vintage_year",
        "vintage year": "vintage_year",
        "fund size": "fund_size",
        "committed fund size": "fund_size",
        "total commitments": "fund_size",
        "net irr": "net_irr",
        "irr (net)": "net_irr",
        "net_irr": "net_irr",
        "net moic": "net_moic",
        "moic (net)": "net_moic",
        "net_moic": "net_moic",
        "net tvpi": "net_tvpi",
        "tvpi": "net_tvpi",
        "net rvpi": "net_rvpi",
        "rvpi": "net_rvpi",
        "net dpi": "net_dpi",
        "dpi": "net_dpi",
        "called capital": "called_capital",
        "capital called": "called_capital",
        "paid in": "called_capital",
        "paid-in capital": "called_capital",
        "distributed capital": "distributed_capital",
        "distributions": "distributed_capital",
        "total distributed": "distributed_capital",
        "nav": "nav",
        "net asset value": "nav",
        "ending nav": "nav",
        "report date": "report_date",
        "as of date": "report_date",
        "as-of date": "report_date",
        "as of": "report_date",
        "currency": "currency",
    }

    col_map = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key in fund_perf_col_map:
            col_map[col] = fund_perf_col_map[key]
    df = df.rename(columns=col_map)

    if "fund_name" not in df.columns:
        log_issue(0, "warning", "Fund Performance sheet missing Fund Name column, skipping")
        return 0

    # Per-firm replace semantics: delete existing rows for this firm before insert.
    if firm_id is not None:
        scoped_delete = CreditFundPerformance.query.filter(CreditFundPerformance.firm_id == firm_id)
        if team_id is None:
            scoped_delete = scoped_delete.filter(CreditFundPerformance.team_id.is_(None))
        else:
            scoped_delete = scoped_delete.filter(CreditFundPerformance.team_id == team_id)
        scoped_delete.delete(synchronize_session=False)

    count = 0
    for idx, row in df.iterrows():
        row_num = idx + 2
        fund_name = _clean_str(row.get("fund_name"))
        if not fund_name:
            log_issue(row_num, "warning", "Fund Performance row missing Fund Name, skipping")
            continue

        perf = CreditFundPerformance(
            fund_name=fund_name,
            vintage_year=_clean_int(row.get("vintage_year")),
            fund_size=_clean_float(row.get("fund_size")),
            net_irr=_normalize_pct(_clean_float(row.get("net_irr"))),
            net_moic=_clean_float(row.get("net_moic")),
            net_dpi=_clean_float(row.get("net_dpi")),
            net_rvpi=_clean_float(row.get("net_rvpi")),
            net_tvpi=_clean_float(row.get("net_tvpi")),
            called_capital=_clean_float(row.get("called_capital")),
            distributed_capital=_clean_float(row.get("distributed_capital")),
            nav=_clean_float(row.get("nav")),
            report_date=_clean_date(row.get("report_date")),
            currency=_clean_str(row.get("currency")) or "USD",
            firm_id=firm_id,
            team_id=team_id,
            upload_batch=batch_id,
        )
        db.session.add(perf)
        count += 1

    return count

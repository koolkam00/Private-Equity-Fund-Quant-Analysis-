import json
import math
import re
import uuid
from collections import defaultdict

import pandas as pd

from models import (
    Deal,
    DealCashflowEvent,
    DealQuarterSnapshot,
    DealUnderwriteBaseline,
    Firm,
    FundQuarterSnapshot,
    UploadIssue,
    db,
)
from services.utils import clean_str, clean_val


COLUMN_MAP = {
    # Identification
    "company": "company_name",
    "company name": "company_name",
    "company/deal name": "company_name",
    "deal name": "company_name",
    "deal": "company_name",
    "portfolio company": "company_name",
    "firm": "firm_name",
    "firm name": "firm_name",
    "pe firm": "firm_name",
    "manager": "firm_name",
    "fund": "fund_number",
    "fund #": "fund_number",
    "fund number": "fund_number",
    "fund name": "fund_number",
    "sector": "sector",
    "industry": "sector",
    "sector/industry": "sector",
    "geography": "geography",
    "region": "geography",
    "country": "geography",
    "status": "status",
    "deal status": "status",
    "realization status": "status",
    "exit type": "exit_type",
    "exit route": "exit_type",
    "exit channel": "exit_type",
    "lead partner": "lead_partner",
    "deal lead": "lead_partner",
    "security type": "security_type",
    "instrument type": "security_type",
    "deal type": "deal_type",
    "investment type": "deal_type",
    "entry channel": "entry_channel",
    "origination channel": "entry_channel",
    "sourcing channel": "entry_channel",
    # Dates
    "investment date": "investment_date",
    "entry date": "investment_date",
    "year invested": "year_invested",
    "vintage": "year_invested",
    "vintage year": "year_invested",
    "exit date": "exit_date",
    # Investment values
    "equity invested": "equity_invested",
    "equity": "equity_invested",
    "invested": "equity_invested",
    "investment amount": "equity_invested",
    "fund size": "fund_size",
    "realized value": "realized_value",
    "realized": "realized_value",
    "unrealized value": "unrealized_value",
    "unrealized": "unrealized_value",
    # Ownership
    "ownership %": "ownership_pct",
    "ownership pct": "ownership_pct",
    "ownership": "ownership_pct",
    # Entry metrics
    "entry revenue": "entry_revenue",
    "revenue at entry": "entry_revenue",
    "entry ebitda": "entry_ebitda",
    "ebitda at entry": "entry_ebitda",
    "entry ev": "entry_enterprise_value",
    "entry tev": "entry_enterprise_value",
    "entry enterprise value": "entry_enterprise_value",
    "entry total enterprise value": "entry_enterprise_value",
    "enterprise value at entry": "entry_enterprise_value",
    "entry net debt": "entry_net_debt",
    "net debt at entry": "entry_net_debt",
    # Exit metrics
    "exit revenue": "exit_revenue",
    "revenue at exit": "exit_revenue",
    "exit ebitda": "exit_ebitda",
    "ebitda at exit": "exit_ebitda",
    "exit ev": "exit_enterprise_value",
    "exit tev": "exit_enterprise_value",
    "exit enterprise value": "exit_enterprise_value",
    "exit total enterprise value": "exit_enterprise_value",
    "enterprise value at exit": "exit_enterprise_value",
    "exit net debt": "exit_net_debt",
    "net debt at exit": "exit_net_debt",
    # Optional legacy
    "irr": "irr",
    "gross irr": "irr",
    "net irr": "net_irr",
    "net moic": "net_moic",
    "dpi": "net_dpi",
}

VALID_FIELDS = {
    "company_name",
    "firm_name",
    "fund_number",
    "sector",
    "geography",
    "status",
    "exit_type",
    "lead_partner",
    "security_type",
    "deal_type",
    "entry_channel",
    "investment_date",
    "year_invested",
    "exit_date",
    "equity_invested",
    "fund_size",
    "realized_value",
    "unrealized_value",
    "ownership_pct",
    "entry_revenue",
    "entry_ebitda",
    "entry_enterprise_value",
    "entry_net_debt",
    "exit_revenue",
    "exit_ebitda",
    "exit_enterprise_value",
    "exit_net_debt",
    "irr",
    "net_irr",
    "net_moic",
    "net_dpi",
}

DATE_COLS = {"investment_date", "exit_date"}
FLOAT_COLS = {
    "equity_invested",
    "fund_size",
    "realized_value",
    "unrealized_value",
    "ownership_pct",
    "entry_revenue",
    "entry_ebitda",
    "entry_enterprise_value",
    "entry_net_debt",
    "exit_revenue",
    "exit_ebitda",
    "exit_enterprise_value",
    "exit_net_debt",
    "irr",
    "net_irr",
    "net_moic",
    "net_dpi",
}
INT_COLS = {"year_invested"}
STR_COLS = {"company_name", "firm_name", "fund_number", "sector", "geography", "status"}
STR_COLS |= {"exit_type", "lead_partner", "security_type", "deal_type", "entry_channel"}

SHEET_ALIASES = {
    "deals": {"deals", "deal", "dealdata", "sheet1", "portfolio"},
    "cashflows": {"cashflows", "cashflow", "cash flows"},
    "deal_quarterly": {"deal quarterly", "dealquarterly", "quarterly deals", "deal snapshots", "deal_snapshot"},
    "fund_quarterly": {"fund quarterly", "fundquarterly", "fund snapshots", "fund_snapshot"},
    "underwrite": {"underwrite", "underwrites", "underwriting"},
}

CASHFLOW_COLUMN_MAP = {
    "company": "company_name",
    "company name": "company_name",
    "deal": "company_name",
    "deal name": "company_name",
    "fund": "fund_number",
    "fund name": "fund_number",
    "event date": "event_date",
    "date": "event_date",
    "cashflow date": "event_date",
    "event type": "event_type",
    "type": "event_type",
    "amount": "amount",
    "cash flow": "amount",
    "cashflow": "amount",
    "notes": "notes",
}

DEAL_QUARTER_COLUMN_MAP = {
    "company": "company_name",
    "company name": "company_name",
    "deal": "company_name",
    "deal name": "company_name",
    "fund": "fund_number",
    "fund name": "fund_number",
    "quarter": "quarter_end",
    "quarter end": "quarter_end",
    "quarter_end": "quarter_end",
    "revenue": "revenue",
    "ebitda": "ebitda",
    "enterprise value": "enterprise_value",
    "ev": "enterprise_value",
    "tev": "enterprise_value",
    "net debt": "net_debt",
    "equity value": "equity_value",
    "valuation basis": "valuation_basis",
    "basis": "valuation_basis",
    "source": "source",
}

FUND_QUARTER_COLUMN_MAP = {
    "fund": "fund_number",
    "fund name": "fund_number",
    "fund number": "fund_number",
    "quarter": "quarter_end",
    "quarter end": "quarter_end",
    "quarter_end": "quarter_end",
    "committed": "committed_capital",
    "committed capital": "committed_capital",
    "paid in": "paid_in_capital",
    "paid in capital": "paid_in_capital",
    "paid_in_capital": "paid_in_capital",
    "distributed": "distributed_capital",
    "distributed capital": "distributed_capital",
    "distributed_capital": "distributed_capital",
    "nav": "nav",
    "unfunded": "unfunded_commitment",
    "unfunded commitment": "unfunded_commitment",
}

UNDERWRITE_COLUMN_MAP = {
    "company": "company_name",
    "company name": "company_name",
    "deal": "company_name",
    "deal name": "company_name",
    "fund": "fund_number",
    "fund name": "fund_number",
    "baseline date": "baseline_date",
    "date": "baseline_date",
    "target irr": "target_irr",
    "irr target": "target_irr",
    "target moic": "target_moic",
    "moic target": "target_moic",
    "target hold": "target_hold_years",
    "target hold years": "target_hold_years",
    "target exit multiple": "target_exit_multiple",
    "exit multiple target": "target_exit_multiple",
    "target revenue cagr": "target_revenue_cagr",
    "target ebitda cagr": "target_ebitda_cagr",
}


def _normalize_ownership(val):
    if val is None:
        return None
    try:
        num = float(val)
    except (TypeError, ValueError):
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    if num > 1.0:
        num /= 100.0
    return num


def _record_issue(issue_report_id, batch_id, firm_id, row_num, company, severity, message, payload):
    db.session.add(
        UploadIssue(
            issue_report_id=issue_report_id,
            firm_id=firm_id,
            upload_batch=batch_id,
            file_type="deals",
            row_number=row_num,
            company_name=company,
            severity=severity,
            message=message,
            payload=json.dumps(payload, default=str),
        )
    )


def _warn_extreme_multiples(deal):
    warnings = []
    if deal.entry_ebitda and deal.entry_ebitda != 0 and deal.entry_enterprise_value is not None:
        entry_mult = deal.entry_enterprise_value / deal.entry_ebitda
        if abs(entry_mult) > 50:
            warnings.append(f"Entry TEV/EBITDA {entry_mult:.1f}x appears extreme")
    if deal.exit_ebitda and deal.exit_ebitda != 0 and deal.exit_enterprise_value is not None:
        exit_mult = deal.exit_enterprise_value / deal.exit_ebitda
        if abs(exit_mult) > 50:
            warnings.append(f"Exit TEV/EBITDA {exit_mult:.1f}x appears extreme")
    return warnings


def _normalize_sheet_name(name):
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())


def _find_sheet(workbook, aliases):
    alias_norm = {_normalize_sheet_name(a) for a in aliases}
    for sheet_name, df in workbook.items():
        if _normalize_sheet_name(sheet_name) in alias_norm:
            return sheet_name, df.copy()
    return None, None


def _select_deals_sheet(workbook):
    sheet_name, df = _find_sheet(workbook, SHEET_ALIASES["deals"])
    if df is not None:
        return sheet_name, df

    # Backward-compatible fallback to the first sheet in old templates.
    first_name = next(iter(workbook.keys()))
    return first_name, workbook[first_name].copy()


def _normalize_deal_key(company_name, fund_number):
    company = clean_str(company_name)
    if company is None:
        return None
    fund = clean_str(fund_number) or ""
    return (company.strip().lower(), fund.strip().lower())


def _build_deal_lookup(firm_id, upload_batch=None):
    by_key = {}
    by_company = defaultdict(list)

    query = Deal.query.filter_by(firm_id=firm_id)
    if upload_batch:
        query = query.filter(Deal.upload_batch == upload_batch)

    for deal in query.all():
        key = _normalize_deal_key(deal.company_name, deal.fund_number)
        if key is None:
            continue
        by_key[key] = deal
        by_company[key[0]].append(deal)

    return {"by_key": by_key, "by_company": by_company}


def _match_deal(row, lookup):
    company = clean_str(row.get("company_name"))
    fund = clean_str(row.get("fund_number")) or ""
    if company is None:
        return None, "missing company name"

    key = _normalize_deal_key(company, fund)
    if key in lookup["by_key"]:
        return lookup["by_key"][key], None

    company_matches = lookup["by_company"].get(company.lower(), [])
    if fund:
        narrowed = [d for d in company_matches if (clean_str(d.fund_number) or "").lower() == fund.lower()]
        if len(narrowed) == 1:
            return narrowed[0], None
        if len(narrowed) > 1:
            return None, f"ambiguous mapping for company '{company}' and fund '{fund}'"

    if len(company_matches) == 1:
        return company_matches[0], None
    if len(company_matches) > 1:
        return None, f"ambiguous company match for '{company}'"
    return None, f"could not find deal '{company}'"


def _normalize_optional_df(df, column_map, valid_fields, date_cols=None, float_cols=None):
    date_cols = date_cols or set()
    float_cols = float_cols or set()

    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    rename_map = {col: column_map[col] for col in df.columns if col in column_map}
    df = df.rename(columns=rename_map)
    df = df[[c for c in df.columns if c in valid_fields]]

    for col in date_cols & set(df.columns):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    for col in float_cols & set(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _parse_cashflows_sheet(df, lookup, firm_id, issue_report_id, batch_id, errors):
    count = 0
    df = _normalize_optional_df(
        df,
        CASHFLOW_COLUMN_MAP,
        {"company_name", "fund_number", "event_date", "event_type", "amount", "notes"},
        date_cols={"event_date"},
        float_cols={"amount"},
    )

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        deal, reason = _match_deal(row, lookup)
        if deal is None:
            msg = f"Cashflows row {row_num}: skipped — {reason}."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, firm_id, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue

        event_date = clean_val(row.get("event_date"))
        amount = clean_val(row.get("amount"))
        event_type = clean_str(row.get("event_type"))
        if event_date is None or amount is None or event_type is None:
            msg = f"Cashflows row {row_num}: skipped — requires Event Date, Event Type, and Amount."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)
            continue

        db.session.add(
            DealCashflowEvent(
                deal_id=deal.id,
                event_date=event_date,
                event_type=event_type,
                amount=float(amount),
                notes=clean_str(row.get("notes")),
                firm_id=firm_id,
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_deal_quarter_sheet(df, lookup, firm_id, issue_report_id, batch_id, errors):
    count = 0
    df = _normalize_optional_df(
        df,
        DEAL_QUARTER_COLUMN_MAP,
        {
            "company_name",
            "fund_number",
            "quarter_end",
            "revenue",
            "ebitda",
            "enterprise_value",
            "net_debt",
            "equity_value",
            "valuation_basis",
            "source",
        },
        date_cols={"quarter_end"},
        float_cols={"revenue", "ebitda", "enterprise_value", "net_debt", "equity_value"},
    )

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        deal, reason = _match_deal(row, lookup)
        if deal is None:
            msg = f"Deal Quarterly row {row_num}: skipped — {reason}."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, firm_id, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue

        quarter_end = clean_val(row.get("quarter_end"))
        if quarter_end is None:
            msg = f"Deal Quarterly row {row_num}: skipped — Quarter End is required."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)
            continue

        db.session.add(
            DealQuarterSnapshot(
                deal_id=deal.id,
                quarter_end=quarter_end,
                revenue=clean_val(row.get("revenue")),
                ebitda=clean_val(row.get("ebitda")),
                enterprise_value=clean_val(row.get("enterprise_value")),
                net_debt=clean_val(row.get("net_debt")),
                equity_value=clean_val(row.get("equity_value")),
                valuation_basis=clean_str(row.get("valuation_basis")),
                source=clean_str(row.get("source")),
                firm_id=firm_id,
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_fund_quarter_sheet(df, firm_id, issue_report_id, batch_id, errors):
    count = 0
    df = _normalize_optional_df(
        df,
        FUND_QUARTER_COLUMN_MAP,
        {
            "fund_number",
            "quarter_end",
            "committed_capital",
            "paid_in_capital",
            "distributed_capital",
            "nav",
            "unfunded_commitment",
        },
        date_cols={"quarter_end"},
        float_cols={
            "committed_capital",
            "paid_in_capital",
            "distributed_capital",
            "nav",
            "unfunded_commitment",
        },
    )

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        fund_number = clean_str(row.get("fund_number"))
        quarter_end = clean_val(row.get("quarter_end"))
        if fund_number is None or quarter_end is None:
            msg = f"Fund Quarterly row {row_num}: skipped — Fund and Quarter End are required."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, firm_id, row_num, None, "warning", msg, row_payload)
            continue

        db.session.add(
            FundQuarterSnapshot(
                fund_number=fund_number,
                quarter_end=quarter_end,
                committed_capital=clean_val(row.get("committed_capital")),
                paid_in_capital=clean_val(row.get("paid_in_capital")),
                distributed_capital=clean_val(row.get("distributed_capital")),
                nav=clean_val(row.get("nav")),
                unfunded_commitment=clean_val(row.get("unfunded_commitment")),
                firm_id=firm_id,
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_underwrite_sheet(df, lookup, firm_id, issue_report_id, batch_id, errors):
    count = 0
    df = _normalize_optional_df(
        df,
        UNDERWRITE_COLUMN_MAP,
        {
            "company_name",
            "fund_number",
            "baseline_date",
            "target_irr",
            "target_moic",
            "target_hold_years",
            "target_exit_multiple",
            "target_revenue_cagr",
            "target_ebitda_cagr",
        },
        date_cols={"baseline_date"},
        float_cols={
            "target_irr",
            "target_moic",
            "target_hold_years",
            "target_exit_multiple",
            "target_revenue_cagr",
            "target_ebitda_cagr",
        },
    )

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        deal, reason = _match_deal(row, lookup)
        if deal is None:
            msg = f"Underwrite row {row_num}: skipped — {reason}."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, firm_id, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue

        db.session.add(
            DealUnderwriteBaseline(
                deal_id=deal.id,
                baseline_date=clean_val(row.get("baseline_date")),
                target_irr=clean_val(row.get("target_irr")),
                target_moic=clean_val(row.get("target_moic")),
                target_hold_years=clean_val(row.get("target_hold_years")),
                target_exit_multiple=clean_val(row.get("target_exit_multiple")),
                target_revenue_cagr=clean_val(row.get("target_revenue_cagr")),
                target_ebitda_cagr=clean_val(row.get("target_ebitda_cagr")),
                firm_id=firm_id,
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_optional_sheets(workbook, deals_sheet_name, firm_id, issue_report_id, batch_id, errors):
    # Limit optional-sheet matching to deals inserted by the current upload batch
    # to avoid accidental collisions against historical rows in the same firm.
    lookup = _build_deal_lookup(firm_id, upload_batch=batch_id)
    counts = {
        "cashflows": 0,
        "deal_quarterly": 0,
        "fund_quarterly": 0,
        "underwrite": 0,
    }

    for key, aliases in (
        ("cashflows", SHEET_ALIASES["cashflows"]),
        ("deal_quarterly", SHEET_ALIASES["deal_quarterly"]),
        ("fund_quarterly", SHEET_ALIASES["fund_quarterly"]),
        ("underwrite", SHEET_ALIASES["underwrite"]),
    ):
        sheet_name, df = _find_sheet(workbook, aliases)
        if df is None:
            continue
        if deals_sheet_name and _normalize_sheet_name(sheet_name) == _normalize_sheet_name(deals_sheet_name):
            continue

        if key == "cashflows":
            counts[key] = _parse_cashflows_sheet(df, lookup, firm_id, issue_report_id, batch_id, errors)
        elif key == "deal_quarterly":
            counts[key] = _parse_deal_quarter_sheet(df, lookup, firm_id, issue_report_id, batch_id, errors)
        elif key == "fund_quarterly":
            counts[key] = _parse_fund_quarter_sheet(df, firm_id, issue_report_id, batch_id, errors)
        elif key == "underwrite":
            counts[key] = _parse_underwrite_sheet(df, lookup, firm_id, issue_report_id, batch_id, errors)

    return counts


def _fund_filter_expr(column, fund_name):
    if fund_name is None:
        return column.is_(None)
    return column == fund_name


def _replace_existing_fund_data(firm_id, fund_names):
    replaced = {}
    for fund_name in sorted(fund_names, key=lambda v: (v is None, v or "")):
        fund_display = fund_name or "Unknown Fund"
        deal_ids = [
            row[0]
            for row in db.session.query(Deal.id)
            .filter(Deal.firm_id == firm_id, _fund_filter_expr(Deal.fund_number, fund_name))
            .all()
        ]
        if deal_ids:
            DealCashflowEvent.query.filter(
                DealCashflowEvent.firm_id == firm_id,
                DealCashflowEvent.deal_id.in_(deal_ids),
            ).delete(synchronize_session=False)
            DealQuarterSnapshot.query.filter(
                DealQuarterSnapshot.firm_id == firm_id,
                DealQuarterSnapshot.deal_id.in_(deal_ids),
            ).delete(synchronize_session=False)
            DealUnderwriteBaseline.query.filter(
                DealUnderwriteBaseline.firm_id == firm_id,
                DealUnderwriteBaseline.deal_id.in_(deal_ids),
            ).delete(synchronize_session=False)

        FundQuarterSnapshot.query.filter(
            FundQuarterSnapshot.firm_id == firm_id,
            _fund_filter_expr(FundQuarterSnapshot.fund_number, fund_name),
        ).delete(synchronize_session=False)
        deleted_deals = Deal.query.filter(
            Deal.firm_id == firm_id,
            _fund_filter_expr(Deal.fund_number, fund_name),
        ).delete(synchronize_session=False)
        replaced[fund_display] = deleted_deals

    return replaced


def _slugify_firm_name(name):
    token = re.sub(r"[^a-z0-9]+", "-", (name or "").strip().lower()).strip("-")
    return token or "firm"


def _ensure_unique_firm_slug(base_slug):
    candidate = base_slug
    idx = 2
    while Firm.query.filter_by(slug=candidate).first() is not None:
        candidate = f"{base_slug}-{idx}"
        idx += 1
    return candidate


def _resolve_or_create_firm(firm_name):
    normalized = clean_str(firm_name)
    if not normalized:
        return None

    existing = Firm.query.filter_by(name=normalized).first()
    if existing is not None:
        return existing

    slug = _ensure_unique_firm_slug(_slugify_firm_name(normalized))
    firm = Firm(name=normalized, slug=slug)
    db.session.add(firm)
    db.session.flush()
    return firm


def parse_deals(file_path, uploader_user_id=None, replace_mode="replace_fund"):
    """Parse deal-level template and insert rows.

    Supports a multi-sheet workbook:
    - Deals (required)
    - Cashflows (optional)
    - Deal Quarterly (optional)
    - Fund Quarterly (optional)
    - Underwrite (optional)

    Args:
        file_path: Local path to uploaded workbook.
        uploader_user_id: Optional uploader identity for audit extensions.
        replace_mode: "replace_fund" (default) or append-like mode.

    Returns: {success, errors, batch_id, bridge_complete, duplicates_skipped,
              quarantined_count, issue_report_id, supplemental_counts, replaced_funds}
    """
    batch_id = str(uuid.uuid4())[:8]
    issue_report_id = str(uuid.uuid4())

    workbook = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
    if not workbook:
        return {
            "success": 0,
            "errors": ["Workbook is empty."],
            "batch_id": batch_id,
            "bridge_complete": 0,
            "duplicates_skipped": 0,
            "quarantined_count": 0,
            "issue_report_id": issue_report_id,
            "supplemental_counts": {"cashflows": 0, "deal_quarterly": 0, "fund_quarterly": 0, "underwrite": 0},
        }

    deals_sheet_name, df = _select_deals_sheet(workbook)
    df.columns = [str(c).strip().lower() for c in df.columns]

    rename_map = {col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP}
    df = df.rename(columns=rename_map)

    if "company_name" not in df.columns:
        return {
            "success": 0,
            "errors": ["Could not find a 'Company Name' (or similar) column in the spreadsheet."],
            "batch_id": batch_id,
            "bridge_complete": 0,
            "duplicates_skipped": 0,
            "quarantined_count": 0,
            "issue_report_id": issue_report_id,
            "supplemental_counts": {"cashflows": 0, "deal_quarterly": 0, "fund_quarterly": 0, "underwrite": 0},
        }

    if "firm_name" not in df.columns:
        return {
            "success": 0,
            "errors": ["Could not find a required 'Firm Name' column in the Deals sheet."],
            "batch_id": batch_id,
            "bridge_complete": 0,
            "duplicates_skipped": 0,
            "quarantined_count": 0,
            "issue_report_id": issue_report_id,
            "supplemental_counts": {"cashflows": 0, "deal_quarterly": 0, "fund_quarterly": 0, "underwrite": 0},
        }

    df = df[[c for c in df.columns if c in VALID_FIELDS]]

    for col in DATE_COLS & set(df.columns):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    for col in FLOAT_COLS & set(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in INT_COLS & set(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in STR_COLS & set(df.columns):
        df[col] = df[col].astype(str).replace("nan", None)

    non_empty_mask = ~df.isna().all(axis=1)
    firm_values = []
    missing_firm_rows = []
    for idx, row in df[non_empty_mask].iterrows():
        firm_val = clean_str(row.get("firm_name"))
        if firm_val:
            firm_values.append(firm_val)
        else:
            missing_firm_rows.append(idx + 2)

    if missing_firm_rows:
        return {
            "success": 0,
            "errors": [f"Firm Name is required on Deals rows: {', '.join(str(r) for r in missing_firm_rows[:20])}"],
            "batch_id": batch_id,
            "bridge_complete": 0,
            "duplicates_skipped": 0,
            "quarantined_count": len(missing_firm_rows),
            "issue_report_id": issue_report_id,
            "supplemental_counts": {"cashflows": 0, "deal_quarterly": 0, "fund_quarterly": 0, "underwrite": 0},
            "replaced_funds": {},
        }

    distinct_firms = sorted(set(firm_values))
    if len(distinct_firms) != 1:
        return {
            "success": 0,
            "errors": [
                "Deals sheet must contain exactly one Firm Name per upload. "
                f"Found {len(distinct_firms)}: {', '.join(distinct_firms[:10])}"
            ],
            "batch_id": batch_id,
            "bridge_complete": 0,
            "duplicates_skipped": 0,
            "quarantined_count": 0,
            "issue_report_id": issue_report_id,
            "supplemental_counts": {"cashflows": 0, "deal_quarterly": 0, "fund_quarterly": 0, "underwrite": 0},
            "replaced_funds": {},
        }

    firm = _resolve_or_create_firm(distinct_firms[0])
    firm_id = firm.id

    if replace_mode == "replace_fund":
        uploaded_funds = set()
        if "fund_number" in df.columns:
            for fund_raw in df["fund_number"].tolist():
                uploaded_funds.add(clean_str(fund_raw))
        else:
            uploaded_funds.add(None)
        replaced_funds = _replace_existing_fund_data(firm_id, uploaded_funds)
        db.session.flush()
        db.session.expunge_all()
    else:
        replaced_funds = {}

    existing_keys = {
        (d.company_name.strip().lower(), (d.fund_number or "").strip().lower())
        for d in Deal.query.filter_by(firm_id=firm_id).all()
        if d.company_name
    }

    success = 0
    errors = []
    bridge_complete = 0
    duplicates_skipped = 0
    quarantined_count = 0

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        company = row.get("company_name")

        if pd.isna(company) or company is None or str(company).strip() in ("", "None"):
            msg = f"Row {row_num}: Quarantined — missing company name."
            errors.append(msg)
            quarantined_count += 1
            _record_issue(issue_report_id, batch_id, firm_id, row_num, None, "error", msg, row_payload)
            continue

        fund_val = clean_str(row.get("fund_number")) or ""
        deal_key = (str(company).strip().lower(), fund_val.strip().lower())
        if deal_key in existing_keys:
            msg = f"Row {row_num}: Skipped duplicate — '{company}' already exists."
            errors.append(msg)
            duplicates_skipped += 1
            _record_issue(issue_report_id, batch_id, firm_id, row_num, str(company).strip(), "warning", msg, row_payload)
            continue
        existing_keys.add(deal_key)

        year_invested_val = clean_val(row.get("year_invested"))
        if year_invested_val is not None:
            try:
                year_invested_val = int(year_invested_val)
            except (TypeError, ValueError):
                year_invested_val = None

        investment_date = clean_val(row.get("investment_date"))
        if year_invested_val is None and investment_date is not None:
            year_invested_val = investment_date.year

        ownership = _normalize_ownership(clean_val(row.get("ownership_pct")))
        status_val = clean_str(row.get("status")) or "Unrealized"
        exit_type_val = clean_str(row.get("exit_type"))
        if exit_type_val is None:
            exit_type_val = "Unrealized / N.A." if status_val.lower() == "unrealized" else "Not Specified"

        lead_partner_val = clean_str(row.get("lead_partner")) or "Unassigned"
        security_type_val = clean_str(row.get("security_type")) or "Common Equity"
        deal_type_val = clean_str(row.get("deal_type")) or "Platform"
        entry_channel_val = clean_str(row.get("entry_channel")) or "Unknown"

        try:
            deal = Deal(
                company_name=str(company).strip(),
                fund_number=clean_str(row.get("fund_number")),
                sector=clean_str(row.get("sector")),
                geography=clean_str(row.get("geography")) or "Unknown",
                status=status_val,
                exit_type=exit_type_val,
                lead_partner=lead_partner_val,
                security_type=security_type_val,
                deal_type=deal_type_val,
                entry_channel=entry_channel_val,
                firm_id=firm_id,
                investment_date=investment_date,
                year_invested=year_invested_val,
                exit_date=clean_val(row.get("exit_date")),
                equity_invested=clean_val(row.get("equity_invested")),
                fund_size=clean_val(row.get("fund_size")),
                realized_value=clean_val(row.get("realized_value")),
                unrealized_value=clean_val(row.get("unrealized_value")),
                ownership_pct=ownership,
                entry_revenue=clean_val(row.get("entry_revenue")),
                entry_ebitda=clean_val(row.get("entry_ebitda")),
                entry_enterprise_value=clean_val(row.get("entry_enterprise_value")),
                entry_net_debt=clean_val(row.get("entry_net_debt")),
                exit_revenue=clean_val(row.get("exit_revenue")),
                exit_ebitda=clean_val(row.get("exit_ebitda")),
                exit_enterprise_value=clean_val(row.get("exit_enterprise_value")),
                exit_net_debt=clean_val(row.get("exit_net_debt")),
                irr=clean_val(row.get("irr")),
                net_irr=clean_val(row.get("net_irr")),
                net_moic=clean_val(row.get("net_moic")),
                net_dpi=clean_val(row.get("net_dpi")),
                upload_batch=batch_id,
            )

            if deal.equity_invested is not None and deal.equity_invested < 0:
                msg = f"Row {row_num}: Quarantined — negative equity invested ({deal.equity_invested})."
                errors.append(msg)
                quarantined_count += 1
                _record_issue(issue_report_id, batch_id, firm_id, row_num, deal.company_name, "error", msg, row_payload)
                continue

            if deal.investment_date and deal.exit_date and deal.exit_date < deal.investment_date:
                msg = (
                    f"Row {row_num}: Quarantined — exit date ({deal.exit_date}) is before "
                    f"investment date ({deal.investment_date})."
                )
                errors.append(msg)
                quarantined_count += 1
                _record_issue(issue_report_id, batch_id, firm_id, row_num, deal.company_name, "error", msg, row_payload)
                continue

            if ownership is not None and ownership > 1.5:
                msg = f"Row {row_num}: Ownership {ownership:.2%} is above expected range."
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)

            for warn in _warn_extreme_multiples(deal):
                msg = f"Row {row_num}: {warn}"
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)

            bridge_fields = [
                deal.entry_revenue,
                deal.entry_ebitda,
                deal.entry_enterprise_value,
                deal.entry_net_debt,
                deal.exit_revenue,
                deal.exit_ebitda,
                deal.exit_enterprise_value,
                deal.exit_net_debt,
            ]
            if not all(v is not None for v in bridge_fields):
                msg = f"Row {row_num}: Missing entry/exit fields for full bridge decomposition."
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)
            else:
                bridge_complete += 1

            db.session.add(deal)
            success += 1
        except Exception as exc:
            msg = f"Row {row_num}: Quarantined — {str(exc)}"
            errors.append(msg)
            quarantined_count += 1
            _record_issue(issue_report_id, batch_id, firm_id, row_num, str(company).strip(), "error", msg, row_payload)

    db.session.flush()
    supplemental_counts = _parse_optional_sheets(workbook, deals_sheet_name, firm_id, issue_report_id, batch_id, errors)
    db.session.commit()

    return {
        "success": success,
        "errors": errors,
        "batch_id": batch_id,
        "bridge_complete": bridge_complete,
        "duplicates_skipped": duplicates_skipped,
        "quarantined_count": quarantined_count,
        "issue_report_id": issue_report_id,
        "supplemental_counts": supplemental_counts,
        "replaced_funds": replaced_funds,
        "firm_name": firm.name,
        "firm_id": firm.id,
    }

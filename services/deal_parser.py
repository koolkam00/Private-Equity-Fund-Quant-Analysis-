import json
import math
import re
import uuid
from collections import defaultdict
from datetime import date

import pandas as pd

from models import (
    Deal,
    DealCashflowEvent,
    DealQuarterSnapshot,
    DealUnderwriteBaseline,
    Firm,
    FundCashflow,
    FundMetadata,
    FundQuarterSnapshot,
    PublicMarketIndexLevel,
    TeamFirmAccess,
    UploadIssue,
    db,
)
from services.fx_rates import resolve_rate_to_usd
from services.utils import DEFAULT_CURRENCY_CODE, clean_str, clean_val, normalize_currency_code


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
    "currency": "firm_currency",
    "firm currency": "firm_currency",
    "base currency": "firm_currency",
    "reporting currency": "firm_currency",
    "performance currency": "performance_currency",
    "perf currency": "performance_currency",
    "equity currency": "performance_currency",
    "investment currency": "performance_currency",
    "financial metric currency": "financial_metric_currency",
    "metric currency": "financial_metric_currency",
    "operating currency": "financial_metric_currency",
    "financials currency": "financial_metric_currency",
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
    "as of date": "as_of_date",
    "as-of date": "as_of_date",
    "as of": "as_of_date",
    "report date": "as_of_date",
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
    # Acquired / bolt-on metrics
    "acquired revenue": "acquired_revenue",
    "bolt-on revenue": "acquired_revenue",
    "add-on revenue": "acquired_revenue",
    "acquisition revenue": "acquired_revenue",
    "acquired ebitda": "acquired_ebitda",
    "bolt-on ebitda": "acquired_ebitda",
    "add-on ebitda": "acquired_ebitda",
    "acquisition ebitda": "acquired_ebitda",
    "acquired tev": "acquired_tev",
    "acquired ev": "acquired_tev",
    "acquired enterprise value": "acquired_tev",
    "bolt-on tev": "acquired_tev",
    "bolt-on ev": "acquired_tev",
    "add-on tev": "acquired_tev",
    "add-on ev": "acquired_tev",
    "acquisition tev": "acquired_tev",
    "acquisition ev": "acquired_tev",
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
    "firm_currency",
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
    "as_of_date",
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
    "acquired_revenue",
    "acquired_ebitda",
    "acquired_tev",
    "irr",
    "net_irr",
    "net_moic",
    "net_dpi",
    "performance_currency",
    "financial_metric_currency",
}

# Currency group constants — which Deal fields belong to each FX rate
PERF_CURRENCY_COLS = {"equity_invested", "realized_value", "unrealized_value", "fund_size"}
FIN_METRIC_CURRENCY_COLS = {
    "entry_revenue", "entry_ebitda", "entry_enterprise_value", "entry_net_debt",
    "exit_revenue", "exit_ebitda", "exit_enterprise_value", "exit_net_debt",
    "acquired_revenue", "acquired_ebitda", "acquired_tev",
}

DATE_COLS = {"investment_date", "exit_date", "as_of_date"}
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
    "acquired_revenue",
    "acquired_ebitda",
    "acquired_tev",
    "irr",
    "net_irr",
    "net_moic",
    "net_dpi",
}
INT_COLS = {"year_invested"}
STR_COLS = {"company_name", "firm_name", "firm_currency", "fund_number", "sector", "geography", "status"}
STR_COLS |= {"exit_type", "lead_partner", "security_type", "deal_type", "entry_channel"}
STR_COLS |= {"performance_currency", "financial_metric_currency"}

SHEET_ALIASES = {
    "deals": {"deals", "deal", "dealdata", "sheet1", "portfolio"},
    "cashflows": {"cashflows", "cashflow", "cash flows"},
    "deal_quarterly": {"deal quarterly", "dealquarterly", "quarterly deals", "deal snapshots", "deal_snapshot"},
    "fund_quarterly": {"fund quarterly", "fundquarterly", "fund snapshots", "fund_snapshot"},
    "underwrite": {"underwrite", "underwrites", "underwriting"},
    "fund_metadata": {"fund metadata", "fund_meta", "fund details", "fund info"},
    "fund_cashflows": {"fund cashflows", "fund cashflow", "fund cash flows"},
    "public_market_benchmarks": {"public market benchmarks", "public markets", "public market", "pme benchmarks"},
}

CASHFLOW_COLUMN_MAP = {
    "firm": "firm_name",
    "firm name": "firm_name",
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
    "firm": "firm_name",
    "firm name": "firm_name",
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
    "firm": "firm_name",
    "firm name": "firm_name",
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
    "firm": "firm_name",
    "firm name": "firm_name",
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

FUND_METADATA_COLUMN_MAP = {
    "firm": "firm_name",
    "firm name": "firm_name",
    "fund": "fund_number",
    "fund name": "fund_number",
    "fund number": "fund_number",
    "vintage": "vintage_year",
    "vintage year": "vintage_year",
    "strategy": "strategy",
    "region": "region_focus",
    "region focus": "region_focus",
    "fund size": "fund_size",
    "first close": "first_close_date",
    "first close date": "first_close_date",
    "final close": "final_close_date",
    "final close date": "final_close_date",
    "manager": "manager_name",
    "manager name": "manager_name",
    "benchmark peer group": "benchmark_peer_group",
    "peer group": "benchmark_peer_group",
    "status": "status",
}

FUND_CASHFLOW_COLUMN_MAP = {
    "firm": "firm_name",
    "firm name": "firm_name",
    "fund": "fund_number",
    "fund name": "fund_number",
    "fund number": "fund_number",
    "event date": "event_date",
    "date": "event_date",
    "cashflow date": "event_date",
    "event type": "event_type",
    "type": "event_type",
    "amount": "amount",
    "nav after event": "nav_after_event",
    "nav": "nav_after_event",
    "currency": "currency_code",
    "currency code": "currency_code",
}

PUBLIC_MARKET_BENCHMARK_COLUMN_MAP = {
    "benchmark": "benchmark_code",
    "benchmark code": "benchmark_code",
    "index code": "benchmark_code",
    "code": "benchmark_code",
    "date": "level_date",
    "level date": "level_date",
    "index date": "level_date",
    "level": "level",
    "index level": "level",
    "currency": "currency_code",
    "currency code": "currency_code",
    "source": "source",
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


def _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, company, severity, message, payload):
    db.session.add(
        UploadIssue(
            issue_report_id=issue_report_id,
            team_id=team_id,
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


def _firm_lookup_key(name):
    normalized = clean_str(name)
    if normalized is None:
        return None
    return normalized.lower()


def _resolve_optional_firm_for_row(row, firm_name_to_id, default_firm_id, require_firm_name):
    firm_name = clean_str(row.get("firm_name"))
    if firm_name is None:
        if require_firm_name:
            return None, "Firm Name is required for multi-firm supplemental uploads."
        if default_firm_id is None:
            return None, "Firm Name is required because a default firm scope is unavailable."
        return default_firm_id, None

    firm_id = firm_name_to_id.get(_firm_lookup_key(firm_name))
    if firm_id is None:
        return None, f"Firm '{firm_name}' was not found in uploaded Deals rows."
    return firm_id, None


def _parse_cashflows_sheet(
    df,
    lookup_by_firm,
    firm_name_to_id,
    default_firm_id,
    require_firm_name,
    team_id,
    issue_report_id,
    batch_id,
    errors,
):
    count = 0
    df = _normalize_optional_df(
        df,
        CASHFLOW_COLUMN_MAP,
        {"firm_name", "company_name", "fund_number", "event_date", "event_type", "amount", "notes"},
        date_cols={"event_date"},
        float_cols={"amount"},
    )
    if require_firm_name and "firm_name" not in df.columns:
        msg = "Cashflows sheet requires a Firm Name column when Deals sheet includes multiple firms."
        errors.append(msg)
        _record_issue(issue_report_id, batch_id, team_id, None, None, None, "warning", msg, {})
        return 0

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        firm_id, firm_err = _resolve_optional_firm_for_row(row, firm_name_to_id, default_firm_id, require_firm_name)
        if firm_id is None:
            msg = f"Cashflows row {row_num}: skipped — {firm_err}"
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, None, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue
        lookup = lookup_by_firm.get(firm_id)
        if lookup is None:
            msg = f"Cashflows row {row_num}: skipped — could not resolve firm upload scope."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue

        deal, reason = _match_deal(row, lookup)
        if deal is None:
            msg = f"Cashflows row {row_num}: skipped — {reason}."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue

        event_date = clean_val(row.get("event_date"))
        amount = clean_val(row.get("amount"))
        event_type = clean_str(row.get("event_type"))
        if event_date is None or amount is None or event_type is None:
            msg = f"Cashflows row {row_num}: skipped — requires Event Date, Event Type, and Amount."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)
            continue

        # Apply performance currency conversion (cashflow amounts are equity-level)
        amount_val = float(amount)
        perf_rate = getattr(deal, "perf_fx_rate_to_usd", None)
        if perf_rate is not None and perf_rate != 1.0:
            amount_val = amount_val * perf_rate

        db.session.add(
            DealCashflowEvent(
                deal_id=deal.id,
                event_date=event_date,
                event_type=event_type,
                amount=amount_val,
                notes=clean_str(row.get("notes")),
                team_id=team_id,
                firm_id=firm_id,
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_deal_quarter_sheet(
    df,
    lookup_by_firm,
    firm_name_to_id,
    default_firm_id,
    require_firm_name,
    team_id,
    issue_report_id,
    batch_id,
    errors,
):
    count = 0
    df = _normalize_optional_df(
        df,
        DEAL_QUARTER_COLUMN_MAP,
        {
            "firm_name",
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
    if require_firm_name and "firm_name" not in df.columns:
        msg = "Deal Quarterly sheet requires a Firm Name column when Deals sheet includes multiple firms."
        errors.append(msg)
        _record_issue(issue_report_id, batch_id, team_id, None, None, None, "warning", msg, {})
        return 0

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        firm_id, firm_err = _resolve_optional_firm_for_row(row, firm_name_to_id, default_firm_id, require_firm_name)
        if firm_id is None:
            msg = f"Deal Quarterly row {row_num}: skipped — {firm_err}"
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, None, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue
        lookup = lookup_by_firm.get(firm_id)
        if lookup is None:
            msg = f"Deal Quarterly row {row_num}: skipped — could not resolve firm upload scope."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue

        deal, reason = _match_deal(row, lookup)
        if deal is None:
            msg = f"Deal Quarterly row {row_num}: skipped — {reason}."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue

        quarter_end = clean_val(row.get("quarter_end"))
        if quarter_end is None:
            msg = f"Deal Quarterly row {row_num}: skipped — Quarter End is required."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)
            continue

        # Apply financial metric currency conversion to quarterly snapshot values
        fin_rate = getattr(deal, "fin_fx_rate_to_usd", None)
        _apply_fin = fin_rate is not None and fin_rate != 1.0

        def _fx_val(raw):
            v = clean_val(raw)
            return (v * fin_rate) if (v is not None and _apply_fin) else v

        db.session.add(
            DealQuarterSnapshot(
                deal_id=deal.id,
                quarter_end=quarter_end,
                revenue=_fx_val(row.get("revenue")),
                ebitda=_fx_val(row.get("ebitda")),
                enterprise_value=_fx_val(row.get("enterprise_value")),
                net_debt=_fx_val(row.get("net_debt")),
                equity_value=_fx_val(row.get("equity_value")),
                valuation_basis=clean_str(row.get("valuation_basis")),
                source=clean_str(row.get("source")),
                team_id=team_id,
                firm_id=firm_id,
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_fund_quarter_sheet(
    df,
    firm_name_to_id,
    default_firm_id,
    require_firm_name,
    team_id,
    issue_report_id,
    batch_id,
    errors,
):
    count = 0
    df = _normalize_optional_df(
        df,
        FUND_QUARTER_COLUMN_MAP,
        {
            "firm_name",
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
    if require_firm_name and "firm_name" not in df.columns:
        msg = "Fund Quarterly sheet requires a Firm Name column when Deals sheet includes multiple firms."
        errors.append(msg)
        _record_issue(issue_report_id, batch_id, team_id, None, None, None, "warning", msg, {})
        return 0

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        firm_id, firm_err = _resolve_optional_firm_for_row(row, firm_name_to_id, default_firm_id, require_firm_name)
        if firm_id is None:
            msg = f"Fund Quarterly row {row_num}: skipped — {firm_err}"
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, None, row_num, None, "warning", msg, row_payload)
            continue

        fund_number = clean_str(row.get("fund_number"))
        quarter_end = clean_val(row.get("quarter_end"))
        if fund_number is None or quarter_end is None:
            msg = f"Fund Quarterly row {row_num}: skipped — Fund and Quarter End are required."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, None, "warning", msg, row_payload)
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
                team_id=team_id,
                firm_id=firm_id,
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_underwrite_sheet(
    df,
    lookup_by_firm,
    firm_name_to_id,
    default_firm_id,
    require_firm_name,
    team_id,
    issue_report_id,
    batch_id,
    errors,
):
    count = 0
    df = _normalize_optional_df(
        df,
        UNDERWRITE_COLUMN_MAP,
        {
            "firm_name",
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
    if require_firm_name and "firm_name" not in df.columns:
        msg = "Underwrite sheet requires a Firm Name column when Deals sheet includes multiple firms."
        errors.append(msg)
        _record_issue(issue_report_id, batch_id, team_id, None, None, None, "warning", msg, {})
        return 0

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        firm_id, firm_err = _resolve_optional_firm_for_row(row, firm_name_to_id, default_firm_id, require_firm_name)
        if firm_id is None:
            msg = f"Underwrite row {row_num}: skipped — {firm_err}"
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, None, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue
        lookup = lookup_by_firm.get(firm_id)
        if lookup is None:
            msg = f"Underwrite row {row_num}: skipped — could not resolve firm upload scope."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
            continue

        deal, reason = _match_deal(row, lookup)
        if deal is None:
            msg = f"Underwrite row {row_num}: skipped — {reason}."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, clean_str(row.get("company_name")), "warning", msg, row_payload)
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
                team_id=team_id,
                firm_id=firm_id,
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_fund_metadata_sheet(
    df,
    firm_name_to_id,
    default_firm_id,
    require_firm_name,
    team_id,
    issue_report_id,
    batch_id,
    errors,
):
    count = 0
    df = _normalize_optional_df(
        df,
        FUND_METADATA_COLUMN_MAP,
        {
            "firm_name",
            "fund_number",
            "vintage_year",
            "strategy",
            "region_focus",
            "fund_size",
            "first_close_date",
            "final_close_date",
            "manager_name",
            "benchmark_peer_group",
            "status",
        },
        date_cols={"first_close_date", "final_close_date"},
        float_cols={"fund_size", "vintage_year"},
    )
    if require_firm_name and "firm_name" not in df.columns:
        msg = "Fund Metadata sheet requires a Firm Name column when Deals sheet includes multiple firms."
        errors.append(msg)
        _record_issue(issue_report_id, batch_id, team_id, None, None, None, "warning", msg, {})
        return 0

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        firm_id, firm_err = _resolve_optional_firm_for_row(row, firm_name_to_id, default_firm_id, require_firm_name)
        if firm_id is None:
            msg = f"Fund Metadata row {row_num}: skipped — {firm_err}"
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, None, row_num, None, "warning", msg, row_payload)
            continue

        fund_number = clean_str(row.get("fund_number"))
        if fund_number is None:
            msg = f"Fund Metadata row {row_num}: skipped — Fund Number is required."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, None, "warning", msg, row_payload)
            continue

        vintage_year = clean_val(row.get("vintage_year"))
        if vintage_year is not None:
            try:
                vintage_year = int(float(vintage_year))
            except (TypeError, ValueError):
                vintage_year = None

        existing = FundMetadata.query.filter_by(
            team_id=team_id,
            firm_id=firm_id,
            fund_number=fund_number,
        ).first()
        if existing is None:
            existing = FundMetadata(
                team_id=team_id,
                firm_id=firm_id,
                fund_number=fund_number,
            )
            db.session.add(existing)

        existing.vintage_year = vintage_year
        existing.strategy = clean_str(row.get("strategy"))
        existing.region_focus = clean_str(row.get("region_focus"))
        existing.fund_size = clean_val(row.get("fund_size"))
        existing.first_close_date = clean_val(row.get("first_close_date"))
        existing.final_close_date = clean_val(row.get("final_close_date"))
        existing.manager_name = clean_str(row.get("manager_name"))
        existing.benchmark_peer_group = clean_str(row.get("benchmark_peer_group"))
        existing.status = clean_str(row.get("status"))
        existing.upload_batch = batch_id
        count += 1

    return count


def _parse_fund_cashflows_sheet(
    df,
    firm_name_to_id,
    default_firm_id,
    require_firm_name,
    team_id,
    issue_report_id,
    batch_id,
    errors,
):
    count = 0
    df = _normalize_optional_df(
        df,
        FUND_CASHFLOW_COLUMN_MAP,
        {
            "firm_name",
            "fund_number",
            "event_date",
            "event_type",
            "amount",
            "nav_after_event",
            "currency_code",
        },
        date_cols={"event_date"},
        float_cols={"amount", "nav_after_event"},
    )
    if require_firm_name and "firm_name" not in df.columns:
        msg = "Fund Cashflows sheet requires a Firm Name column when Deals sheet includes multiple firms."
        errors.append(msg)
        _record_issue(issue_report_id, batch_id, team_id, None, None, None, "warning", msg, {})
        return 0

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        firm_id, firm_err = _resolve_optional_firm_for_row(row, firm_name_to_id, default_firm_id, require_firm_name)
        if firm_id is None:
            msg = f"Fund Cashflows row {row_num}: skipped — {firm_err}"
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, None, row_num, None, "warning", msg, row_payload)
            continue

        fund_number = clean_str(row.get("fund_number"))
        event_date = clean_val(row.get("event_date"))
        event_type = clean_str(row.get("event_type"))
        amount = clean_val(row.get("amount"))
        if fund_number is None or event_date is None or event_type is None or amount is None:
            msg = f"Fund Cashflows row {row_num}: skipped — Fund Number, Event Date, Event Type, and Amount are required."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, None, "warning", msg, row_payload)
            continue

        db.session.add(
            FundCashflow(
                fund_number=fund_number,
                firm_id=firm_id,
                team_id=team_id,
                event_date=event_date,
                event_type=event_type,
                amount=float(amount),
                nav_after_event=clean_val(row.get("nav_after_event")),
                currency_code=normalize_currency_code(row.get("currency_code"), default=None),
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_public_market_benchmarks_sheet(
    df,
    team_id,
    issue_report_id,
    batch_id,
    errors,
):
    count = 0
    df = _normalize_optional_df(
        df,
        PUBLIC_MARKET_BENCHMARK_COLUMN_MAP,
        {"benchmark_code", "level_date", "level", "currency_code", "source"},
        date_cols={"level_date"},
        float_cols={"level"},
    )

    normalized_codes = set()
    for _, row in df.iterrows():
        code = clean_str(row.get("benchmark_code"))
        if code is not None:
            normalized_codes.add(code)

    if normalized_codes:
        PublicMarketIndexLevel.query.filter(
            PublicMarketIndexLevel.team_id == team_id,
            PublicMarketIndexLevel.benchmark_code.in_(sorted(normalized_codes)),
        ).delete(synchronize_session=False)

    seen_keys = set()
    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        benchmark_code = clean_str(row.get("benchmark_code"))
        level_date = clean_val(row.get("level_date"))
        level = clean_val(row.get("level"))
        if benchmark_code is None or level_date is None or level is None:
            msg = f"Public Market Benchmarks row {row_num}: skipped — Benchmark Code, Date, and Level are required."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, None, row_num, None, "warning", msg, row_payload)
            continue

        key = (benchmark_code.lower(), level_date)
        if key in seen_keys:
            msg = f"Public Market Benchmarks row {row_num}: skipped — duplicate benchmark/date key."
            errors.append(msg)
            _record_issue(issue_report_id, batch_id, team_id, None, row_num, None, "warning", msg, row_payload)
            continue
        seen_keys.add(key)

        db.session.add(
            PublicMarketIndexLevel(
                team_id=team_id,
                benchmark_code=benchmark_code,
                level_date=level_date,
                level=float(level),
                currency_code=normalize_currency_code(row.get("currency_code"), default=None),
                source=clean_str(row.get("source")),
                upload_batch=batch_id,
            )
        )
        count += 1

    return count


def _parse_optional_sheets(
    workbook,
    deals_sheet_name,
    team_id,
    firm_name_to_id,
    default_firm_id,
    require_firm_name,
    issue_report_id,
    batch_id,
    errors,
):
    # Limit optional-sheet matching to deals inserted by the current upload batch
    # to avoid accidental collisions against historical rows in the same firm.
    lookup_by_firm = {}
    for firm_id in sorted(set(firm_name_to_id.values())):
        lookup_by_firm[firm_id] = _build_deal_lookup(firm_id, upload_batch=batch_id)

    counts = {
        "cashflows": 0,
        "deal_quarterly": 0,
        "fund_quarterly": 0,
        "underwrite": 0,
        "fund_metadata": 0,
        "fund_cashflows": 0,
        "public_market_benchmarks": 0,
    }

    for key, aliases in (
        ("cashflows", SHEET_ALIASES["cashflows"]),
        ("deal_quarterly", SHEET_ALIASES["deal_quarterly"]),
        ("fund_quarterly", SHEET_ALIASES["fund_quarterly"]),
        ("underwrite", SHEET_ALIASES["underwrite"]),
        ("fund_metadata", SHEET_ALIASES["fund_metadata"]),
        ("fund_cashflows", SHEET_ALIASES["fund_cashflows"]),
        ("public_market_benchmarks", SHEET_ALIASES["public_market_benchmarks"]),
    ):
        sheet_name, df = _find_sheet(workbook, aliases)
        if df is None:
            continue
        if deals_sheet_name and _normalize_sheet_name(sheet_name) == _normalize_sheet_name(deals_sheet_name):
            continue

        if key == "cashflows":
            counts[key] = _parse_cashflows_sheet(
                df,
                lookup_by_firm,
                firm_name_to_id,
                default_firm_id,
                require_firm_name,
                team_id,
                issue_report_id,
                batch_id,
                errors,
            )
        elif key == "deal_quarterly":
            counts[key] = _parse_deal_quarter_sheet(
                df,
                lookup_by_firm,
                firm_name_to_id,
                default_firm_id,
                require_firm_name,
                team_id,
                issue_report_id,
                batch_id,
                errors,
            )
        elif key == "fund_quarterly":
            counts[key] = _parse_fund_quarter_sheet(
                df,
                firm_name_to_id,
                default_firm_id,
                require_firm_name,
                team_id,
                issue_report_id,
                batch_id,
                errors,
            )
        elif key == "underwrite":
            counts[key] = _parse_underwrite_sheet(
                df,
                lookup_by_firm,
                firm_name_to_id,
                default_firm_id,
                require_firm_name,
                team_id,
                issue_report_id,
                batch_id,
                errors,
            )
        elif key == "fund_metadata":
            counts[key] = _parse_fund_metadata_sheet(
                df,
                firm_name_to_id,
                default_firm_id,
                require_firm_name,
                team_id,
                issue_report_id,
                batch_id,
                errors,
            )
        elif key == "fund_cashflows":
            counts[key] = _parse_fund_cashflows_sheet(
                df,
                firm_name_to_id,
                default_firm_id,
                require_firm_name,
                team_id,
                issue_report_id,
                batch_id,
                errors,
            )
        elif key == "public_market_benchmarks":
            counts[key] = _parse_public_market_benchmarks_sheet(
                df,
                team_id,
                issue_report_id,
                batch_id,
                errors,
            )

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
        FundMetadata.query.filter(
            FundMetadata.firm_id == firm_id,
            _fund_filter_expr(FundMetadata.fund_number, fund_name),
        ).delete(synchronize_session=False)
        FundCashflow.query.filter(
            FundCashflow.firm_id == firm_id,
            _fund_filter_expr(FundCashflow.fund_number, fund_name),
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


def _resolve_or_create_firm(firm_name, base_currency=DEFAULT_CURRENCY_CODE):
    normalized = clean_str(firm_name)
    if not normalized:
        return None
    normalized_currency = normalize_currency_code(base_currency, default=DEFAULT_CURRENCY_CODE) or DEFAULT_CURRENCY_CODE

    existing = Firm.query.filter_by(name=normalized).first()
    if existing is not None:
        if (existing.base_currency or "").upper() != normalized_currency:
            existing.base_currency = normalized_currency
            db.session.flush()
        return existing

    slug = _ensure_unique_firm_slug(_slugify_firm_name(normalized))
    firm = Firm(name=normalized, slug=slug, base_currency=normalized_currency)
    db.session.add(firm)
    db.session.flush()
    return firm


def _refresh_firm_fx_metadata(firm, upload_date=None):
    as_of = upload_date or date.today()
    code = normalize_currency_code(getattr(firm, "base_currency", None), default=DEFAULT_CURRENCY_CODE) or DEFAULT_CURRENCY_CODE
    firm.base_currency = code

    if code == DEFAULT_CURRENCY_CODE:
        firm.fx_rate_to_usd = 1.0
        firm.fx_rate_date = as_of
        firm.fx_rate_source = "Identity"
        firm.fx_last_status = "ok"
        db.session.flush()
        return {
            "ok": True,
            "fx_rate_to_usd": 1.0,
            "fx_rate_date": as_of,
            "fx_rate_source": "Identity",
            "fx_status": "ok",
            "fx_warning": None,
        }

    fx = resolve_rate_to_usd(code, as_of)
    if fx.get("ok"):
        firm.fx_rate_to_usd = fx.get("rate")
        firm.fx_rate_date = fx.get("effective_date")
        firm.fx_rate_source = fx.get("source")
        firm.fx_last_status = "ok"
        db.session.flush()
        return {
            "ok": True,
            "fx_rate_to_usd": firm.fx_rate_to_usd,
            "fx_rate_date": firm.fx_rate_date,
            "fx_rate_source": firm.fx_rate_source,
            "fx_status": "ok",
            "fx_warning": None,
        }

    firm.fx_rate_to_usd = None
    firm.fx_rate_date = None
    firm.fx_rate_source = fx.get("source")
    firm.fx_last_status = "lookup_failed"
    raw_warning = clean_str(fx.get("warning"))
    if raw_warning:
        warning = raw_warning.splitlines()[0].strip()
    else:
        warning = f"FX lookup failed [lookup_failed] for {code}->USD on {as_of.isoformat()}."
    if "showing native currency values" not in warning.lower():
        warning = f"{warning} Showing native currency values."
    db.session.flush()
    return {
        "ok": False,
        "fx_rate_to_usd": None,
        "fx_rate_date": None,
        "fx_rate_source": firm.fx_rate_source,
        "fx_status": "lookup_failed",
        "fx_warning": warning,
    }


def _ensure_team_firm_access(team_id, firm_id, created_by_user_id=None):
    if team_id is None or firm_id is None:
        return
    existing = TeamFirmAccess.query.filter_by(team_id=team_id, firm_id=firm_id).first()
    if existing is None:
        db.session.add(
            TeamFirmAccess(
                team_id=team_id,
                firm_id=firm_id,
                created_by_user_id=created_by_user_id,
            )
        )
        db.session.flush()


def parse_deals(file_path, team_id, uploader_user_id=None, replace_mode="replace_fund"):
    """Parse deal-level template and insert rows.

    Supports a multi-sheet workbook:
    - Deals (required)
      - Firm Name required on all non-empty rows
      - As Of Date required and consistent per firm
      - Firm Currency optional ISO-3 (defaults to USD) and consistent per firm
    - Cashflows (optional)
    - Deal Quarterly (optional)
    - Fund Quarterly (optional)
    - Underwrite (optional)
    - Fund Metadata (optional)
    - Fund Cashflows (optional)
    - Public Market Benchmarks (optional)

    Args:
        file_path: Local path to uploaded workbook.
        team_id: Team context for uploader/access mapping.
        uploader_user_id: Optional uploader identity for audit extensions.
        replace_mode: "replace_fund" (default) or append-like mode.

    Returns: {success, errors, batch_id, bridge_complete, duplicates_skipped,
              quarantined_count, issue_report_id, supplemental_counts, replaced_funds}
    """
    batch_id = str(uuid.uuid4())[:8]
    issue_report_id = str(uuid.uuid4())
    zero_supplemental = {
        "cashflows": 0,
        "deal_quarterly": 0,
        "fund_quarterly": 0,
        "underwrite": 0,
        "fund_metadata": 0,
        "fund_cashflows": 0,
        "public_market_benchmarks": 0,
    }

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
            "supplemental_counts": zero_supplemental,
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
            "supplemental_counts": zero_supplemental,
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
            "supplemental_counts": zero_supplemental,
        }

    if "as_of_date" not in df.columns:
        return {
            "success": 0,
            "errors": ["Could not find a required 'As Of Date' column in the Deals sheet."],
            "batch_id": batch_id,
            "bridge_complete": 0,
            "duplicates_skipped": 0,
            "quarantined_count": 0,
            "issue_report_id": issue_report_id,
            "supplemental_counts": zero_supplemental,
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
    non_empty_rows = df[non_empty_mask]
    firm_groups = {}
    missing_firm_rows = []
    for idx, row in non_empty_rows.iterrows():
        firm_val = clean_str(row.get("firm_name"))
        if firm_val:
            key = _firm_lookup_key(firm_val)
            group = firm_groups.setdefault(
                key,
                {
                    "firm_name": firm_val,
                    "row_indices": [],
                    "as_of_dates": set(),
                    "currency_values": [],
                    "perf_currency_values": [],
                    "fin_currency_by_row": {},
                    "missing_as_of_rows": [],
                },
            )
            group["row_indices"].append(idx)
            as_of_val = clean_val(row.get("as_of_date"))
            if as_of_val is None:
                group["missing_as_of_rows"].append(idx + 2)
            else:
                group["as_of_dates"].add(as_of_val)
            if "firm_currency" in df.columns:
                code = normalize_currency_code(row.get("firm_currency"), default=None)
                if code is None and clean_str(row.get("firm_currency")) is not None:
                    return {
                        "success": 0,
                        "errors": [
                            "Firm Currency must be a valid ISO-3 code (e.g., USD, EUR, GBP) for all Deals rows."
                        ],
                        "batch_id": batch_id,
                        "bridge_complete": 0,
                        "duplicates_skipped": 0,
                        "quarantined_count": 0,
                        "issue_report_id": issue_report_id,
                        "supplemental_counts": zero_supplemental,
                        "replaced_funds": {},
                    }
                if code:
                    group["currency_values"].append(code)

            # Collect performance currency (must be uniform per firm)
            if "performance_currency" in df.columns:
                perf_code = normalize_currency_code(row.get("performance_currency"), default=None)
                if perf_code is None and clean_str(row.get("performance_currency")) is not None:
                    return {
                        "success": 0,
                        "errors": [
                            "Performance Currency must be a valid ISO-3 code (e.g., USD, EUR, GBP) for all Deals rows."
                        ],
                        "batch_id": batch_id,
                        "bridge_complete": 0,
                        "duplicates_skipped": 0,
                        "quarantined_count": 0,
                        "issue_report_id": issue_report_id,
                        "supplemental_counts": zero_supplemental,
                        "replaced_funds": {},
                    }
                if perf_code:
                    group["perf_currency_values"].append(perf_code)

            # Collect financial metric currency (can vary per deal)
            if "financial_metric_currency" in df.columns:
                fin_code = normalize_currency_code(row.get("financial_metric_currency"), default=None)
                if fin_code is None and clean_str(row.get("financial_metric_currency")) is not None:
                    return {
                        "success": 0,
                        "errors": [
                            "Financial Metric Currency must be a valid ISO-3 code (e.g., USD, EUR, GBP) for all Deals rows."
                        ],
                        "batch_id": batch_id,
                        "bridge_complete": 0,
                        "duplicates_skipped": 0,
                        "quarantined_count": 0,
                        "issue_report_id": issue_report_id,
                        "supplemental_counts": zero_supplemental,
                        "replaced_funds": {},
                    }
                if fin_code:
                    group["fin_currency_by_row"][idx] = fin_code
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
            "supplemental_counts": zero_supplemental,
            "replaced_funds": {},
        }

    if not firm_groups:
        return {
            "success": 0,
            "errors": [],
            "batch_id": batch_id,
            "bridge_complete": 0,
            "duplicates_skipped": 0,
            "quarantined_count": 0,
            "issue_report_id": issue_report_id,
            "supplemental_counts": zero_supplemental,
            "replaced_funds": {},
        }

    firm_contexts = {}
    for lookup_key in sorted(firm_groups.keys()):
        group = firm_groups[lookup_key]
        if group["missing_as_of_rows"]:
            return {
                "success": 0,
                "errors": [
                    f"As Of Date is required for firm '{group['firm_name']}' on Deals rows: "
                    f"{', '.join(str(r) for r in group['missing_as_of_rows'][:20])}"
                ],
                "batch_id": batch_id,
                "bridge_complete": 0,
                "duplicates_skipped": 0,
                "quarantined_count": len(group["missing_as_of_rows"]),
                "issue_report_id": issue_report_id,
                "supplemental_counts": zero_supplemental,
                "replaced_funds": {},
            }

        if len(group["as_of_dates"]) != 1:
            return {
                "success": 0,
                "errors": [
                    f"Deals rows for firm '{group['firm_name']}' must contain exactly one As Of Date. "
                    f"Found {len(group['as_of_dates'])} distinct values."
                ],
                "batch_id": batch_id,
                "bridge_complete": 0,
                "duplicates_skipped": 0,
                "quarantined_count": 0,
                "issue_report_id": issue_report_id,
                "supplemental_counts": zero_supplemental,
                "replaced_funds": {},
            }

        distinct_currencies = sorted(set(group["currency_values"]))
        if len(distinct_currencies) > 1:
            return {
                "success": 0,
                "errors": [
                    f"Deals rows for firm '{group['firm_name']}' must contain exactly one Firm Currency. "
                    f"Found {len(distinct_currencies)}: {', '.join(distinct_currencies[:10])}"
                ],
                "batch_id": batch_id,
                "bridge_complete": 0,
                "duplicates_skipped": 0,
                "quarantined_count": 0,
                "issue_report_id": issue_report_id,
                "supplemental_counts": zero_supplemental,
                "replaced_funds": {},
            }
        firm_currency = distinct_currencies[0] if distinct_currencies else DEFAULT_CURRENCY_CODE
        upload_as_of_date = next(iter(group["as_of_dates"]))

        # Resolve performance currency: performance_currency -> firm_currency -> USD
        distinct_perf = sorted(set(group["perf_currency_values"]))
        if len(distinct_perf) > 1:
            return {
                "success": 0,
                "errors": [
                    f"Deals rows for firm '{group['firm_name']}' must contain exactly one Performance Currency. "
                    f"Found {len(distinct_perf)}: {', '.join(distinct_perf[:10])}"
                ],
                "batch_id": batch_id,
                "bridge_complete": 0,
                "duplicates_skipped": 0,
                "quarantined_count": 0,
                "issue_report_id": issue_report_id,
                "supplemental_counts": zero_supplemental,
                "replaced_funds": {},
            }
        perf_currency = distinct_perf[0] if distinct_perf else firm_currency

        # Financial metric currency per row — fallback to perf_currency if not specified
        fin_currency_by_row = group["fin_currency_by_row"]  # {row_idx: ISO code}

        # Resolve FX rates: performance currency (once per firm)
        perf_fx = resolve_rate_to_usd(perf_currency, date.today())
        perf_rate = perf_fx.get("rate") if perf_fx.get("ok") else None

        # Resolve FX rates: distinct financial metric currencies (cached per currency)
        distinct_fin_currencies = sorted(set(fin_currency_by_row.values()))
        fin_fx_by_currency = {}
        for fin_code in distinct_fin_currencies:
            if fin_code == perf_currency:
                fin_fx_by_currency[fin_code] = perf_fx  # reuse
            else:
                fin_fx_by_currency[fin_code] = resolve_rate_to_usd(fin_code, date.today())
        # Also resolve the default (perf_currency) for rows without explicit fin currency
        if perf_currency not in fin_fx_by_currency:
            fin_fx_by_currency[perf_currency] = perf_fx

        firm = _resolve_or_create_firm(group["firm_name"], base_currency=perf_currency)
        _ensure_team_firm_access(team_id, firm.id, created_by_user_id=uploader_user_id)
        fx_meta = _refresh_firm_fx_metadata(firm, upload_date=date.today())
        firm_contexts[lookup_key] = {
            "firm_name": group["firm_name"],
            "firm_id": firm.id,
            "firm_currency": firm.base_currency or DEFAULT_CURRENCY_CODE,
            "perf_currency": perf_currency,
            "perf_rate": perf_rate,
            "perf_fx": perf_fx,
            "fin_currency_by_row": fin_currency_by_row,
            "fin_fx_by_currency": fin_fx_by_currency,
            "as_of_date": upload_as_of_date,
            "fx_meta": fx_meta,
            "row_indices": list(group["row_indices"]),
            "replaced_funds": {},
            "success_count": 0,
            "conversion_warnings": [],
        }

    if replace_mode == "replace_fund":
        for lookup_key in sorted(firm_contexts.keys()):
            context = firm_contexts[lookup_key]
            uploaded_funds = set()
            if "fund_number" in df.columns:
                for row_idx in context["row_indices"]:
                    uploaded_funds.add(clean_str(df.at[row_idx, "fund_number"]))
            else:
                uploaded_funds.add(None)
            context["replaced_funds"] = _replace_existing_fund_data(context["firm_id"], uploaded_funds)
        db.session.flush()
        db.session.expunge_all()

    existing_keys_by_firm = {}
    for context in firm_contexts.values():
        firm_id = context["firm_id"]
        existing_keys_by_firm[firm_id] = {
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
        firm_name = clean_str(row.get("firm_name"))
        lookup_key = _firm_lookup_key(firm_name)
        context = firm_contexts.get(lookup_key)
        if context is None:
            msg = f"Row {row_num}: Quarantined — unknown firm scope '{firm_name or 'N/A'}'."
            errors.append(msg)
            quarantined_count += 1
            _record_issue(issue_report_id, batch_id, team_id, None, row_num, clean_str(row.get("company_name")), "error", msg, row_payload)
            continue
        firm_id = context["firm_id"]

        company = row.get("company_name")

        if pd.isna(company) or company is None or str(company).strip() in ("", "None"):
            msg = f"Row {row_num}: Quarantined — missing company name."
            errors.append(msg)
            quarantined_count += 1
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, None, "error", msg, row_payload)
            continue

        fund_val = clean_str(row.get("fund_number")) or ""
        deal_key = (str(company).strip().lower(), fund_val.strip().lower())
        existing_keys = existing_keys_by_firm[firm_id]
        if deal_key in existing_keys:
            msg = f"Row {row_num}: Skipped duplicate — '{company}' already exists."
            errors.append(msg)
            duplicates_skipped += 1
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, str(company).strip(), "warning", msg, row_payload)
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
                team_id=team_id,
                firm_id=firm_id,
                investment_date=investment_date,
                year_invested=year_invested_val,
                exit_date=clean_val(row.get("exit_date")),
                as_of_date=clean_val(row.get("as_of_date")),
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
                acquired_revenue=clean_val(row.get("acquired_revenue")),
                acquired_ebitda=clean_val(row.get("acquired_ebitda")),
                acquired_tev=clean_val(row.get("acquired_tev")),
                upload_batch=batch_id,
            )

            # --- Currency conversion (upload-time, to USD) ---
            row_perf_currency = context["perf_currency"]
            row_perf_rate = context["perf_rate"]
            row_fin_currency = context["fin_currency_by_row"].get(idx, row_perf_currency)
            row_fin_fx = context["fin_fx_by_currency"].get(row_fin_currency, context["perf_fx"])
            row_fin_rate = row_fin_fx.get("rate") if row_fin_fx.get("ok") else None

            # Store currency metadata on deal
            deal.performance_currency = row_perf_currency
            deal.financial_metric_currency = row_fin_currency
            deal.perf_fx_rate_to_usd = row_perf_rate
            deal.fin_fx_rate_to_usd = row_fin_rate

            # Convert performance fields (equity, realized, unrealized, fund_size)
            if row_perf_rate is not None and row_perf_rate != 1.0:
                for _field in PERF_CURRENCY_COLS:
                    _val = getattr(deal, _field, None)
                    if _val is not None:
                        setattr(deal, _field, _val * row_perf_rate)
            elif row_perf_rate is None and row_perf_currency != DEFAULT_CURRENCY_CODE:
                msg = (f"Row {row_num}: FX lookup failed for {row_perf_currency}→USD. "
                       f"Performance values stored in native {row_perf_currency}.")
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num,
                              deal.company_name, "warning", msg, row_payload)
                context["conversion_warnings"].append(msg)

            # Convert financial metric fields (revenue, ebitda, tev, net_debt, acquired_*)
            if row_fin_rate is not None and row_fin_rate != 1.0:
                for _field in FIN_METRIC_CURRENCY_COLS:
                    _val = getattr(deal, _field, None)
                    if _val is not None:
                        setattr(deal, _field, _val * row_fin_rate)
            elif row_fin_rate is None and row_fin_currency != DEFAULT_CURRENCY_CODE:
                msg = (f"Row {row_num}: FX lookup failed for {row_fin_currency}→USD. "
                       f"Financial metric values stored in native {row_fin_currency}.")
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num,
                              deal.company_name, "warning", msg, row_payload)
                context["conversion_warnings"].append(msg)

            if deal.equity_invested is not None and deal.equity_invested < 0:
                msg = f"Row {row_num}: Quarantined — negative equity invested ({deal.equity_invested})."
                errors.append(msg)
                quarantined_count += 1
                _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, deal.company_name, "error", msg, row_payload)
                continue

            if deal.investment_date and deal.exit_date and deal.exit_date < deal.investment_date:
                msg = (
                    f"Row {row_num}: Quarantined — exit date ({deal.exit_date}) is before "
                    f"investment date ({deal.investment_date})."
                )
                errors.append(msg)
                quarantined_count += 1
                _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, deal.company_name, "error", msg, row_payload)
                continue

            if ownership is not None and ownership > 1.5:
                msg = f"Row {row_num}: Ownership {ownership:.2%} is above expected range."
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)

            for warn in _warn_extreme_multiples(deal):
                msg = f"Row {row_num}: {warn}"
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)

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
                _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, deal.company_name, "warning", msg, row_payload)
            else:
                bridge_complete += 1

            db.session.add(deal)
            success += 1
            context["success_count"] += 1
        except Exception as exc:
            msg = f"Row {row_num}: Quarantined — {str(exc)}"
            errors.append(msg)
            quarantined_count += 1
            _record_issue(issue_report_id, batch_id, team_id, firm_id, row_num, str(company).strip(), "error", msg, row_payload)

    db.session.flush()
    firm_name_to_id = {lookup_key: context["firm_id"] for lookup_key, context in firm_contexts.items()}
    is_multi_firm = len(firm_contexts) > 1
    default_firm_id = None if is_multi_firm else next(iter(firm_contexts.values()))["firm_id"]
    supplemental_counts = _parse_optional_sheets(
        workbook,
        deals_sheet_name,
        team_id,
        firm_name_to_id,
        default_firm_id,
        is_multi_firm,
        issue_report_id,
        batch_id,
        errors,
    )
    db.session.commit()

    sorted_contexts = [firm_contexts[key] for key in sorted(firm_contexts.keys())]
    firms_processed = []
    for context in sorted_contexts:
        fx_meta = context["fx_meta"] or {}
        fin_currencies_used = sorted(set(context.get("fin_currency_by_row", {}).values()))
        firms_processed.append(
            {
                "firm_name": context["firm_name"],
                "firm_id": context["firm_id"],
                "currency": context["firm_currency"],
                "perf_currency": context.get("perf_currency", context["firm_currency"]),
                "perf_fx_rate": context.get("perf_rate"),
                "fin_currencies": fin_currencies_used if fin_currencies_used else [context.get("perf_currency", context["firm_currency"])],
                "as_of_date": context["as_of_date"],
                "success_count": context["success_count"],
                "replaced_funds": context["replaced_funds"],
                "fx_status": fx_meta.get("fx_status"),
                "fx_rate_to_usd": fx_meta.get("fx_rate_to_usd"),
                "fx_rate_date": fx_meta.get("fx_rate_date"),
                "fx_warning": fx_meta.get("fx_warning"),
                "conversion_warnings": context.get("conversion_warnings", []),
            }
        )

    primary_context = sorted_contexts[0]
    primary_fx = primary_context["fx_meta"] or {}
    top_level_as_of = max(
        [ctx["as_of_date"] for ctx in sorted_contexts if ctx.get("as_of_date") is not None],
        default=None,
    )
    top_level_replaced = primary_context["replaced_funds"] if not is_multi_firm else {}

    return {
        "success": success,
        "errors": errors,
        "batch_id": batch_id,
        "bridge_complete": bridge_complete,
        "duplicates_skipped": duplicates_skipped,
        "quarantined_count": quarantined_count,
        "issue_report_id": issue_report_id,
        "supplemental_counts": supplemental_counts,
        "replaced_funds": top_level_replaced,
        "firm_name": primary_context["firm_name"] if not is_multi_firm else None,
        "firm_id": primary_context["firm_id"] if not is_multi_firm else None,
        "firm_currency": primary_context["firm_currency"] if not is_multi_firm else None,
        "fx_rate_to_usd": primary_fx.get("fx_rate_to_usd") if not is_multi_firm else None,
        "fx_rate_date": primary_fx.get("fx_rate_date") if not is_multi_firm else None,
        "fx_status": primary_fx.get("fx_status") if not is_multi_firm else None,
        "fx_warning": primary_fx.get("fx_warning") if not is_multi_firm else None,
        "as_of_date": top_level_as_of,
        "firm_count": len(sorted_contexts),
        "firms_processed": firms_processed,
    }

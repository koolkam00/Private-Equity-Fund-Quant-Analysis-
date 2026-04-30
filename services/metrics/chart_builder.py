"""Chart Builder field catalog and query execution service."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
import math

from sqlalchemy import select

from models import (
    BenchmarkPoint,
    Deal,
    DealCashflowEvent,
    DealQuarterSnapshot,
    DealUnderwriteBaseline,
    FundQuarterSnapshot,
    db,
)
from peqa.services.filtering import apply_deal_filters, build_deal_scope_query
from peqa.services.metrics.status import normalize_realization_status
from services.metrics.common import safe_divide
from services.metrics.deal import compute_deal_metrics

SUPPORTED_SOURCES = ("deals", "deal_quarterly", "fund_quarterly", "cashflows", "underwrite", "benchmarks")
SUPPORTED_CHART_TYPES = ("auto", "bar", "line", "area", "scatter", "bubble", "donut")
SUPPORTED_AGGS = ("count", "count_distinct", "sum", "avg", "wavg", "min", "max")

DEFAULT_GROUP_LIMIT = 200
DEFAULT_SCATTER_LIMIT = 1500
DEFAULT_TABLE_LIMIT = 5000

GLOBAL_FILTER_KEYS = (
    "fund",
    "status",
    "sector",
    "geography",
    "vintage",
    "exit_type",
    "lead_partner",
    "security_type",
    "deal_type",
    "entry_channel",
    "benchmark_asset_class",
)

DEFAULT_WEIGHT_FIELD_BY_SOURCE = {
    "deals": "equity_invested",
    "deal_quarterly": "equity_invested",
    "underwrite": "equity_invested",
    "fund_quarterly": "paid_in_capital",
}

NUMERIC_TYPES = {"number", "currency", "percent", "multiple", "year"}


class ChartBuilderError(ValueError):
    """Deterministic validation/runtime error for chart-builder requests."""


FIELD_CATALOG = {
    "deals": {
        "label": "Deals",
        "default_weight_field": "equity_invested",
        "dimensions": [
            {"field": "deal_id", "label": "Deal ID", "type": "number"},
            {"field": "company_name", "label": "Company", "type": "string"},
            {"field": "fund_number", "label": "Fund", "type": "string"},
            {"field": "status", "label": "Status", "type": "enum"},
            {"field": "sector", "label": "Sector", "type": "string"},
            {"field": "geography", "label": "Geography", "type": "string"},
            {"field": "investment_date", "label": "Investment Date", "type": "date"},
            {"field": "exit_date", "label": "Exit Date", "type": "date"},
            {"field": "vintage_year", "label": "Vintage Year", "type": "year"},
            {"field": "exit_type", "label": "Exit Type", "type": "string"},
            {"field": "lead_partner", "label": "Lead Partner", "type": "string"},
            {"field": "security_type", "label": "Security Type", "type": "string"},
            {"field": "deal_type", "label": "Deal Type", "type": "string"},
            {"field": "entry_channel", "label": "Entry Channel", "type": "string"},
        ],
        "measures": [
            {"field": "equity_invested", "label": "Equity Invested", "type": "currency"},
            {"field": "realized_value", "label": "Realized Value", "type": "currency"},
            {"field": "unrealized_value", "label": "Unrealized Value", "type": "currency"},
            {"field": "total_value", "label": "Total Value", "type": "currency"},
            {"field": "value_created", "label": "Value Created", "type": "currency"},
            {"field": "gross_moic", "label": "Gross MOIC", "type": "multiple"},
            {"field": "gross_irr", "label": "Gross IRR", "type": "percent"},
            {"field": "implied_irr", "label": "Implied IRR", "type": "percent"},
            {"field": "hold_period", "label": "Hold Period (Years)", "type": "number"},
            {"field": "entry_revenue", "label": "Entry Revenue", "type": "currency"},
            {"field": "exit_revenue", "label": "Exit Revenue", "type": "currency"},
            {"field": "entry_ebitda", "label": "Entry EBITDA", "type": "currency"},
            {"field": "exit_ebitda", "label": "Exit EBITDA", "type": "currency"},
            {"field": "entry_enterprise_value", "label": "Entry TEV", "type": "currency"},
            {"field": "exit_enterprise_value", "label": "Exit TEV", "type": "currency"},
            {"field": "entry_net_debt", "label": "Entry Net Debt", "type": "currency"},
            {"field": "exit_net_debt", "label": "Exit Net Debt", "type": "currency"},
            {"field": "entry_tev_ebitda", "label": "Entry TEV / EBITDA", "type": "multiple"},
            {"field": "exit_tev_ebitda", "label": "Exit TEV / EBITDA", "type": "multiple"},
            {"field": "entry_tev_revenue", "label": "Entry TEV / Revenue", "type": "multiple"},
            {"field": "exit_tev_revenue", "label": "Exit TEV / Revenue", "type": "multiple"},
            {"field": "entry_net_debt_ebitda", "label": "Entry Net Debt / EBITDA", "type": "multiple"},
            {"field": "exit_net_debt_ebitda", "label": "Exit Net Debt / EBITDA", "type": "multiple"},
            {"field": "entry_net_debt_tev", "label": "Entry Net Debt / TEV", "type": "percent"},
            {"field": "exit_net_debt_tev", "label": "Exit Net Debt / TEV", "type": "percent"},
            {"field": "entry_ebitda_margin", "label": "Entry EBITDA Margin", "type": "percent"},
            {"field": "exit_ebitda_margin", "label": "Exit EBITDA Margin", "type": "percent"},
            {"field": "revenue_growth", "label": "Revenue Growth", "type": "percent"},
            {"field": "ebitda_growth", "label": "EBITDA Growth", "type": "percent"},
            {"field": "revenue_cagr", "label": "Revenue CAGR", "type": "percent"},
            {"field": "ebitda_cagr", "label": "EBITDA CAGR", "type": "percent"},
            {"field": "bridge_revenue", "label": "Bridge Revenue Growth", "type": "currency"},
            {"field": "bridge_ebitda_growth", "label": "Bridge EBITDA Growth", "type": "currency"},
            {"field": "bridge_margin", "label": "Bridge Margin Expansion", "type": "currency"},
            {"field": "bridge_multiple", "label": "Bridge Multiple Expansion", "type": "currency"},
            {"field": "bridge_leverage", "label": "Bridge Leverage", "type": "currency"},
            {"field": "bridge_other", "label": "Bridge Other", "type": "currency"},
        ],
    },
    "deal_quarterly": {
        "label": "Deal Quarterly",
        "default_weight_field": "equity_invested",
        "dimensions": [
            {"field": "deal_id", "label": "Deal ID", "type": "number"},
            {"field": "company_name", "label": "Company", "type": "string"},
            {"field": "fund_number", "label": "Fund", "type": "string"},
            {"field": "status", "label": "Status", "type": "enum"},
            {"field": "sector", "label": "Sector", "type": "string"},
            {"field": "geography", "label": "Geography", "type": "string"},
            {"field": "quarter_end", "label": "Quarter End", "type": "date"},
            {"field": "quarter_year", "label": "Quarter Year", "type": "year"},
            {"field": "valuation_basis", "label": "Valuation Basis", "type": "string"},
            {"field": "source", "label": "Source", "type": "string"},
        ],
        "measures": [
            {"field": "equity_invested", "label": "Equity Invested", "type": "currency"},
            {"field": "revenue", "label": "Revenue", "type": "currency"},
            {"field": "ebitda", "label": "EBITDA", "type": "currency"},
            {"field": "enterprise_value", "label": "Enterprise Value", "type": "currency"},
            {"field": "net_debt", "label": "Net Debt", "type": "currency"},
            {"field": "equity_value", "label": "Equity Value", "type": "currency"},
            {"field": "ev_ebitda", "label": "EV / EBITDA", "type": "multiple"},
            {"field": "ev_revenue", "label": "EV / Revenue", "type": "multiple"},
            {"field": "nd_ebitda", "label": "Net Debt / EBITDA", "type": "multiple"},
            {"field": "nd_tev", "label": "Net Debt / TEV", "type": "percent"},
            {"field": "ebitda_margin", "label": "EBITDA Margin", "type": "percent"},
        ],
    },
    "fund_quarterly": {
        "label": "Fund Quarterly",
        "default_weight_field": "paid_in_capital",
        "dimensions": [
            {"field": "fund_number", "label": "Fund", "type": "string"},
            {"field": "quarter_end", "label": "Quarter End", "type": "date"},
            {"field": "quarter_year", "label": "Quarter Year", "type": "year"},
        ],
        "measures": [
            {"field": "committed_capital", "label": "Committed Capital", "type": "currency"},
            {"field": "paid_in_capital", "label": "Paid-In Capital", "type": "currency"},
            {"field": "distributed_capital", "label": "Distributed Capital", "type": "currency"},
            {"field": "nav", "label": "NAV", "type": "currency"},
            {"field": "unfunded_commitment", "label": "Unfunded Commitment", "type": "currency"},
            {"field": "tvpi", "label": "TVPI", "type": "multiple"},
            {"field": "dpi", "label": "DPI", "type": "multiple"},
            {"field": "rvpi", "label": "RVPI", "type": "multiple"},
            {"field": "pic", "label": "PIC", "type": "percent"},
        ],
    },
    "cashflows": {
        "label": "Cashflows",
        "default_weight_field": None,
        "dimensions": [
            {"field": "deal_id", "label": "Deal ID", "type": "number"},
            {"field": "company_name", "label": "Company", "type": "string"},
            {"field": "fund_number", "label": "Fund", "type": "string"},
            {"field": "status", "label": "Status", "type": "enum"},
            {"field": "sector", "label": "Sector", "type": "string"},
            {"field": "geography", "label": "Geography", "type": "string"},
            {"field": "event_date", "label": "Event Date", "type": "date"},
            {"field": "event_year", "label": "Event Year", "type": "year"},
            {"field": "event_type", "label": "Event Type", "type": "string"},
        ],
        "measures": [
            {"field": "amount", "label": "Amount", "type": "currency"},
            {"field": "abs_amount", "label": "Absolute Amount", "type": "currency"},
        ],
    },
    "underwrite": {
        "label": "Underwrite",
        "default_weight_field": "equity_invested",
        "dimensions": [
            {"field": "deal_id", "label": "Deal ID", "type": "number"},
            {"field": "company_name", "label": "Company", "type": "string"},
            {"field": "fund_number", "label": "Fund", "type": "string"},
            {"field": "status", "label": "Status", "type": "enum"},
            {"field": "sector", "label": "Sector", "type": "string"},
            {"field": "geography", "label": "Geography", "type": "string"},
            {"field": "lead_partner", "label": "Lead Partner", "type": "string"},
            {"field": "entry_channel", "label": "Entry Channel", "type": "string"},
            {"field": "baseline_date", "label": "Baseline Date", "type": "date"},
            {"field": "baseline_year", "label": "Baseline Year", "type": "year"},
        ],
        "measures": [
            {"field": "equity_invested", "label": "Equity Invested", "type": "currency"},
            {"field": "actual_moic", "label": "Actual MOIC", "type": "multiple"},
            {"field": "target_moic", "label": "Target MOIC", "type": "multiple"},
            {"field": "delta_moic", "label": "MOIC Delta", "type": "multiple"},
            {"field": "actual_irr", "label": "Actual IRR", "type": "percent"},
            {"field": "target_irr", "label": "Target IRR", "type": "percent"},
            {"field": "delta_irr", "label": "IRR Delta", "type": "percent"},
            {"field": "actual_hold_years", "label": "Actual Hold", "type": "number"},
            {"field": "target_hold_years", "label": "Target Hold", "type": "number"},
            {"field": "delta_hold_years", "label": "Hold Delta", "type": "number"},
            {"field": "actual_exit_multiple", "label": "Actual Exit Multiple", "type": "multiple"},
            {"field": "target_exit_multiple", "label": "Target Exit Multiple", "type": "multiple"},
            {"field": "delta_exit_multiple", "label": "Exit Multiple Delta", "type": "multiple"},
            {"field": "actual_revenue_cagr", "label": "Actual Revenue CAGR", "type": "percent"},
            {"field": "target_revenue_cagr", "label": "Target Revenue CAGR", "type": "percent"},
            {"field": "delta_revenue_cagr", "label": "Revenue CAGR Delta", "type": "percent"},
            {"field": "actual_ebitda_cagr", "label": "Actual EBITDA CAGR", "type": "percent"},
            {"field": "target_ebitda_cagr", "label": "Target EBITDA CAGR", "type": "percent"},
            {"field": "delta_ebitda_cagr", "label": "EBITDA CAGR Delta", "type": "percent"},
        ],
    },
    "benchmarks": {
        "label": "Benchmarks",
        "default_weight_field": None,
        "dimensions": [
            {"field": "asset_class", "label": "Asset Class", "type": "string"},
            {"field": "vintage_year", "label": "Vintage Year", "type": "year"},
            {"field": "metric", "label": "Metric", "type": "enum"},
            {"field": "quartile", "label": "Quartile", "type": "enum"},
            {"field": "upload_batch", "label": "Upload Batch", "type": "string"},
        ],
        "measures": [
            {"field": "value", "label": "Benchmark Value", "type": "number"},
        ],
    },
}


def _to_float(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _normalize_status(status):
    return normalize_realization_status(status)


def _deal_vintage_year(deal):
    if deal.year_invested is not None:
        return int(deal.year_invested)
    if deal.investment_date is not None:
        return int(deal.investment_date.year)
    return None


def _normalize_global_filters(global_filters):
    values = {}
    for key in GLOBAL_FILTER_KEYS:
        val = global_filters.get(key) if isinstance(global_filters, dict) else None
        values[key] = (str(val).strip() if val is not None else "")
    return values


def _deal_passes_global_filters(deal, global_filters):
    if global_filters.get("fund") and (deal.fund_number or "") != global_filters["fund"]:
        return False
    if global_filters.get("status") and (deal.status or "") != global_filters["status"]:
        return False
    if global_filters.get("sector") and (deal.sector or "") != global_filters["sector"]:
        return False
    if global_filters.get("geography") and (deal.geography or "") != global_filters["geography"]:
        return False
    if global_filters.get("exit_type") and (deal.exit_type or "Not Specified") != global_filters["exit_type"]:
        return False
    if global_filters.get("lead_partner") and (deal.lead_partner or "Unassigned") != global_filters["lead_partner"]:
        return False
    if global_filters.get("security_type") and (deal.security_type or "Common Equity") != global_filters["security_type"]:
        return False
    if global_filters.get("deal_type") and (deal.deal_type or "Platform") != global_filters["deal_type"]:
        return False
    if global_filters.get("entry_channel") and (deal.entry_channel or "Unknown") != global_filters["entry_channel"]:
        return False
    if global_filters.get("vintage"):
        try:
            v = int(global_filters["vintage"])
        except (TypeError, ValueError):
            return False
        if _deal_vintage_year(deal) != v:
            return False
    return True


def _team_firm_deals(team_id, firm_id, global_filters):
    query = build_deal_scope_query(team_id=team_id, firm_id=firm_id)
    return apply_deal_filters(query, global_filters).all()


def _filtered_deal_ids_query(team_id, firm_id, global_filters):
    return apply_deal_filters(
        build_deal_scope_query(team_id=team_id, firm_id=firm_id),
        global_filters,
    ).with_entities(Deal.id)


def _row_count_for_source(source, team_id, firm_id, global_filters):
    if source == "deals":
        return apply_deal_filters(
            build_deal_scope_query(team_id=team_id, firm_id=firm_id),
            global_filters,
        ).count()

    if source == "deal_quarterly":
        if firm_id is None:
            return 0
        subquery = _filtered_deal_ids_query(team_id, firm_id, global_filters).subquery()
        return (
            db.session.query(db.func.count(DealQuarterSnapshot.id))
            .filter(DealQuarterSnapshot.firm_id == firm_id, DealQuarterSnapshot.deal_id.in_(select(subquery.c.id)))
            .scalar()
            or 0
        )

    if source == "fund_quarterly":
        if firm_id is None:
            return 0
        query = FundQuarterSnapshot.query.filter(FundQuarterSnapshot.firm_id == firm_id)
        if team_id is None:
            query = query.filter(FundQuarterSnapshot.team_id.is_(None))
        else:
            query = query.filter(
                (FundQuarterSnapshot.team_id == team_id) | (FundQuarterSnapshot.team_id.is_(None))
            )
        if global_filters.get("fund"):
            query = query.filter(FundQuarterSnapshot.fund_number == global_filters["fund"])
        return query.count()

    if source == "cashflows":
        if firm_id is None:
            return 0
        subquery = _filtered_deal_ids_query(team_id, firm_id, global_filters).subquery()
        return (
            db.session.query(db.func.count(DealCashflowEvent.id))
            .filter(DealCashflowEvent.firm_id == firm_id, DealCashflowEvent.deal_id.in_(select(subquery.c.id)))
            .scalar()
            or 0
        )

    if source == "underwrite":
        if firm_id is None:
            return 0
        subquery = _filtered_deal_ids_query(team_id, firm_id, global_filters).subquery()
        return (
            db.session.query(db.func.count(db.func.distinct(DealUnderwriteBaseline.deal_id)))
            .filter(
                DealUnderwriteBaseline.firm_id == firm_id,
                DealUnderwriteBaseline.deal_id.in_(select(subquery.c.id)),
            )
            .scalar()
            or 0
        )

    if source == "benchmarks":
        if team_id is None:
            return 0
        query = BenchmarkPoint.query.filter(BenchmarkPoint.team_id == team_id)
        asset_class = (global_filters.get("benchmark_asset_class") or "").strip()
        if asset_class:
            query = query.filter(BenchmarkPoint.asset_class == asset_class)
        return query.count()

    raise ChartBuilderError(f"Unsupported source '{source}'.")


def _bridge_driver_from_metrics(metrics, key):
    bridge = metrics.get("bridge_additive_fund") or {}
    display_rows = bridge.get("display_drivers") or []
    if key == "revenue":
        return _to_float((bridge.get("drivers_dollar") or {}).get("revenue"))
    if key == "margin":
        return _to_float((bridge.get("drivers_dollar") or {}).get("margin"))
    if key == "multiple":
        return _to_float((bridge.get("drivers_dollar") or {}).get("multiple"))
    if key == "leverage":
        return _to_float((bridge.get("drivers_dollar") or {}).get("leverage"))
    if key == "other":
        return _to_float((bridge.get("drivers_dollar") or {}).get("other"))
    if key == "ebitda_growth":
        for row in display_rows:
            if row.get("key") == "ebitda_growth":
                return _to_float(row.get("dollar"))
        return 0.0
    return None


def _load_rows_deals(team_id, firm_id, global_filters):
    deals = _team_firm_deals(team_id, firm_id, global_filters)
    metrics_by_id = {deal.id: compute_deal_metrics(deal) for deal in deals}
    rows = []
    for deal in deals:
        m = metrics_by_id[deal.id]
        rows.append(
            {
                "deal_id": deal.id,
                "company_name": deal.company_name,
                "fund_number": deal.fund_number or "Unknown Fund",
                "status": _normalize_status(deal.status),
                "sector": deal.sector or "Unknown",
                "geography": deal.geography or "Unknown",
                "investment_date": deal.investment_date,
                "exit_date": deal.exit_date,
                "vintage_year": _deal_vintage_year(deal),
                "exit_type": deal.exit_type or "Not Specified",
                "lead_partner": deal.lead_partner or "Unassigned",
                "security_type": deal.security_type or "Common Equity",
                "deal_type": deal.deal_type or "Platform",
                "entry_channel": deal.entry_channel or "Unknown",
                "equity_invested": _to_float(m.get("equity")),
                "realized_value": _to_float(m.get("realized")),
                "unrealized_value": _to_float(m.get("unrealized")),
                "total_value": _to_float(m.get("value_total")),
                "value_created": _to_float(m.get("value_created")),
                "gross_moic": _to_float(m.get("moic")),
                "gross_irr": _to_float(m.get("gross_irr")),
                "implied_irr": _to_float(m.get("implied_irr")),
                "hold_period": _to_float(m.get("hold_period")),
                "entry_revenue": _to_float(m.get("entry_revenue")),
                "exit_revenue": _to_float(m.get("exit_revenue")),
                "entry_ebitda": _to_float(m.get("entry_ebitda")),
                "exit_ebitda": _to_float(m.get("exit_ebitda")),
                "entry_enterprise_value": _to_float(m.get("entry_enterprise_value")),
                "exit_enterprise_value": _to_float(m.get("exit_enterprise_value")),
                "entry_net_debt": _to_float(m.get("entry_net_debt")),
                "exit_net_debt": _to_float(m.get("exit_net_debt")),
                "entry_tev_ebitda": _to_float(m.get("entry_tev_ebitda")),
                "exit_tev_ebitda": _to_float(m.get("exit_tev_ebitda")),
                "entry_tev_revenue": _to_float(m.get("entry_tev_revenue")),
                "exit_tev_revenue": _to_float(m.get("exit_tev_revenue")),
                "entry_net_debt_ebitda": _to_float(m.get("entry_net_debt_ebitda")),
                "exit_net_debt_ebitda": _to_float(m.get("exit_net_debt_ebitda")),
                "entry_net_debt_tev": _to_float(m.get("entry_net_debt_tev")),
                "exit_net_debt_tev": _to_float(m.get("exit_net_debt_tev")),
                "entry_ebitda_margin": _to_float(m.get("entry_ebitda_margin")),
                "exit_ebitda_margin": _to_float(m.get("exit_ebitda_margin")),
                "revenue_growth": _to_float(m.get("revenue_growth")),
                "ebitda_growth": _to_float(m.get("ebitda_growth")),
                "revenue_cagr": _to_float(m.get("revenue_cagr")),
                "ebitda_cagr": _to_float(m.get("ebitda_cagr")),
                "bridge_revenue": _bridge_driver_from_metrics(m, "revenue"),
                "bridge_ebitda_growth": _bridge_driver_from_metrics(m, "ebitda_growth"),
                "bridge_margin": _bridge_driver_from_metrics(m, "margin"),
                "bridge_multiple": _bridge_driver_from_metrics(m, "multiple"),
                "bridge_leverage": _bridge_driver_from_metrics(m, "leverage"),
                "bridge_other": _bridge_driver_from_metrics(m, "other"),
            }
        )
    return rows


def _load_rows_deal_quarterly(team_id, firm_id, global_filters):
    deals = _team_firm_deals(team_id, firm_id, global_filters)
    deal_map = {d.id: d for d in deals}
    if not deal_map:
        return []
    rows = (
        DealQuarterSnapshot.query.filter(
            DealQuarterSnapshot.firm_id == firm_id,
            DealQuarterSnapshot.deal_id.in_(deal_map.keys()),
        )
        .order_by(DealQuarterSnapshot.quarter_end.asc(), DealQuarterSnapshot.deal_id.asc())
        .all()
    )
    out = []
    for row in rows:
        deal = deal_map.get(row.deal_id)
        if deal is None:
            continue
        ebitda = _to_float(row.ebitda)
        revenue = _to_float(row.revenue)
        ev = _to_float(row.enterprise_value)
        net_debt = _to_float(row.net_debt)
        equity_value = _to_float(row.equity_value)
        if equity_value is None and ev is not None and net_debt is not None:
            equity_value = ev - net_debt
        ev_ebitda = safe_divide(ev, ebitda)
        if ev_ebitda is not None and ev_ebitda < 0:
            ev_ebitda = None
        out.append(
            {
                "deal_id": row.deal_id,
                "company_name": deal.company_name,
                "fund_number": deal.fund_number or "Unknown Fund",
                "status": _normalize_status(deal.status),
                "sector": deal.sector or "Unknown",
                "geography": deal.geography or "Unknown",
                "quarter_end": row.quarter_end,
                "quarter_year": row.quarter_end.year if row.quarter_end else None,
                "valuation_basis": row.valuation_basis or "Unknown",
                "source": row.source or "Unknown",
                "equity_invested": _to_float(deal.equity_invested),
                "revenue": revenue,
                "ebitda": ebitda,
                "enterprise_value": ev,
                "net_debt": net_debt,
                "equity_value": equity_value,
                "ev_ebitda": _to_float(ev_ebitda),
                "ev_revenue": _to_float(safe_divide(ev, revenue)),
                "nd_ebitda": _to_float(safe_divide(net_debt, ebitda)),
                "nd_tev": _to_float(safe_divide(net_debt, ev)),
                "ebitda_margin": _to_float(safe_divide(ebitda, revenue) * 100 if safe_divide(ebitda, revenue) is not None else None),
            }
        )
    return out


def _load_rows_fund_quarterly(team_id, firm_id, global_filters):
    if firm_id is None:
        return []
    query = FundQuarterSnapshot.query.filter(FundQuarterSnapshot.firm_id == firm_id)
    if team_id is None:
        query = query.filter(FundQuarterSnapshot.team_id.is_(None))
    else:
        query = query.filter(
            (FundQuarterSnapshot.team_id == team_id) | (FundQuarterSnapshot.team_id.is_(None))
        )
    if global_filters.get("fund"):
        query = query.filter(FundQuarterSnapshot.fund_number == global_filters["fund"])
    rows = query.order_by(FundQuarterSnapshot.quarter_end.asc(), FundQuarterSnapshot.fund_number.asc()).all()
    out = []
    for row in rows:
        paid_in = _to_float(row.paid_in_capital)
        distributed = _to_float(row.distributed_capital)
        nav = _to_float(row.nav)
        committed = _to_float(row.committed_capital)
        out.append(
            {
                "fund_number": row.fund_number or "Unknown Fund",
                "quarter_end": row.quarter_end,
                "quarter_year": row.quarter_end.year if row.quarter_end else None,
                "committed_capital": committed,
                "paid_in_capital": paid_in,
                "distributed_capital": distributed,
                "nav": nav,
                "unfunded_commitment": _to_float(row.unfunded_commitment),
                "tvpi": _to_float(safe_divide((distributed or 0.0) + (nav or 0.0), paid_in)),
                "dpi": _to_float(safe_divide(distributed, paid_in)),
                "rvpi": _to_float(safe_divide(nav, paid_in)),
                "pic": _to_float(safe_divide(paid_in, committed)),
            }
        )
    return out


def _load_rows_cashflows(team_id, firm_id, global_filters):
    deals = _team_firm_deals(team_id, firm_id, global_filters)
    deal_map = {d.id: d for d in deals}
    if not deal_map:
        return []
    rows = (
        DealCashflowEvent.query.filter(
            DealCashflowEvent.firm_id == firm_id,
            DealCashflowEvent.deal_id.in_(deal_map.keys()),
        )
        .order_by(DealCashflowEvent.event_date.asc(), DealCashflowEvent.deal_id.asc())
        .all()
    )
    out = []
    for row in rows:
        deal = deal_map.get(row.deal_id)
        if deal is None:
            continue
        amount = _to_float(row.amount)
        out.append(
            {
                "deal_id": row.deal_id,
                "company_name": deal.company_name,
                "fund_number": deal.fund_number or "Unknown Fund",
                "status": _normalize_status(deal.status),
                "sector": deal.sector or "Unknown",
                "geography": deal.geography or "Unknown",
                "event_date": row.event_date,
                "event_year": row.event_date.year if row.event_date else None,
                "event_type": row.event_type or "Unknown",
                "amount": amount,
                "abs_amount": abs(amount) if amount is not None else None,
            }
        )
    return out


def _as_rate(value):
    out = _to_float(value)
    if out is None:
        return None
    if abs(out) > 1.5:
        return out / 100.0
    return out


def _load_rows_underwrite(team_id, firm_id, global_filters):
    deals = _team_firm_deals(team_id, firm_id, global_filters)
    deal_map = {d.id: d for d in deals}
    if not deal_map:
        return []
    baselines = (
        DealUnderwriteBaseline.query.filter(
            DealUnderwriteBaseline.firm_id == firm_id,
            DealUnderwriteBaseline.deal_id.in_(deal_map.keys()),
        )
        .order_by(DealUnderwriteBaseline.deal_id.asc(), DealUnderwriteBaseline.baseline_date.asc(), DealUnderwriteBaseline.id.asc())
        .all()
    )
    latest = {}
    for row in baselines:
        latest[row.deal_id] = row

    metrics_by_id = {deal.id: compute_deal_metrics(deal) for deal in deals}
    out = []
    for deal_id, baseline in latest.items():
        deal = deal_map.get(deal_id)
        if deal is None:
            continue
        m = metrics_by_id[deal_id]
        target_irr = _as_rate(baseline.target_irr)
        target_rev_cagr = _as_rate(baseline.target_revenue_cagr)
        target_ebitda_cagr = _as_rate(baseline.target_ebitda_cagr)
        actual_irr = _to_float(deal.irr)
        actual_moic = _to_float(m.get("moic"))
        target_moic = _to_float(baseline.target_moic)
        actual_hold = _to_float(m.get("hold_period"))
        target_hold = _to_float(baseline.target_hold_years)
        actual_exit_multiple = _to_float(m.get("exit_tev_ebitda"))
        target_exit_multiple = _to_float(baseline.target_exit_multiple)
        actual_rev_cagr = _to_float(m.get("revenue_cagr"))
        actual_ebitda_cagr = _to_float(m.get("ebitda_cagr"))

        out.append(
            {
                "deal_id": deal.id,
                "company_name": deal.company_name,
                "fund_number": deal.fund_number or "Unknown Fund",
                "status": _normalize_status(deal.status),
                "sector": deal.sector or "Unknown",
                "geography": deal.geography or "Unknown",
                "lead_partner": deal.lead_partner or "Unassigned",
                "entry_channel": deal.entry_channel or "Unknown",
                "baseline_date": baseline.baseline_date,
                "baseline_year": baseline.baseline_date.year if baseline.baseline_date else None,
                "equity_invested": _to_float(deal.equity_invested),
                "actual_moic": actual_moic,
                "target_moic": target_moic,
                "delta_moic": _to_float(actual_moic - target_moic) if actual_moic is not None and target_moic is not None else None,
                "actual_irr": actual_irr,
                "target_irr": target_irr,
                "delta_irr": _to_float(actual_irr - target_irr) if actual_irr is not None and target_irr is not None else None,
                "actual_hold_years": actual_hold,
                "target_hold_years": target_hold,
                "delta_hold_years": _to_float(actual_hold - target_hold) if actual_hold is not None and target_hold is not None else None,
                "actual_exit_multiple": actual_exit_multiple,
                "target_exit_multiple": target_exit_multiple,
                "delta_exit_multiple": _to_float(actual_exit_multiple - target_exit_multiple)
                if actual_exit_multiple is not None and target_exit_multiple is not None
                else None,
                "actual_revenue_cagr": actual_rev_cagr,
                "target_revenue_cagr": _to_float(target_rev_cagr * 100.0) if target_rev_cagr is not None else None,
                "delta_revenue_cagr": _to_float(actual_rev_cagr - (target_rev_cagr * 100.0))
                if actual_rev_cagr is not None and target_rev_cagr is not None
                else None,
                "actual_ebitda_cagr": actual_ebitda_cagr,
                "target_ebitda_cagr": _to_float(target_ebitda_cagr * 100.0) if target_ebitda_cagr is not None else None,
                "delta_ebitda_cagr": _to_float(actual_ebitda_cagr - (target_ebitda_cagr * 100.0))
                if actual_ebitda_cagr is not None and target_ebitda_cagr is not None
                else None,
            }
        )
    return out


def _load_rows_benchmarks(team_id, _firm_id, global_filters):
    if team_id is None:
        return []
    query = BenchmarkPoint.query.filter(BenchmarkPoint.team_id == team_id)
    asset_class = (global_filters.get("benchmark_asset_class") or "").strip()
    if asset_class:
        query = query.filter(BenchmarkPoint.asset_class == asset_class)
    rows = query.order_by(BenchmarkPoint.asset_class.asc(), BenchmarkPoint.vintage_year.asc()).all()
    return [
        {
            "asset_class": row.asset_class,
            "vintage_year": int(row.vintage_year) if row.vintage_year is not None else None,
            "metric": row.metric,
            "quartile": row.quartile,
            "upload_batch": row.upload_batch or "",
            "value": _to_float(row.value),
        }
        for row in rows
    ]


def _rows_for_source(source, team_id, firm_id, global_filters):
    if source == "deals":
        return _load_rows_deals(team_id, firm_id, global_filters)
    if source == "deal_quarterly":
        return _load_rows_deal_quarterly(team_id, firm_id, global_filters)
    if source == "fund_quarterly":
        return _load_rows_fund_quarterly(team_id, firm_id, global_filters)
    if source == "cashflows":
        return _load_rows_cashflows(team_id, firm_id, global_filters)
    if source == "underwrite":
        return _load_rows_underwrite(team_id, firm_id, global_filters)
    if source == "benchmarks":
        return _load_rows_benchmarks(team_id, firm_id, global_filters)
    raise ChartBuilderError(f"Unsupported source '{source}'.")


def _catalog_lookup(source):
    if source not in FIELD_CATALOG:
        raise ChartBuilderError(f"Unsupported source '{source}'.")
    dims = [dict(item, kind="dimension") for item in FIELD_CATALOG[source]["dimensions"]]
    measures = [dict(item, kind="measure") for item in FIELD_CATALOG[source]["measures"]]
    by_field = {item["field"]: item for item in dims + measures}
    return dims, measures, by_field


def _field_numeric(field_meta):
    return field_meta is not None and field_meta.get("type") in NUMERIC_TYPES


def _bucket_date_value(value, bucket):
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value.date()
    if not isinstance(value, date):
        return value
    if bucket == "year":
        return value.year
    if bucket == "month":
        return f"{value.year:04d}-{value.month:02d}"
    if bucket == "quarter":
        q = ((value.month - 1) // 3) + 1
        return f"{value.year:04d}-Q{q}"
    return value.isoformat()


def _bucket_value(raw_value, field_meta, bucket):
    if field_meta is None:
        return raw_value
    ftype = field_meta.get("type")
    if ftype == "date":
        return _bucket_date_value(raw_value, bucket)
    if ftype == "year":
        if raw_value is None:
            return None
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return None
    return raw_value


def _coerce_filter_value(field_meta, value):
    if field_meta is None:
        return value
    ftype = field_meta.get("type")
    if ftype in NUMERIC_TYPES:
        return _to_float(value)
    if ftype == "date":
        if isinstance(value, date):
            return value.isoformat()
        return str(value).strip() if value is not None else ""
    return str(value).strip() if value is not None else ""


def _row_filter_match(row, field_meta, operator, filter_value):
    field = field_meta.get("field")
    row_value = row.get(field)
    op = (operator or "eq").strip().lower()

    if op in {"is_null", "isnull"}:
        return row_value is None
    if op in {"not_null", "notnull"}:
        return row_value is not None

    lhs = _coerce_filter_value(field_meta, row_value)
    rhs = _coerce_filter_value(field_meta, filter_value)

    if op == "eq":
        return lhs == rhs
    if op == "neq":
        return lhs != rhs
    if op == "contains":
        return str(rhs).lower() in str(lhs).lower() if lhs is not None and rhs is not None else False
    if op == "in":
        if isinstance(filter_value, list):
            norm = {_coerce_filter_value(field_meta, v) for v in filter_value}
            return lhs in norm
        return lhs == rhs
    if op in {"gt", "gte", "lt", "lte"}:
        lhs_num = _to_float(lhs)
        rhs_num = _to_float(rhs)
        if lhs_num is None or rhs_num is None:
            return False
        if op == "gt":
            return lhs_num > rhs_num
        if op == "gte":
            return lhs_num >= rhs_num
        if op == "lt":
            return lhs_num < rhs_num
        return lhs_num <= rhs_num
    raise ChartBuilderError(f"Unsupported filter operator '{operator}'.")


def _apply_local_filters(rows, filters, field_map):
    if not filters:
        return rows
    out = []
    for row in rows:
        keep = True
        for rule in filters:
            field = (rule.get("field") or "").strip()
            if not field:
                continue
            meta = field_map.get(field)
            if meta is None:
                raise ChartBuilderError(f"Unknown filter field '{field}'.")
            if not _row_filter_match(row, meta, rule.get("op", "eq"), rule.get("value")):
                keep = False
                break
        if keep:
            out.append(row)
    return out


def _aggregate(rows, field, agg, source, weight_field=None):
    agg_name = (agg or "").strip().lower()
    if agg_name not in SUPPORTED_AGGS:
        raise ChartBuilderError(f"Unsupported aggregation '{agg}'.")

    if agg_name == "count":
        if field:
            return len([r for r in rows if r.get(field) is not None])
        return len(rows)

    if agg_name == "count_distinct":
        return len({r.get(field) for r in rows if r.get(field) is not None})

    values = [_to_float(r.get(field)) for r in rows]
    values = [v for v in values if v is not None]
    if not values:
        return None

    if agg_name == "sum":
        return sum(values)
    if agg_name == "avg":
        return sum(values) / len(values)
    if agg_name == "min":
        return min(values)
    if agg_name == "max":
        return max(values)
    if agg_name == "wavg":
        selected_weight_field = (weight_field or "").strip() or DEFAULT_WEIGHT_FIELD_BY_SOURCE.get(source)
        if not selected_weight_field:
            raise ChartBuilderError(f"Weighted average is not supported for source '{source}'.")
        numerator = 0.0
        denominator = 0.0
        for row in rows:
            value = _to_float(row.get(field))
            weight = _to_float(row.get(selected_weight_field))
            if value is None or weight is None or weight <= 0:
                continue
            numerator += value * weight
            denominator += weight
        if denominator <= 0:
            return None
        return numerator / denominator
    raise ChartBuilderError(f"Unsupported aggregation '{agg}'.")


def _resolve_limit(spec_limit, default_limit):
    if spec_limit is None:
        return default_limit
    try:
        value = int(spec_limit)
    except (TypeError, ValueError):
        raise ChartBuilderError("limit must be an integer.")
    return max(1, value)


def _sort_groups(group_rows, sort_spec, default_key):
    if not group_rows:
        return group_rows
    if not isinstance(sort_spec, dict):
        sort_spec = {}
    by = (sort_spec.get("by") or default_key or "").strip()
    direction = (sort_spec.get("direction") or "asc").strip().lower()
    reverse = direction == "desc"

    if not by:
        return group_rows

    def sort_key(row):
        value = row.get(by)
        if value is None:
            return (1, "")
        if isinstance(value, (int, float)):
            return (0, value)
        return (0, str(value))

    return sorted(group_rows, key=sort_key, reverse=reverse)


def resolve_auto_chart_type(spec, sample_meta):
    requested = (spec.get("chart_type") or "auto").strip().lower()
    if requested not in SUPPORTED_CHART_TYPES:
        raise ChartBuilderError(f"Unsupported chart_type '{requested}'.")
    if requested != "auto":
        return requested

    x_type = sample_meta.get("x_type")
    has_numeric_y = bool(sample_meta.get("has_numeric_y"))
    x_is_numeric = bool(sample_meta.get("x_is_numeric"))
    has_size = bool(sample_meta.get("has_size"))
    series_present = bool(sample_meta.get("series_present"))
    cardinality = sample_meta.get("x_cardinality") or 0
    y_count = sample_meta.get("y_count") or 0

    if x_type in {"date", "year"} and has_numeric_y:
        return "line"
    if x_is_numeric and has_numeric_y:
        return "bubble" if has_size else "scatter"
    if x_type in {"string", "enum"} and y_count >= 1:
        if y_count == 1 and cardinality <= 8 and not series_present:
            return "donut"
        if series_present:
            return "bar"
        return "bar"
    return "bar"


def _series_color(index):
    palette = (
        "#0e7c66",
        "#2f5d50",
        "#e9b44c",
        "#b83c4a",
        "#4d8f81",
        "#6e8f5b",
        "#705f91",
        "#3f6f9e",
    )
    return palette[index % len(palette)]


def _render_dataset_label(series_value, y_label, series_field_present):
    if series_field_present:
        return f"{series_value} - {y_label}"
    return y_label


def _validate_spec(source, spec):
    dims, measures, field_map = _catalog_lookup(source)
    measure_fields = {m["field"] for m in measures}

    x_spec = spec.get("x") if isinstance(spec.get("x"), dict) else {}
    x_field = (x_spec.get("field") or "").strip()
    if x_field and x_field not in field_map:
        raise ChartBuilderError(f"Unknown x field '{x_field}' for source '{source}'.")

    y_specs = spec.get("y")
    if not isinstance(y_specs, list) or not y_specs:
        raise ChartBuilderError("At least one y measure is required.")
    normalized_y = []
    for idx, y in enumerate(y_specs):
        if not isinstance(y, dict):
            raise ChartBuilderError(f"Invalid y spec at index {idx}.")
        field = (y.get("field") or "").strip()
        agg = (y.get("agg") or "").strip().lower()
        if not field:
            raise ChartBuilderError(f"Missing y field at index {idx}.")
        if field not in field_map:
            raise ChartBuilderError(f"Unknown y field '{field}' for source '{source}'.")
        if agg not in SUPPORTED_AGGS:
            raise ChartBuilderError(f"Unsupported aggregation '{agg}' for y field '{field}'.")
        if agg != "count" and field not in measure_fields and agg != "count_distinct":
            raise ChartBuilderError(f"Aggregation '{agg}' is only supported on measures for field '{field}'.")
        normalized_y.append(
            {
                "field": field,
                "agg": agg,
                "label": (y.get("label") or field_map[field]["label"]).strip(),
                "color": (y.get("color") or "").strip(),
                "weight_field": (y.get("weight_field") or "").strip() or None,
                "type": field_map[field]["type"],
            }
        )

    series_spec = spec.get("series") if isinstance(spec.get("series"), dict) else {}
    series_field = (series_spec.get("field") or "").strip()
    if series_field and series_field not in field_map:
        raise ChartBuilderError(f"Unknown series field '{series_field}' for source '{source}'.")

    size_spec = spec.get("size") if isinstance(spec.get("size"), dict) else {}
    size_field = (size_spec.get("field") or "").strip()
    size_agg = (size_spec.get("agg") or "sum").strip().lower()
    if size_field:
        if size_field not in field_map:
            raise ChartBuilderError(f"Unknown size field '{size_field}' for source '{source}'.")
        if size_agg not in SUPPORTED_AGGS:
            raise ChartBuilderError(f"Unsupported size aggregation '{size_agg}'.")
        if size_field not in measure_fields and size_agg not in {"count", "count_distinct"}:
            raise ChartBuilderError(f"Size field '{size_field}' must be numeric for agg '{size_agg}'.")

    return {
        "dims": dims,
        "measures": measures,
        "field_map": field_map,
        "x_field": x_field,
        "x_bucket": (x_spec.get("bucket") or "").strip().lower() or None,
        "series_field": series_field,
        "y_specs": normalized_y,
        "size_field": size_field,
        "size_agg": size_agg,
    }


def _build_scatter_payload(rows, source, validated, chart_type_resolved, sort_spec, limit):
    x_field = validated["x_field"]
    y_spec = validated["y_specs"][0]
    series_field = validated["series_field"]
    size_field = validated["size_field"]

    if not x_field:
        raise ChartBuilderError("Scatter/bubble charts require an x field.")
    points = []
    for row in rows:
        x_val = _to_float(row.get(x_field))
        y_val = _to_float(row.get(y_spec["field"]))
        if x_val is None or y_val is None:
            continue
        size_val = _to_float(row.get(size_field)) if size_field else None
        points.append(
            {
                "x": x_val,
                "y": y_val,
                "size_value": size_val,
                "series": row.get(series_field) if series_field else None,
                "row": row,
            }
        )

    default_sort = "x"
    sorted_points = _sort_groups(points, sort_spec, default_sort)
    truncated = False
    warnings = []
    if len(sorted_points) > limit:
        sorted_points = sorted_points[:limit]
        truncated = True
        warnings.append(f"Point cap applied at {limit} rows.")

    grouped = defaultdict(list)
    if series_field:
        for point in sorted_points:
            grouped[str(point.get("series") or "Unspecified")].append(point)
    else:
        grouped[y_spec["label"]].extend(sorted_points)

    datasets = []
    for idx, (series_key, series_points) in enumerate(grouped.items()):
        color = _series_color(idx)
        points_payload = []
        for point in series_points:
            if chart_type_resolved == "bubble":
                radius = 8
                if point["size_value"] is not None:
                    radius = max(4, min(24, math.sqrt(abs(point["size_value"])) * 2.0))
                points_payload.append({"x": point["x"], "y": point["y"], "r": radius})
            else:
                points_payload.append({"x": point["x"], "y": point["y"]})
        datasets.append(
            {
                "label": series_key,
                "data": points_payload,
                "backgroundColor": color + "66",
                "borderColor": color,
                "value_type": y_spec["type"],
            }
        )

    table_rows = []
    for point in sorted_points[:DEFAULT_TABLE_LIMIT]:
        row_data = {
            "x": point["x"],
            "y": point["y"],
            "series": point.get("series"),
        }
        if size_field:
            row_data["size"] = point["size_value"]
        table_rows.append(row_data)
    if len(sorted_points) > DEFAULT_TABLE_LIMIT:
        truncated = True
        warnings.append(f"Table rows capped at {DEFAULT_TABLE_LIMIT}.")

    table_columns = [
        {"key": "x", "label": validated["field_map"][x_field]["label"], "type": validated["field_map"][x_field]["type"]},
        {"key": "y", "label": y_spec["label"], "type": y_spec["type"]},
    ]
    if series_field:
        table_columns.append(
            {
                "key": "series",
                "label": validated["field_map"][series_field]["label"],
                "type": validated["field_map"][series_field]["type"],
            }
        )
    if size_field:
        table_columns.append(
            {
                "key": "size",
                "label": validated["field_map"][size_field]["label"],
                "type": validated["field_map"][size_field]["type"],
            }
        )

    return {
        "chart_type_resolved": chart_type_resolved,
        "labels": [],
        "datasets": datasets,
        "table_columns": table_columns,
        "table_rows": table_rows,
        "meta": {
            "row_count": len(points),
            "truncated": truncated,
            "warnings": warnings,
            "source": source,
        },
    }


def _build_grouped_payload(rows, source, validated, chart_type_resolved, sort_spec, limit):
    x_field = validated["x_field"]
    x_bucket = validated["x_bucket"]
    series_field = validated["series_field"]
    y_specs = validated["y_specs"]
    field_map = validated["field_map"]

    grouped = defaultdict(list)
    for row in rows:
        x_val = _bucket_value(row.get(x_field), field_map.get(x_field), x_bucket) if x_field else "All Rows"
        series_val = row.get(series_field) if series_field else None
        grouped[(x_val, series_val)].append(row)

    aggregate_rows = []
    for (x_val, series_val), group_rows in grouped.items():
        row_out = {"x": x_val, "series": series_val}
        for y in y_specs:
            row_out[y["label"]] = _aggregate(
                group_rows,
                y["field"],
                y["agg"],
                source,
                weight_field=y.get("weight_field"),
            )
        aggregate_rows.append(row_out)

    sorted_rows = _sort_groups(aggregate_rows, sort_spec, "x")
    warnings = []
    truncated = False

    allowed_x = []
    seen_x = set()
    for row in sorted_rows:
        x_val = row.get("x")
        if x_val in seen_x:
            continue
        seen_x.add(x_val)
        allowed_x.append(x_val)
    if len(allowed_x) > limit:
        allowed_x = allowed_x[:limit]
        truncated = True
        warnings.append(f"Category cap applied at {limit}.")

    filtered_rows = [row for row in sorted_rows if row.get("x") in set(allowed_x)]
    labels = allowed_x

    datasets = []
    if series_field:
        series_values = []
        for row in filtered_rows:
            series_val = row.get("series")
            if series_val not in series_values:
                series_values.append(series_val)
        for y in y_specs:
            for idx, series_val in enumerate(series_values):
                data = []
                for x_val in labels:
                    found = next(
                        (
                            r
                            for r in filtered_rows
                            if r.get("x") == x_val and r.get("series") == series_val
                        ),
                        None,
                    )
                    data.append(found.get(y["label"]) if found is not None else None)
                color = y["color"] or _series_color(idx)
                datasets.append(
                    {
                        "label": _render_dataset_label(series_val, y["label"], True),
                        "data": data,
                        "backgroundColor": color,
                        "borderColor": color,
                        "value_type": y["type"],
                    }
                )
    else:
        for idx, y in enumerate(y_specs):
            data = []
            for x_val in labels:
                found = next((r for r in filtered_rows if r.get("x") == x_val), None)
                data.append(found.get(y["label"]) if found is not None else None)
            color = y["color"] or _series_color(idx)
            datasets.append(
                {
                    "label": y["label"],
                    "data": data,
                    "backgroundColor": color,
                    "borderColor": color,
                    "value_type": y["type"],
                }
            )

    table_rows = filtered_rows[:DEFAULT_TABLE_LIMIT]
    if len(filtered_rows) > DEFAULT_TABLE_LIMIT:
        truncated = True
        warnings.append(f"Table rows capped at {DEFAULT_TABLE_LIMIT}.")

    table_columns = [{"key": "x", "label": field_map.get(x_field, {}).get("label", "Group"), "type": field_map.get(x_field, {}).get("type", "string")}]
    if series_field:
        table_columns.append({"key": "series", "label": field_map[series_field]["label"], "type": field_map[series_field]["type"]})
    for y in y_specs:
        table_columns.append({"key": y["label"], "label": y["label"], "type": y["type"]})

    return {
        "chart_type_resolved": chart_type_resolved,
        "labels": labels,
        "datasets": datasets,
        "table_columns": table_columns,
        "table_rows": table_rows,
        "meta": {
            "row_count": len(aggregate_rows),
            "truncated": truncated,
            "warnings": warnings,
            "source": source,
        },
    }


def build_chart_field_catalog(team_id, firm_id, global_filters):
    filters = _normalize_global_filters(global_filters or {})
    sources = []
    for source in SUPPORTED_SOURCES:
        dims, measures, _field_map = _catalog_lookup(source)
        try:
            row_count = _row_count_for_source(source, team_id, firm_id, filters)
        except Exception:
            row_count = 0
        wavg_supported = DEFAULT_WEIGHT_FIELD_BY_SOURCE.get(source) is not None
        sources.append(
            {
                "key": source,
                "label": FIELD_CATALOG[source]["label"],
                "dimensions": dims,
                "measures": measures,
                "default_weight_field": DEFAULT_WEIGHT_FIELD_BY_SOURCE.get(source),
                "wavg_supported": wavg_supported,
                "row_count": row_count,
            }
        )

    return {
        "sources": sources,
        "default_source": "deals",
        "global_filters": filters,
    }


def run_chart_query(spec, team_id, firm_id, global_filters):
    if not isinstance(spec, dict):
        raise ChartBuilderError("Request body must be a JSON object.")
    source = (spec.get("source") or "").strip()
    if source not in SUPPORTED_SOURCES:
        raise ChartBuilderError(f"Unsupported source '{source}'.")

    filters = _normalize_global_filters(global_filters or {})
    validated = _validate_spec(source, spec)
    rows = _rows_for_source(source, team_id, firm_id, filters)
    rows = _apply_local_filters(rows, spec.get("filters"), validated["field_map"])

    x_field = validated["x_field"]
    x_meta = validated["field_map"].get(x_field)
    has_numeric_y = any(y["type"] in NUMERIC_TYPES for y in validated["y_specs"])
    x_cardinality = 0
    if x_field:
        bucket = validated["x_bucket"]
        values = {
            _bucket_value(row.get(x_field), x_meta, bucket)
            for row in rows
            if row.get(x_field) is not None
        }
        x_cardinality = len(values)

    sample_meta = {
        "x_type": x_meta.get("type") if x_meta else None,
        "x_is_numeric": _field_numeric(x_meta),
        "has_numeric_y": has_numeric_y,
        "has_size": bool(validated["size_field"]),
        "series_present": bool(validated["series_field"]),
        "x_cardinality": x_cardinality,
        "y_count": len(validated["y_specs"]),
    }

    chart_type_resolved = resolve_auto_chart_type(spec, sample_meta)
    sort_spec = spec.get("sort") if isinstance(spec.get("sort"), dict) else {}

    if chart_type_resolved in {"scatter", "bubble"}:
        if chart_type_resolved == "bubble":
            point_limit = min(_resolve_limit(spec.get("limit"), DEFAULT_SCATTER_LIMIT), DEFAULT_SCATTER_LIMIT)
        else:
            point_limit = min(_resolve_limit(spec.get("limit"), DEFAULT_SCATTER_LIMIT), DEFAULT_SCATTER_LIMIT)
        payload = _build_scatter_payload(rows, source, validated, chart_type_resolved, sort_spec, point_limit)
    else:
        group_limit = min(_resolve_limit(spec.get("limit"), DEFAULT_GROUP_LIMIT), DEFAULT_GROUP_LIMIT)
        payload = _build_grouped_payload(rows, source, validated, chart_type_resolved, sort_spec, group_limit)

    if chart_type_resolved in {"bar", "line", "area"} and validated["series_field"]:
        payload["meta"]["stacked"] = True
    return payload

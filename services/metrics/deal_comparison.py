"""Cross-firm deal-level comparison analytics."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from statistics import median

from services.metrics.common import safe_divide


def _deal_hold_years(deal):
    """Return hold period in years, or None if dates are missing."""
    start = deal.investment_date
    end = deal.exit_date or deal.as_of_date
    if start is None or end is None:
        return None
    if isinstance(start, str) or isinstance(end, str):
        return None
    delta = (end - start).days
    if delta <= 0:
        return None
    return round(delta / 365.25, 1)


def _deal_moic(deal):
    """Compute gross MOIC directly from deal fields (lightweight)."""
    equity = deal.equity_invested
    if not equity or equity <= 0:
        return None
    realised = deal.realized_value or 0
    unrealised = deal.unrealized_value or 0
    total = realised + unrealised
    if total == 0:
        return None
    return round(total / equity, 2)


def _deal_row(deal, firm_id, firm_name, fund_vintage_lookup):
    """Build a single deal row dict with lightweight metrics."""
    moic = _deal_moic(deal)
    hold = _deal_hold_years(deal)
    vintage = None
    if deal.year_invested:
        vintage = deal.year_invested
    elif deal.investment_date:
        vintage = deal.investment_date.year

    return {
        "firm_id": firm_id,
        "firm_name": firm_name,
        "deal_id": deal.id,
        "company_name": deal.company_name or "Unknown",
        "fund_name": deal.fund_number or "Unknown Fund",
        "sector": deal.sector or "Unknown",
        "geography": deal.geography or "Unknown",
        "status": deal.status or "Unknown",
        "exit_type": deal.exit_type,
        "year_invested": vintage,
        "hold_period": hold,
        "equity_invested": deal.equity_invested,
        "moic": moic,
        "irr": deal.irr,
        "realized_value": deal.realized_value,
        "unrealized_value": deal.unrealized_value,
        "total_value": (deal.realized_value or 0) + (deal.unrealized_value or 0),
    }


def _apply_comparison_filters(rows, filters):
    """Apply AND-across-categories, multi-select-within filters to deal rows."""
    if not filters:
        return rows
    result = rows
    for key in ("sector", "geography", "status", "exit_type"):
        vals = filters.get(key)
        if vals:
            val_set = set(vals)
            result = [r for r in result if r.get(key) in val_set]
    vintage_vals = filters.get("vintage")
    if vintage_vals:
        vintage_set = set(vintage_vals)
        result = [r for r in result if r.get("year_invested") in vintage_set]
    return result


def compute_deal_level_comparison(firms_data, filters=None):
    """Build a cross-firm deal-level comparison payload.

    Parameters
    ----------
    firms_data : list[dict]
        Each dict: { firm_id, firm_name, deals, fund_vintage_lookup }
    filters : dict, optional
        { sector: [], geography: [], status: [], vintage: [], exit_type: [] }

    Returns
    -------
    dict with keys:
        deal_rows     – list of deal dicts with firm_name/firm_id attached
        firm_summaries – { firm_id: { deal_count, avg_moic, avg_irr, ... } }
        filter_options – { sector: [...], geography: [...], ... }
        kpi           – { total_deals, weighted_avg_moic, weighted_avg_irr, median_hold }
    """
    all_rows = []

    for firm in firms_data:
        firm_id = firm["firm_id"]
        firm_name = firm["firm_name"]
        deals = firm["deals"]
        fvl = firm.get("fund_vintage_lookup") or {}

        for deal in deals:
            all_rows.append(_deal_row(deal, firm_id, firm_name, fvl))

    # Build filter options BEFORE applying filters (so dropdowns show all options)
    filter_options = _build_filter_options(all_rows)

    # Apply filters
    filtered = _apply_comparison_filters(all_rows, filters)

    # Sort by MOIC descending, None to bottom
    filtered.sort(key=lambda r: (r["moic"] is None, -(r["moic"] or 0)))

    # Build firm summaries
    by_firm = defaultdict(list)
    for row in filtered:
        by_firm[row["firm_id"]].append(row)

    firm_summaries = {}
    for fid, rows in by_firm.items():
        moic_vals = [r["moic"] for r in rows if r["moic"] is not None]
        irr_vals = [r["irr"] for r in rows if r["irr"] is not None]
        hold_vals = [r["hold_period"] for r in rows if r["hold_period"] is not None]
        firm_summaries[fid] = {
            "firm_name": rows[0]["firm_name"],
            "deal_count": len(rows),
            "avg_moic": safe_divide(sum(moic_vals), len(moic_vals)) if moic_vals else None,
            "avg_irr": safe_divide(sum(irr_vals), len(irr_vals)) if irr_vals else None,
            "median_hold": median(hold_vals) if hold_vals else None,
        }

    # Build KPI aggregates
    all_moics = [r["moic"] for r in filtered if r["moic"] is not None]
    all_irrs = [r["irr"] for r in filtered if r["irr"] is not None]
    all_holds = [r["hold_period"] for r in filtered if r["hold_period"] is not None]

    # Weighted averages (by equity invested)
    w_moic_num, w_moic_den = 0, 0
    w_irr_num, w_irr_den = 0, 0
    for r in filtered:
        eq = r["equity_invested"] or 0
        if eq > 0 and r["moic"] is not None:
            w_moic_num += r["moic"] * eq
            w_moic_den += eq
        if eq > 0 and r["irr"] is not None:
            w_irr_num += r["irr"] * eq
            w_irr_den += eq

    kpi = {
        "total_deals": len(filtered),
        "weighted_avg_moic": safe_divide(w_moic_num, w_moic_den),
        "weighted_avg_irr": safe_divide(w_irr_num, w_irr_den),
        "median_hold": median(all_holds) if all_holds else None,
    }

    return {
        "deal_rows": filtered,
        "firm_summaries": firm_summaries,
        "filter_options": filter_options,
        "kpi": kpi,
    }


def _build_filter_options(rows):
    """Extract unique filter values from deal rows."""
    sectors = set()
    geos = set()
    statuses = set()
    exit_types = set()
    vintages = set()
    for r in rows:
        if r.get("sector"):
            sectors.add(r["sector"])
        if r.get("geography"):
            geos.add(r["geography"])
        if r.get("status"):
            statuses.add(r["status"])
        if r.get("exit_type"):
            exit_types.add(r["exit_type"])
        if r.get("year_invested"):
            vintages.add(r["year_invested"])
    return {
        "sector": sorted(sectors),
        "geography": sorted(geos),
        "status": sorted(statuses),
        "exit_type": sorted(exit_types),
        "vintage": sorted(vintages),
    }

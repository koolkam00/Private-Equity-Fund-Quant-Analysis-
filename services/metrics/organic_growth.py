"""Organic vs Acquired Growth Analysis — decompose revenue and EBITDA growth
into organic (operational) and acquired (bolt-on M&A) contributions.

Only includes deals with add-on activity (non-zero acquired_revenue, acquired_ebitda,
or acquired_tev). Deals without add-on data are excluded from this analysis.

Methodology:
- Organic Revenue Growth = Exit Revenue - Entry Revenue - Acquired Revenue
- Acquired Revenue Contribution = Acquired Revenue (cumulative at bolt-on entry)
- Organic CAGR = ((Exit Revenue - Acquired Revenue) / Entry Revenue)^(1/hold) - 1

Limitation: Organic CAGR assumes acquired revenue existed for the full hold period.
For deals with mid-hold acquisitions, this overstates organic CAGR slightly.

Bridge integration: For deals with ebitda_additive bridge and acquired data, the
revenue driver is decomposed into organic and acquired sub-components using
acquired_ebitda * x0 (entry EV/EBITDA multiple) for the acquired portion.
"""

from __future__ import annotations

from services.metrics.common import EPS, safe_divide, resolve_analysis_as_of_date
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import compute_bridge_aggregate, compute_portfolio_analytics


def _wavg(pairs):
    """Equity-weighted average. Returns None if no valid pairs."""
    if not pairs:
        return None
    numer = sum(v * w for v, w in pairs)
    denom = sum(w for _, w in pairs)
    if denom <= 0:
        return None
    return numer / denom


def compute_organic_growth_analysis(deals, metrics_by_id=None):
    """Build the organic vs acquired analysis payload.

    Only includes deals with add-on activity (acquired_data_provided).
    Returns a dict with summary_cards, deal_rows, charts, bridge_decomposition,
    aggregate_bridge, and portfolio_metrics for the add-on subset.
    """
    if metrics_by_id is None:
        as_of = resolve_analysis_as_of_date(deals)
        metrics_by_id = {d.id: compute_deal_metrics(d, as_of_date=as_of) for d in deals}

    # Filter to only deals with add-on activity
    all_deal_count = len(deals)
    acq_deals = [
        d for d in deals
        if metrics_by_id.get(d.id, {}).get("acquired_data_status") == "acquired_data_provided"
    ]
    has_any_acquired_data = len(acq_deals) > 0

    deal_rows = []

    # Accumulators for portfolio-level aggregates (add-on deals only)
    total_organic_rev = 0.0
    total_acquired_rev = 0.0
    total_organic_ebitda = 0.0
    total_acquired_ebitda = 0.0
    rev_count = 0
    ebitda_count = 0

    # For equity-weighted portfolio CAGRs
    organic_rev_cagr_pairs = []
    total_rev_cagr_pairs = []
    organic_ebitda_cagr_pairs = []
    total_ebitda_cagr_pairs = []

    # Bridge decomposition rows
    bridge_rows = []

    for deal in acq_deals:
        m = metrics_by_id.get(deal.id)
        if m is None:
            continue

        equity = m.get("equity") or 0

        row = {
            "deal_id": deal.id,
            "company_name": deal.company_name,
            "fund_number": deal.fund_number,
            "sector": deal.sector,
            "status": deal.status,
            "hold_period": m.get("hold_period"),
            "equity": equity,
            "moic": m.get("moic"),
            "acquired_data_status": m.get("acquired_data_status"),
            # Revenue
            "entry_revenue": m.get("entry_revenue"),
            "exit_revenue": m.get("exit_revenue"),
            "acquired_revenue": m.get("acquired_revenue"),
            "organic_revenue_growth": m.get("organic_revenue_growth"),
            "acquired_revenue_contribution": m.get("acquired_revenue_contribution"),
            "total_revenue_growth": m.get("total_revenue_growth"),
            "organic_revenue_pct": m.get("organic_revenue_pct"),
            "acquired_revenue_pct": m.get("acquired_revenue_pct"),
            "organic_revenue_cagr": m.get("organic_revenue_cagr"),
            "total_revenue_cagr": m.get("revenue_cagr"),
            # EBITDA
            "entry_ebitda": m.get("entry_ebitda"),
            "exit_ebitda": m.get("exit_ebitda"),
            "acquired_ebitda": m.get("acquired_ebitda"),
            "organic_ebitda_growth": m.get("organic_ebitda_growth"),
            "acquired_ebitda_contribution": m.get("acquired_ebitda_contribution"),
            "total_ebitda_growth": m.get("total_ebitda_growth"),
            "organic_ebitda_pct": m.get("organic_ebitda_pct"),
            "acquired_ebitda_pct": m.get("acquired_ebitda_pct"),
            "organic_ebitda_cagr": m.get("organic_ebitda_cagr"),
            "total_ebitda_cagr": m.get("ebitda_cagr"),
            # TEV
            "acquired_tev": m.get("acquired_tev"),
            "entry_enterprise_value": m.get("entry_enterprise_value"),
            "exit_enterprise_value": m.get("exit_enterprise_value"),
        }
        deal_rows.append(row)

        # Accumulate portfolio totals (add-on deals only)
        if m.get("organic_revenue_growth") is not None:
            total_organic_rev += m["organic_revenue_growth"]
            total_acquired_rev += m.get("acquired_revenue_contribution") or 0
            rev_count += 1
        if m.get("organic_ebitda_growth") is not None:
            total_organic_ebitda += m["organic_ebitda_growth"]
            total_acquired_ebitda += m.get("acquired_ebitda_contribution") or 0
            ebitda_count += 1

        # Equity-weighted CAGR pairs
        if m.get("organic_revenue_cagr") is not None and equity > 0:
            organic_rev_cagr_pairs.append((m["organic_revenue_cagr"], equity))
        if m.get("revenue_cagr") is not None and equity > 0:
            total_rev_cagr_pairs.append((m["revenue_cagr"], equity))
        if m.get("organic_ebitda_cagr") is not None and equity > 0:
            organic_ebitda_cagr_pairs.append((m["organic_ebitda_cagr"], equity))
        if m.get("ebitda_cagr") is not None and equity > 0:
            total_ebitda_cagr_pairs.append((m["ebitda_cagr"], equity))

        # Bridge decomposition: split revenue driver into organic vs acquired
        bridge = m.get("bridge_additive_fund") or {}
        if (
            bridge.get("ready")
            and bridge.get("calculation_method") == "ebitda_additive"
        ):
            drivers = bridge.get("company_drivers_dollar") or {}
            total_rev_driver = drivers.get("revenue")

            # Use acquired_ebitda * x0 for better margin accuracy
            entry_ebitda = deal.entry_ebitda
            entry_ev = deal.entry_enterprise_value
            acq_ebitda = m.get("acquired_ebitda") or 0
            x0 = safe_divide(entry_ev, entry_ebitda)
            ownership = bridge.get("ownership_pct") or 1.0

            if x0 is not None and x0 > 0 and total_rev_driver is not None:
                acquired_bridge = acq_ebitda * x0 * ownership
                organic_bridge = (total_rev_driver * ownership) - acquired_bridge
                bridge_rows.append({
                    "deal_id": deal.id,
                    "company_name": deal.company_name,
                    "fund_number": deal.fund_number,
                    "organic_revenue_contribution": organic_bridge,
                    "acquired_revenue_contribution": acquired_bridge,
                    "total_revenue_driver": total_rev_driver * ownership,
                    "margin_contribution": (drivers.get("margin") or 0) * ownership,
                    "multiple_contribution": (drivers.get("multiple") or 0) * ownership,
                    "leverage_contribution": (drivers.get("leverage") or 0) * ownership,
                })

    # Sort deal rows by MOIC desc
    deal_rows.sort(key=lambda r: -(r["moic"] or 0))

    # Portfolio-level aggregates (add-on deals only)
    total_rev_growth = total_organic_rev + total_acquired_rev
    total_ebitda_growth_sum = total_organic_ebitda + total_acquired_ebitda

    summary_cards = {
        "deals_total": all_deal_count,
        "deals_with_acquisitions": len(acq_deals),
        "deals_with_revenue_data": rev_count,
        "deals_with_ebitda_data": ebitda_count,
        # Revenue
        "portfolio_organic_revenue_growth": total_organic_rev if rev_count else None,
        "portfolio_acquired_revenue_growth": total_acquired_rev if rev_count else None,
        "portfolio_total_revenue_growth": total_rev_growth if rev_count else None,
        "portfolio_organic_revenue_pct": safe_divide(total_organic_rev, total_rev_growth)
        if rev_count and abs(total_rev_growth) > EPS
        else None,
        "portfolio_organic_revenue_cagr": _wavg(organic_rev_cagr_pairs),
        "portfolio_total_revenue_cagr": _wavg(total_rev_cagr_pairs),
        # EBITDA
        "portfolio_organic_ebitda_growth": total_organic_ebitda if ebitda_count else None,
        "portfolio_acquired_ebitda_growth": total_acquired_ebitda if ebitda_count else None,
        "portfolio_total_ebitda_growth": total_ebitda_growth_sum if ebitda_count else None,
        "portfolio_organic_ebitda_pct": safe_divide(total_organic_ebitda, total_ebitda_growth_sum)
        if ebitda_count and abs(total_ebitda_growth_sum) > EPS
        else None,
        "portfolio_organic_ebitda_cagr": _wavg(organic_ebitda_cagr_pairs),
        "portfolio_total_ebitda_cagr": _wavg(total_ebitda_cagr_pairs),
    }

    # Chart data (all deal_rows now have acquired data)
    charts = {
        "organic_vs_acquired_revenue": {
            "labels": [r["company_name"] or "Unknown" for r in deal_rows],
            "organic": [r.get("organic_revenue_growth") for r in deal_rows],
            "acquired": [r.get("acquired_revenue_contribution") for r in deal_rows],
        },
        "organic_vs_acquired_ebitda": {
            "labels": [r["company_name"] or "Unknown" for r in deal_rows],
            "organic": [r.get("organic_ebitda_growth") for r in deal_rows],
            "acquired": [r.get("acquired_ebitda_contribution") for r in deal_rows],
        },
        "cagr_comparison": {
            "labels": [r["company_name"] or "Unknown" for r in deal_rows],
            "organic_revenue_cagr": [r.get("organic_revenue_cagr") for r in deal_rows],
            "total_revenue_cagr": [r.get("total_revenue_cagr") for r in deal_rows],
            "organic_ebitda_cagr": [r.get("organic_ebitda_cagr") for r in deal_rows],
            "total_ebitda_cagr": [r.get("total_ebitda_cagr") for r in deal_rows],
        },
    }

    # Aggregate value creation bridge for the add-on subset
    acq_metrics = {d.id: metrics_by_id[d.id] for d in acq_deals if d.id in metrics_by_id}
    aggregate_bridge = compute_bridge_aggregate(acq_deals, basis="fund") if acq_deals else {}

    # Portfolio analytics for the add-on subset
    portfolio_metrics = compute_portfolio_analytics(acq_deals, metrics_by_id=acq_metrics) if acq_deals else {}

    return {
        "meta": {
            "title": "Organic vs Acquired Growth",
            "as_of_date": str(resolve_analysis_as_of_date(deals)) if deals else None,
            "has_acquired_data": has_any_acquired_data,
            "deals_shown": len(acq_deals),
            "deals_total": all_deal_count,
        },
        "summary_cards": summary_cards,
        "deal_rows": deal_rows,
        "charts": charts,
        "bridge_decomposition": bridge_rows,
        "aggregate_bridge": aggregate_bridge,
        "portfolio_metrics": portfolio_metrics,
        "methodology_notes": [
            "Only deals with add-on activity (non-zero acquired revenue, EBITDA, or TEV) are shown.",
            "Organic growth is derived as: Exit Total - Entry Base - Acquired bolt-on contribution.",
            "Organic CAGR assumes acquired revenue/EBITDA existed for the full hold period. "
            "For deals with mid-hold acquisitions, this may slightly overstate organic CAGR.",
            "Bridge decomposition uses acquired_ebitda × entry EV/EBITDA multiple to capture "
            "margin differences between platform and bolt-on acquisitions.",
            "Bridge split is only available for deals using the EBITDA additive bridge method.",
        ],
    }

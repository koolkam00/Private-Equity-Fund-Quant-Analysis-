"""Executive Summary Analysis — one-page comprehensive fund overview.

Composes existing metric functions into a single payload that covers:
- Fund-level KPIs (MOIC, IRR, value created)
- Portfolio health score (composite 0-100)
- Performance attribution (value bridge drivers)
- Deal ranking (top/bottom performers, outliers)
- Sector & concentration analysis
- Vintage year cohort analysis
- Auto-generated executive summary text
"""

from collections import defaultdict

from services.metrics.common import safe_divide
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import (
    compute_bridge_aggregate,
    compute_moic_hold_scatter,
    compute_portfolio_analytics,
    compute_realized_unrealized_exposure,
    compute_value_creation_mix,
    compute_vintage_series,
)


def _get_moic(deal, metrics_by_id):
    """Get MOIC for a deal from pre-computed metrics."""
    m = metrics_by_id.get(deal.id, {})
    return m.get("moic")


def _get_irr(deal, metrics_by_id):
    """Get gross IRR for a deal from pre-computed metrics."""
    m = metrics_by_id.get(deal.id, {})
    return m.get("gross_irr") or deal.irr


# ---------------------------------------------------------------------------
# Health Score
# ---------------------------------------------------------------------------

def _compute_health_score(deals, portfolio, metrics_by_id):
    """Composite fund health score 0-100 from four components (each 0-25)."""
    if not deals:
        return {"score": 0, "moic": 0, "irr": 0, "diversification": 0, "realization": 0}

    # MOIC component (0-25): 1x=0, 2x=15, 3x+=25
    gross_moic_data = portfolio.get("returns", {}).get("gross_moic", {})
    wavg_moic = gross_moic_data.get("wavg") or gross_moic_data.get("avg") or 0
    moic_score = min(25, max(0, (wavg_moic - 1.0) * 12.5))

    # IRR component (0-25): 0%=0, 15%=15, 25%+=25
    irr_data = portfolio.get("returns", {}).get("gross_irr", {})
    wavg_irr = irr_data.get("wavg") or irr_data.get("avg") or 0
    irr_score = min(25, max(0, (wavg_irr or 0) * 100))

    # Diversification (0-25): inverse of top-3 concentration
    equities = sorted([(d.equity_invested or 0) for d in deals], reverse=True)
    total_eq = sum(equities)
    if total_eq > 0 and len(equities) >= 3:
        top3_pct = sum(equities[:3]) / total_eq
        div_score = max(0, min(25, (1 - top3_pct) * 25))
    else:
        div_score = 0

    # Realization (0-25): % of deals fully realized
    n = len(deals)
    realized = sum(1 for d in deals if (d.status or "").lower() in ("fully realized", "realized"))
    real_score = (realized / n) * 25 if n > 0 else 0

    total = int(round(moic_score + irr_score + div_score + real_score))
    return {
        "score": min(100, total),
        "moic": int(round(moic_score)),
        "irr": int(round(irr_score)),
        "diversification": int(round(div_score)),
        "realization": int(round(real_score)),
    }


# ---------------------------------------------------------------------------
# Concentration
# ---------------------------------------------------------------------------

def _compute_concentration(deals, metrics_by_id):
    """Top-3 deal and sector concentration metrics."""
    if not deals:
        return {"top3_pct": None, "top3_deals": [], "sectors": {}}

    sorted_deals = sorted(deals, key=lambda d: d.equity_invested or 0, reverse=True)
    total_equity = sum(d.equity_invested or 0 for d in deals)
    top3 = sorted_deals[:3]
    top3_pct = safe_divide(sum(d.equity_invested or 0 for d in top3), total_equity)

    top3_deals = [
        {
            "name": d.company_name,
            "equity": d.equity_invested or 0,
            "pct": safe_divide(d.equity_invested or 0, total_equity, 0) * 100,
        }
        for d in top3
    ]

    sectors = defaultdict(lambda: {"count": 0, "equity": 0, "moics": [], "irrs": []})
    for d in deals:
        s = d.sector or "Unclassified"
        sectors[s]["count"] += 1
        sectors[s]["equity"] += d.equity_invested or 0
        moic = _get_moic(d, metrics_by_id)
        irr = _get_irr(d, metrics_by_id)
        if moic is not None:
            sectors[s]["moics"].append(moic)
        if irr is not None:
            sectors[s]["irrs"].append(irr)

    sector_summary = {}
    for name, data in sectors.items():
        sector_summary[name] = {
            "count": data["count"],
            "equity": data["equity"],
            "pct": safe_divide(data["equity"], total_equity, 0) * 100,
            "avg_moic": safe_divide(sum(data["moics"]), len(data["moics"])),
            "avg_irr": safe_divide(sum(data["irrs"]), len(data["irrs"])),
        }

    return {"top3_pct": top3_pct, "top3_deals": top3_deals, "sectors": sector_summary}


# ---------------------------------------------------------------------------
# Deal Ranking
# ---------------------------------------------------------------------------

def _compute_deal_ranking(deals, metrics_by_id, rank_by="moic"):
    """Rank deals by MOIC or IRR. Identify top/bottom 5 and outliers."""
    ranking = []
    for d in deals:
        moic = _get_moic(d, metrics_by_id)
        irr = _get_irr(d, metrics_by_id)
        if rank_by == "irr":
            val = irr * 100 if irr is not None else None
        else:
            val = moic
        ranking.append({
            "id": d.id,
            "name": d.company_name,
            "fund": d.fund_number or "—",
            "sector": d.sector or "—",
            "status": d.status or "Unrealized",
            "equity": d.equity_invested or 0,
            "moic": moic,
            "irr": irr * 100 if irr is not None else None,
            "value": val,
        })

    ranking.sort(key=lambda x: x["value"] or -999, reverse=True)

    with_moic = [r for r in ranking if r["moic"] is not None]
    top5 = with_moic[:5]
    bottom5 = list(reversed(with_moic[-5:])) if len(with_moic) > 5 else []

    # Outlier detection: 1.5 std devs from mean MOIC
    outlier_ids = set()
    moic_vals = [r["moic"] for r in ranking if r["moic"] is not None]
    if len(moic_vals) >= 3:
        mean = sum(moic_vals) / len(moic_vals)
        std = (sum((v - mean) ** 2 for v in moic_vals) / len(moic_vals)) ** 0.5
        if std > 0:
            for r in ranking:
                if r["moic"] is not None and abs(r["moic"] - mean) > 1.5 * std:
                    outlier_ids.add(r["id"])

    return {
        "ranking": ranking,
        "top5": top5,
        "bottom5": bottom5,
        "outlier_ids": list(outlier_ids),
    }


# ---------------------------------------------------------------------------
# Executive Summary Text
# ---------------------------------------------------------------------------

def _generate_summary_text(deals, portfolio, concentration, bridge):
    """Auto-generate 3-4 sentence executive summary from computed metrics."""
    parts = []

    total_equity = portfolio.get("total_equity", 0)
    n = len(deals)
    funds = {d.fund_number for d in deals if d.fund_number}

    gross_moic_data = portfolio.get("returns", {}).get("gross_moic", {})
    wavg_moic = gross_moic_data.get("wavg") or gross_moic_data.get("avg") or 0

    if n > 0:
        fund_label = f"Fund {next(iter(funds))}" if len(funds) == 1 else f"{len(funds)} funds"
        parts.append(
            f"{fund_label} deployed {total_equity:,.1f} across {n} deals"
            f" with a gross MOIC of {wavg_moic:.2f}x."
        )

    # Primary value driver from bridge
    display_drivers = bridge.get("display_drivers") if bridge else []
    if display_drivers:
        driver_label_map = {
            "revenue": "revenue growth",
            "ebitda_growth": "EBITDA growth",
            "margin": "margin expansion",
            "multiple": "multiple expansion",
            "leverage": "leverage / debt paydown",
        }
        driver_effects = {}
        for dd in display_drivers:
            key = dd.get("key", "")
            if key in driver_label_map:
                driver_effects[driver_label_map[key]] = abs(dd.get("dollar") or 0)
        if any(v > 0 for v in driver_effects.values()):
            top_driver = max(driver_effects, key=driver_effects.get)
            parts.append(f"Returns are primarily driven by {top_driver}.")

    # Concentration
    if concentration and concentration.get("top3_pct") is not None:
        pct = concentration["top3_pct"]
        if pct > 0.6:
            parts.append(f"Concentration risk is elevated — top 3 deals represent {pct*100:.0f}% of equity.")
        elif pct > 0.4:
            parts.append(f"Portfolio is moderately concentrated — top 3 deals at {pct*100:.0f}% of equity.")
        else:
            parts.append(f"Portfolio is well-diversified — top 3 deals at {pct*100:.0f}% of equity.")

    # Realization
    realized = sum(1 for d in deals if (d.status or "").lower() in ("fully realized", "realized"))
    if n > 0:
        parts.append(f"{realized} of {n} deals ({realized/n*100:.0f}%) are fully realized.")

    return " ".join(parts) if parts else "Upload deal data to generate an executive summary."


# ---------------------------------------------------------------------------
# Fund Comparison
# ---------------------------------------------------------------------------

def _compute_fund_breakdown(deals, metrics_by_id):
    """Per-fund summary for multi-fund comparison."""
    fund_names = sorted({d.fund_number for d in deals if d.fund_number})
    if len(fund_names) < 2:
        return {}

    result = {}
    for fund in fund_names:
        fd = [d for d in deals if d.fund_number == fund]
        n = len(fd)
        total_eq = sum(d.equity_invested or 0 for d in fd)
        total_val = sum((d.realized_value or 0) + (d.unrealized_value or 0) for d in fd)
        wavg_moic = safe_divide(total_val, total_eq)
        irr_pairs = [(d.irr, d.equity_invested or 0) for d in fd if d.irr is not None]
        tie = sum(w for _, w in irr_pairs)
        wavg_irr = safe_divide(sum(v * w for v, w in irr_pairs), tie)
        realized = sum(1 for d in fd if (d.status or "").lower() in ("fully realized", "realized"))
        value_created = total_val - total_eq

        result[fund] = {
            "deals": n,
            "total_equity": total_eq,
            "wavg_moic": wavg_moic,
            "wavg_irr": wavg_irr,
            "realized": realized,
            "value_created": value_created,
        }

    return result


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def compute_executive_summary_analysis(filtered_deals, metrics_by_id=None, firm_id=None, team_id=None, rank_by="moic"):
    """Compute the comprehensive executive summary payload."""
    if metrics_by_id is None:
        metrics_by_id = {d.id: compute_deal_metrics(d) for d in filtered_deals}

    # --- Reuse existing portfolio analytics ---
    portfolio = compute_portfolio_analytics(filtered_deals, metrics_by_id=metrics_by_id)

    # --- Value bridge (fund-level) ---
    bridge = compute_bridge_aggregate(filtered_deals, basis="fund")

    # --- Scatter data (MOIC vs hold period) ---
    scatter = compute_moic_hold_scatter(filtered_deals, metrics_by_id=metrics_by_id)

    # --- Vintage series ---
    vintage = compute_vintage_series(filtered_deals, metrics_by_id=metrics_by_id)

    # --- Value creation mix ---
    value_mix = compute_value_creation_mix(filtered_deals, metrics_by_id=metrics_by_id)

    # --- Realized/unrealized exposure ---
    exposure = compute_realized_unrealized_exposure(filtered_deals)

    # --- Custom computations ---
    health_score = _compute_health_score(filtered_deals, portfolio, metrics_by_id)
    concentration = _compute_concentration(filtered_deals, metrics_by_id)
    deal_ranking = _compute_deal_ranking(filtered_deals, metrics_by_id, rank_by=rank_by)
    fund_breakdown = _compute_fund_breakdown(filtered_deals, metrics_by_id)

    # --- Aggregates ---
    total_equity = portfolio.get("total_equity", 0)
    total_realized = sum(d.realized_value or 0 for d in filtered_deals)
    total_unrealized = sum(d.unrealized_value or 0 for d in filtered_deals)
    value_created = portfolio.get("total_value_created", 0)

    # --- Takeaway text ---
    takeaway = _generate_summary_text(filtered_deals, portfolio, concentration, bridge)

    # --- Coverage and confidence ---
    n = len(filtered_deals)
    has_moic = sum(1 for d in filtered_deals if _get_moic(d, metrics_by_id) is not None)
    has_irr = sum(1 for d in filtered_deals if d.irr is not None)
    has_entry = sum(1 for d in filtered_deals if d.entry_enterprise_value is not None)
    has_exit = sum(1 for d in filtered_deals if d.exit_enterprise_value is not None)

    coverage = {
        "moic": safe_divide(has_moic, n, 0),
        "irr": safe_divide(has_irr, n, 0),
        "entry_financials": safe_divide(has_entry, n, 0),
        "exit_financials": safe_divide(has_exit, n, 0),
    }

    risk_flags = []
    if concentration.get("top3_pct") and concentration["top3_pct"] > 0.6:
        risk_flags.append(f"Top 3 deals represent {concentration['top3_pct']*100:.0f}% of equity — elevated concentration risk.")
    if n > 0 and coverage["moic"] < 0.5:
        risk_flags.append(f"Only {has_moic} of {n} deals have MOIC data — portfolio metrics may be incomplete.")
    if bridge and bridge.get("coverage", 0) < 0.3:
        risk_flags.append("Value bridge covers less than 30% of deals — attribution chart reflects partial portfolio.")

    return {
        "takeaway": takeaway,
        "coverage": coverage,
        "confidence": coverage,
        "risk_flags": risk_flags,

        "total_deals": n,
        "total_equity": total_equity,
        "total_realized": total_realized,
        "total_unrealized": total_unrealized,
        "value_created": value_created,
        "portfolio": portfolio,

        "health_score": health_score,
        "bridge": bridge,
        "deal_ranking": deal_ranking,
        "rank_by": rank_by,
        "concentration": concentration,
        "vintage": vintage,
        "scatter": scatter,
        "value_mix": value_mix,
        "exposure": exposure,
        "fund_breakdown": fund_breakdown,
    }

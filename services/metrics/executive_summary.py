"""Executive Summary Analysis — LP-question-driven one-page fund overview.

Organized around 5 LP questions:
1. Is this fund on track or in trouble? (Performance)
2. Where is my money? (Capital Deployment)
3. What's driving returns? (Value Creation)
4. What should I worry about? (Risk Watchlist)
5. How does this compare? (Peer Context)

Each section produces a traffic-light signal (green/amber/red/gray) and headline.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date

from services.metrics.common import safe_divide, resolve_analysis_as_of_date
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import (
    compute_bridge_aggregate,
    compute_moic_hold_scatter,
    compute_portfolio_analytics,
    compute_realized_unrealized_exposure,
    compute_value_creation_mix,
    compute_vintage_series,
)
from services.metrics.risk import compute_loss_and_distribution
from services.metrics.benchmarking import rank_benchmark_metric

# Minimum deals for meaningful signal computation
MIN_DEALS_FOR_SIGNAL = 3


def _is_realized_status(status):
    """Consistent realized-status check aligned with portfolio.py normalization."""
    s = (status or "").strip().lower()
    return s in ("fully realized", "realized", "full realization")


def _get_moic(deal, metrics_by_id):
    m = metrics_by_id.get(deal.id, {})
    return m.get("moic")


def _get_irr(deal, metrics_by_id):
    m = metrics_by_id.get(deal.id, {})
    return m.get("gross_irr") or deal.irr


# ---------------------------------------------------------------------------
# Signal Functions (one per LP question)
# ---------------------------------------------------------------------------

def _performance_signal(portfolio, benchmark_result):
    """Section 1: Is this fund on track or in trouble?"""
    gross_moic_data = portfolio.get("returns", {}).get("gross_moic", {})
    wavg_moic = gross_moic_data.get("wavg") or gross_moic_data.get("avg")

    if wavg_moic is None:
        return {"signal": "gray", "headline": "Insufficient data"}

    # Check benchmark quartile if available
    best_rank = None
    if benchmark_result:
        for row in benchmark_result.get("fund_rows") or []:
            cr = row.get("composite_rank") or {}
            code = cr.get("rank_code")
            if code and code != "na":
                best_rank = code
                break  # Use first fund's composite rank

    # Determine signal: RED > AMBER > GREEN
    if wavg_moic < 1.0 or best_rank == "q4":
        signal = "red"
    elif wavg_moic < 1.5 or best_rank in ("q2", "q3"):
        signal = "amber"
    elif wavg_moic >= 1.5:
        signal = "green"
    else:
        signal = "amber"

    headline = f"{wavg_moic:.2f}x MOIC"
    if best_rank and best_rank != "na":
        rank_labels = {"top5": "Top 5%", "q1": "Q1", "q2": "Q2", "q3": "Q3", "q4": "Q4"}
        headline += f", {rank_labels.get(best_rank, '')}"

    return {"signal": signal, "headline": headline}


def _capital_signal(portfolio, concentration, deals, metrics_by_id, as_of_date):
    """Section 2: Where is my money?"""
    total_equity = portfolio.get("total_equity", 0)
    total_realized = portfolio.get("total_realized", 0)
    total_unrealized = portfolio.get("total_unrealized", 0)
    dpi = safe_divide(total_realized, total_equity)
    top3_pct = concentration.get("top3_pct")

    if total_equity is None or total_equity == 0:
        return {
            "signal": "gray", "headline": "No equity data",
            "dpi": None, "top3_pct": None, "unrealized_aging_count": 0,
        }

    # Count unrealized deals held >5 years
    aging_count = 0
    ref_date = as_of_date or date.today()
    for d in deals:
        if _is_realized_status(d.status):
            continue
        m = metrics_by_id.get(d.id, {})
        hold = m.get("hold_period")
        if hold is not None and hold >= 5 and m.get("unrealized", 0) > 0:
            aging_count += 1

    # Signal logic
    if dpi is not None and dpi < 0.2:
        signal = "red"
    elif top3_pct is not None and top3_pct > 0.60:
        signal = "red"
    elif (dpi is not None and dpi < 0.5) or (top3_pct is not None and top3_pct > 0.40):
        signal = "amber"
    elif dpi is not None and dpi > 0.5 and top3_pct is not None and top3_pct < 0.40:
        signal = "green"
    else:
        signal = "amber"

    headline_parts = []
    if dpi is not None:
        headline_parts.append(f"DPI {dpi:.2f}x")
    if top3_pct is not None:
        headline_parts.append(f"top-3 {top3_pct*100:.0f}%")
    headline = ", ".join(headline_parts) if headline_parts else "Limited data"

    return {
        "signal": signal, "headline": headline,
        "dpi": dpi, "top3_pct": top3_pct,
        "unrealized_aging_count": aging_count,
        "total_equity": total_equity,
        "total_realized": total_realized,
        "total_unrealized": total_unrealized,
    }


def _value_creation_signal(bridge):
    """Section 3: What's driving returns?"""
    drivers = bridge.get("display_drivers") if bridge else []
    ready_count = bridge.get("ready_count", 0) if bridge else 0
    bridge_coverage = bridge.get("coverage", 0) if bridge else 0

    if not drivers or ready_count == 0:
        return {
            "signal": "gray", "headline": "Insufficient bridge data",
            "primary_driver": None, "bridge_coverage": 0,
        }

    # Find primary driver by absolute $ value
    driver_label_map = {
        "revenue": "revenue growth",
        "ebitda_growth": "EBITDA growth",
        "margin": "margin expansion",
        "multiple": "multiple expansion",
        "leverage": "leverage / debt paydown",
    }
    driver_effects = {}
    for dd in drivers:
        key = dd.get("key", "")
        if key in driver_label_map:
            driver_effects[key] = abs(dd.get("dollar") or 0)

    primary = max(driver_effects, key=driver_effects.get) if driver_effects else None

    # Compute operational share (revenue + margin as % of total value created)
    total_driver_sum = sum(abs(dd.get("dollar") or 0) for dd in drivers if dd.get("key") != "other")
    operational_sum = sum(
        abs(dd.get("dollar") or 0) for dd in drivers
        if dd.get("key") in ("revenue", "margin", "ebitda_growth")
    )
    leverage_sum = sum(
        abs(dd.get("dollar") or 0) for dd in drivers
        if dd.get("key") == "leverage"
    )

    op_share = safe_divide(operational_sum, total_driver_sum) if total_driver_sum > 0 else None
    lev_share = safe_divide(leverage_sum, total_driver_sum) if total_driver_sum > 0 else None

    if op_share is not None and op_share > 0.5:
        signal = "green"
    elif lev_share is not None and lev_share > 0.5:
        signal = "red"
    else:
        signal = "amber"

    headline = f"Driven by {driver_label_map.get(primary, 'operations')}" if primary else "Mixed drivers"

    return {
        "signal": signal, "headline": headline,
        "primary_driver": primary,
        "bridge_coverage": bridge_coverage,
    }


def _risk_signal(deals, metrics_by_id, as_of_date):
    """Section 4: What should I worry about?"""
    ref_date = as_of_date or date.today()

    # Loss ratios (already in % form 0-100)
    loss_data = compute_loss_and_distribution(deals, metrics_by_id=metrics_by_id)
    loss_ratios = loss_data.get("loss_ratios", {})
    loss_ratio_capital = loss_ratios.get("capital_pct")  # 0-100 scale
    loss_count = loss_ratios.get("loss_count", 0)

    # Stale marks: unrealized deals where as_of_date is None or >180 days old
    stale_count = 0
    for d in deals:
        if _is_realized_status(d.status):
            continue
        m = metrics_by_id.get(d.id, {})
        if m.get("unrealized", 0) <= 0:
            continue
        deal_as_of = getattr(d, "as_of_date", None)
        if deal_as_of is None or (ref_date - deal_as_of).days > 180:
            stale_count += 1

    # Deals below plan (unrealized with MOIC < 1.0x)
    below_plan = []
    for d in deals:
        if _is_realized_status(d.status):
            continue
        m = metrics_by_id.get(d.id, {})
        moic = m.get("moic")
        unrealized = m.get("unrealized", 0)
        if moic is not None and moic < 1.0 and unrealized > 0:
            below_plan.append({
                "id": d.id,
                "name": d.company_name,
                "fund": d.fund_number or "—",
                "moic": moic,
                "equity": m.get("equity", 0),
                "unrealized": unrealized,
            })
    below_plan.sort(key=lambda x: x["equity"], reverse=True)
    top3_risk = below_plan[:3]

    # Signal: compare loss_ratio_capital (0-100 scale) against thresholds
    if (loss_ratio_capital is not None and loss_ratio_capital > 25) or stale_count > 0:
        signal = "red"
    elif loss_ratio_capital is not None and loss_ratio_capital > 10:
        signal = "amber"
    elif loss_ratio_capital is not None and loss_ratio_capital <= 10 and stale_count == 0:
        signal = "green"
    else:
        signal = "gray"

    if signal == "green":
        headline = "No concerns"
    else:
        parts = []
        if loss_count > 0:
            parts.append(f"{loss_count} losses")
        if stale_count > 0:
            parts.append(f"{stale_count} stale marks")
        headline = ", ".join(parts) if parts else "Limited data"

    return {
        "signal": signal, "headline": headline,
        "loss_ratio_capital": loss_ratio_capital,
        "loss_ratio_count": loss_ratios.get("count_pct"),
        "loss_count": loss_count,
        "stale_marks_count": stale_count,
        "below_plan_count": len(below_plan),
        "top3_risk_deals": top3_risk,
        "moic_distribution": loss_data.get("moic_distribution", []),
    }


def _peer_signal(benchmark_result):
    """Section 5: How does this compare?"""
    if not benchmark_result or not benchmark_result.get("fund_rows"):
        return {
            "signal": "gray",
            "headline": "No benchmarks uploaded",
            "benchmark_available": False,
            "fund_rows": [],
        }

    fund_rows = benchmark_result.get("fund_rows", [])
    any_ranked = any(
        row.get("composite_rank", {}).get("rank_code", "na") != "na"
        for row in fund_rows
    )
    if not any_ranked:
        return {
            "signal": "gray",
            "headline": "No benchmark matches",
            "benchmark_available": True,
            "fund_rows": fund_rows,
        }

    # Aggregate quartile positions
    rank_codes = []
    for row in fund_rows:
        code = row.get("composite_rank", {}).get("rank_code")
        if code and code != "na":
            rank_codes.append(code)

    q4_count = sum(1 for c in rank_codes if c == "q4")
    top_half = sum(1 for c in rank_codes if c in ("top5", "q1", "q2"))

    if q4_count > 0:
        signal = "red"
    elif top_half == len(rank_codes):
        signal = "green"
    else:
        signal = "amber"

    # Best composite label
    best = rank_codes[0] if rank_codes else "na"
    label_map = {"top5": "Top 5%", "q1": "1st Quartile", "q2": "2nd Quartile", "q3": "3rd Quartile", "q4": "4th Quartile"}
    headline = label_map.get(best, "Mixed") + (" composite" if len(rank_codes) == 1 else " avg")

    return {
        "signal": signal, "headline": headline,
        "benchmark_available": True,
        "fund_rows": fund_rows,
    }


# ---------------------------------------------------------------------------
# Retained helpers
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

    sectors = defaultdict(lambda: {"count": 0, "equity": 0, "moic_pairs": [], "irr_pairs": []})
    for d in deals:
        s = d.sector or "Unclassified"
        eq = d.equity_invested or 0
        sectors[s]["count"] += 1
        sectors[s]["equity"] += eq
        moic = _get_moic(d, metrics_by_id)
        irr = _get_irr(d, metrics_by_id)
        if moic is not None:
            sectors[s]["moic_pairs"].append((moic, eq))
        if irr is not None:
            sectors[s]["irr_pairs"].append((irr, eq))

    sector_summary = {}
    for name, data in sectors.items():
        moic_w = [(v, w) for v, w in data["moic_pairs"] if w > 0]
        irr_w = [(v, w) for v, w in data["irr_pairs"] if w > 0]
        wavg_moic = (sum(v * w for v, w in moic_w) / sum(w for _, w in moic_w)) if moic_w else (
            sum(v for v, _ in data["moic_pairs"]) / len(data["moic_pairs"]) if data["moic_pairs"] else None
        )
        wavg_irr = (sum(v * w for v, w in irr_w) / sum(w for _, w in irr_w)) if irr_w else (
            sum(v for v, _ in data["irr_pairs"]) / len(data["irr_pairs"]) if data["irr_pairs"] else None
        )
        sector_summary[name] = {
            "count": data["count"],
            "equity": data["equity"],
            "pct": safe_divide(data["equity"], total_equity, 0) * 100,
            "avg_moic": wavg_moic,
            "avg_irr": wavg_irr,
        }

    return {"top3_pct": top3_pct, "top3_deals": top3_deals, "sectors": sector_summary}


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

    outlier_ids = set()
    moic_vals = [r["moic"] for r in ranking if r["moic"] is not None]
    if len(moic_vals) >= 3:
        mean = sum(moic_vals) / len(moic_vals)
        std = (sum((v - mean) ** 2 for v in moic_vals) / (len(moic_vals) - 1)) ** 0.5
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


def _compute_health_score(deals, portfolio, metrics_by_id):
    """Composite fund health score 0-100 (retained for backward compatibility)."""
    if not deals:
        return {"score": 0, "moic": 0, "irr": 0, "diversification": 0, "realization": 0}

    gross_moic_data = portfolio.get("returns", {}).get("gross_moic", {})
    wavg_moic = gross_moic_data.get("wavg") or gross_moic_data.get("avg") or 0
    moic_score = min(25, max(0, (wavg_moic - 1.0) * 12.5))

    irr_data = portfolio.get("returns", {}).get("gross_irr", {})
    wavg_irr = irr_data.get("wavg") or irr_data.get("avg") or 0
    irr_score = min(25, max(0, (wavg_irr or 0) * 100))

    equities = sorted([(d.equity_invested or 0) for d in deals], reverse=True)
    total_eq = sum(equities)
    if total_eq > 0 and len(equities) >= 3:
        top3_pct = sum(equities[:3]) / total_eq
        div_score = max(0, min(25, (1 - top3_pct) * 25))
    else:
        div_score = 0

    n = len(deals)
    realized = sum(1 for d in deals if _is_realized_status(d.status))
    real_score = (realized / n) * 25 if n > 0 else 0

    total = int(round(moic_score + irr_score + div_score + real_score))
    return {
        "score": min(100, total),
        "moic": int(round(moic_score)),
        "irr": int(round(irr_score)),
        "diversification": int(round(div_score)),
        "realization": int(round(real_score)),
    }


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
        realized = sum(1 for d in fd if _is_realized_status(d.status))
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

def compute_executive_summary_analysis(
    filtered_deals,
    metrics_by_id=None,
    firm_id=None,
    team_id=None,
    rank_by="moic",
    benchmark_thresholds=None,
    benchmark_asset_class="",
    as_of_date=None,
):
    """Compute the LP-question-driven executive summary payload."""
    if metrics_by_id is None:
        metrics_by_id = {d.id: compute_deal_metrics(d) for d in filtered_deals}

    as_of = as_of_date or resolve_analysis_as_of_date(filtered_deals)
    n = len(filtered_deals)

    # --- Reuse existing compute functions ---
    portfolio = compute_portfolio_analytics(filtered_deals, metrics_by_id=metrics_by_id)
    bridge = compute_bridge_aggregate(filtered_deals, basis="fund")
    scatter = compute_moic_hold_scatter(filtered_deals, metrics_by_id=metrics_by_id)
    vintage = compute_vintage_series(filtered_deals, metrics_by_id=metrics_by_id)
    value_mix = compute_value_creation_mix(filtered_deals, metrics_by_id=metrics_by_id)
    exposure = compute_realized_unrealized_exposure(filtered_deals)
    concentration = _compute_concentration(filtered_deals, metrics_by_id)
    deal_ranking = _compute_deal_ranking(filtered_deals, metrics_by_id, rank_by=rank_by)
    fund_breakdown = _compute_fund_breakdown(filtered_deals, metrics_by_id)
    health_score = _compute_health_score(filtered_deals, portfolio, metrics_by_id)

    # --- Benchmark (optional — only if thresholds provided) ---
    benchmark_result = None
    if benchmark_thresholds:
        from peqa.services.filtering import build_fund_vintage_lookup
        benchmark_result = {
            "fund_rows": [],
        }
        try:
            from services.metrics.benchmarking import compute_benchmarking_analysis
            benchmark_result = compute_benchmarking_analysis(
                filtered_deals,
                benchmark_thresholds=benchmark_thresholds,
                benchmark_asset_class=benchmark_asset_class,
                metrics_by_id=metrics_by_id,
                as_of_date=as_of,
                fund_vintage_lookup=build_fund_vintage_lookup(filtered_deals, team_id=team_id, firm_id=firm_id),
            )
        except Exception:
            pass  # Benchmark failure should not break executive summary

    # --- Compute 5 section signals ---
    too_few_deals = n < MIN_DEALS_FOR_SIGNAL

    perf = _performance_signal(portfolio, benchmark_result)
    capital = _capital_signal(portfolio, concentration, filtered_deals, metrics_by_id, as_of)
    creation = _value_creation_signal(bridge)
    risk = _risk_signal(filtered_deals, metrics_by_id, as_of)
    peers = _peer_signal(benchmark_result)

    # Override all signals to gray when portfolio is too small
    if too_few_deals:
        for section in (perf, capital, creation, risk, peers):
            section["signal"] = "gray"
            section["headline"] = f"Filtered to {n} deal{'s' if n != 1 else ''}"

    # --- Portfolio Pulse (traffic light array) ---
    pulse = [
        {"label": "Performance", "signal": perf["signal"], "headline": perf["headline"]},
        {"label": "Capital", "signal": capital["signal"], "headline": capital["headline"]},
        {"label": "Value Creation", "signal": creation["signal"], "headline": creation["headline"]},
        {"label": "Risks", "signal": risk["signal"], "headline": risk["headline"]},
        {"label": "Peers", "signal": peers["signal"], "headline": peers["headline"]},
    ]

    # --- Aggregates ---
    total_equity = portfolio.get("total_equity", 0)
    total_realized = sum(d.realized_value or 0 for d in filtered_deals)
    total_unrealized = sum(d.unrealized_value or 0 for d in filtered_deals)
    value_created = portfolio.get("total_value_created", 0)

    # --- Coverage ---
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
        # LP-question sections
        "pulse": pulse,
        "section_performance": perf,
        "section_capital": capital,
        "section_value_creation": creation,
        "section_risk": risk,
        "section_peers": peers,

        # Retained top-level fields
        "coverage": coverage,
        "confidence": coverage,
        "risk_flags": risk_flags,

        "total_deals": n,
        "total_equity": total_equity,
        "total_realized": total_realized,
        "total_unrealized": total_unrealized,
        "value_created": value_created,
        "portfolio": portfolio,

        # Retained for backward compat and Details section
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
        "benchmark_result": benchmark_result,
        "filtered_deal_count": n,
    }

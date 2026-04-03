"""Organic vs Acquired Growth Attribution — full portfolio analysis.

Includes ALL deals (not just those with bolt-on data). Decomposes growth into
organic (operational) and acquired (bolt-on M&A) contributions. Classifies deals
into cohorts: Pure Organic, Acquisition-Augmented, Acquisition-Dependent.

Complementary to VCA EBITDA: VCA explains WHERE returns came from (EBITDA growth
vs multiple expansion vs leverage). This page explains WHERE GROWTH came from
(organic operations vs acquisitions).
"""

from __future__ import annotations

import re
from collections import defaultdict

from services.metrics.common import EPS, safe_divide, percentile_rank, resolve_analysis_as_of_date
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import compute_bridge_aggregate


# ---------------------------------------------------------------------------
# Cohort classification
# ---------------------------------------------------------------------------

COHORT_PURE_ORGANIC = "Pure Organic"
COHORT_AUGMENTED = "Acquisition-Augmented"
COHORT_DEPENDENT = "Acquisition-Dependent"


def _classify_cohort(row):
    """Classify a deal into a growth cohort.

    4 cases:
    - No acquisition data -> Pure Organic
    - Has acquisitions, total_growth > 0, organic > 50% -> Augmented
    - Has acquisitions, total_growth > 0, acquired >= 50% -> Dependent
    - Has acquisitions, total_growth <= 0 -> Dependent (declining)
    """
    if row.get("acquired_data_status") != "acquired_data_provided":
        return COHORT_PURE_ORGANIC

    total_growth = row.get("total_revenue_growth")
    if total_growth is None:
        # No revenue data to classify on, but has acquired data
        return COHORT_DEPENDENT

    if total_growth <= 0:
        # Acquisitions failed to offset decline
        return COHORT_DEPENDENT

    organic_pct = row.get("organic_revenue_pct")
    if organic_pct is not None and organic_pct > 0.5:
        return COHORT_AUGMENTED
    return COHORT_DEPENDENT


# ---------------------------------------------------------------------------
# Per-deal computed metrics
# ---------------------------------------------------------------------------

def _compute_organic_margin(row):
    """Compute entry margin, organic exit margin, and margin expansion."""
    entry_rev = row.get("entry_revenue")
    entry_ebitda = row.get("entry_ebitda")
    exit_rev = row.get("exit_revenue")
    exit_ebitda = row.get("exit_ebitda")
    acq_rev = row.get("acquired_revenue") or 0
    acq_ebitda = row.get("acquired_ebitda") or 0

    entry_margin = safe_divide(entry_ebitda, entry_rev)
    organic_exit_rev = (exit_rev or 0) - acq_rev
    organic_exit_ebitda = (exit_ebitda or 0) - acq_ebitda
    organic_exit_margin = safe_divide(organic_exit_ebitda, organic_exit_rev) if organic_exit_rev and abs(organic_exit_rev) > EPS else None

    expansion = None
    if entry_margin is not None and organic_exit_margin is not None:
        expansion = organic_exit_margin - entry_margin

    return {
        "entry_ebitda_margin": entry_margin,
        "organic_exit_ebitda_margin": organic_exit_margin,
        "organic_margin_expansion": expansion,
    }


def _compute_acquisition_efficiency(row):
    """MOIC uplift from acquisitions: blended_moic - hypothetical_organic_moic.

    Returns None if deal has no acquisitions or if organic_value < 0.
    """
    if row.get("acquired_data_status") != "acquired_data_provided":
        return None

    moic = row.get("moic")
    equity = row.get("equity")
    total_value = row.get("total_value")
    if moic is None or equity is None or equity <= 0 or total_value is None:
        return None

    # Compute acquired contribution to value
    acq_tev = row.get("acquired_tev")
    if acq_tev is None or acq_tev <= 0:
        # Fallback: acquired_ebitda * exit_tev_ebitda
        acq_ebitda = row.get("acquired_ebitda")
        exit_tev_ebitda = row.get("exit_tev_ebitda")
        if acq_ebitda is not None and exit_tev_ebitda is not None and exit_tev_ebitda > 0:
            acq_tev = acq_ebitda * exit_tev_ebitda
        else:
            return None

    organic_value = total_value - acq_tev
    if organic_value < 0:
        # Acquisitions exceeded total value -- efficiency undefined
        return None

    organic_moic = safe_divide(organic_value, equity)
    if organic_moic is None:
        return None

    return moic - organic_moic


# ---------------------------------------------------------------------------
# Growth quality score (0-100)
# ---------------------------------------------------------------------------

def _compute_growth_quality_scores(deal_rows):
    """Compute growth quality score (0-100) for each deal in place.

    4 components of 25 pts each, based on percentile rank within portfolio.
    """
    # Collect values for percentile ranking
    org_rev_cagrs = [r.get("organic_revenue_cagr") for r in deal_rows]
    org_ebitda_cagrs = [r.get("organic_ebitda_cagr") for r in deal_rows]
    margin_expansions = [r.get("organic_margin_expansion") for r in deal_rows]

    for row in deal_rows:
        # Component 1: Organic revenue CAGR (25 pts)
        c1 = 25.0 * percentile_rank(row.get("organic_revenue_cagr"), org_rev_cagrs)

        # Component 2: Organic EBITDA CAGR (25 pts)
        c2 = 25.0 * percentile_rank(row.get("organic_ebitda_cagr"), org_ebitda_cagrs)

        # Component 3: Acquisition independence (25 pts)
        acq_pct = row.get("acquired_revenue_pct")
        if acq_pct is not None:
            c3 = 25.0 * max(0.0, 1.0 - acq_pct)
        elif row.get("acquired_data_status") != "acquired_data_provided":
            c3 = 25.0  # Pure organic = full independence
        else:
            c3 = 12.5  # Neutral (has acq data but no growth data)

        # Component 4: Organic margin expansion (25 pts)
        c4 = 25.0 * percentile_rank(row.get("organic_margin_expansion"), margin_expansions)

        score = max(0.0, min(100.0, c1 + c2 + c3 + c4))
        row["growth_quality_score"] = round(score, 1)


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def _wavg(pairs):
    """Equity-weighted average. Returns None if no valid pairs."""
    if not pairs:
        return None
    numer = sum(v * w for v, w in pairs)
    denom = sum(w for _, w in pairs)
    if denom <= 0:
        return None
    return numer / denom


def _aggregate_cohort(rows):
    """Compute aggregate metrics for a list of deal rows."""
    if not rows:
        return None

    deal_count = len(rows)
    total_equity = sum(r.get("equity") or 0 for r in rows)
    total_value = sum(r.get("total_value") or 0 for r in rows)

    # Equity-weighted MOIC
    moic_pairs = [(r["moic"], r["equity"]) for r in rows if r.get("moic") is not None and (r.get("equity") or 0) > 0]
    weighted_moic = _wavg(moic_pairs)

    # Equity-weighted IRR
    irr_pairs = [(r["irr"], r["equity"]) for r in rows if r.get("irr") is not None and (r.get("equity") or 0) > 0]
    weighted_irr = _wavg(irr_pairs)

    # Equity-weighted CAGRs
    org_rev_cagr_pairs = [(r["organic_revenue_cagr"], r["equity"]) for r in rows if r.get("organic_revenue_cagr") is not None and (r.get("equity") or 0) > 0]
    org_ebitda_cagr_pairs = [(r["organic_ebitda_cagr"], r["equity"]) for r in rows if r.get("organic_ebitda_cagr") is not None and (r.get("equity") or 0) > 0]
    total_rev_cagr_pairs = [(r["total_revenue_cagr"], r["equity"]) for r in rows if r.get("total_revenue_cagr") is not None and (r.get("equity") or 0) > 0]

    # Hold period (equity-weighted)
    hold_pairs = [(r["hold_period"], r["equity"]) for r in rows if r.get("hold_period") is not None and (r.get("equity") or 0) > 0]

    # Loss ratio
    moic_deals = [r for r in rows if r.get("moic") is not None]
    loss_count = sum(1 for r in moic_deals if r["moic"] < 1.0)
    loss_ratio = safe_divide(loss_count, len(moic_deals)) if moic_deals else None

    # Growth quality score (equity-weighted)
    gqs_pairs = [(r["growth_quality_score"], r["equity"]) for r in rows if r.get("growth_quality_score") is not None and (r.get("equity") or 0) > 0]

    # Acquisition efficiency (equity-weighted, only for deals with value)
    eff_pairs = [(r["acquisition_efficiency"], r["equity"]) for r in rows if r.get("acquisition_efficiency") is not None and (r.get("equity") or 0) > 0]

    # Organic growth totals
    total_organic_rev = sum(r.get("organic_revenue_growth") or 0 for r in rows if r.get("organic_revenue_growth") is not None)
    total_acquired_rev = sum(r.get("acquired_revenue_contribution") or 0 for r in rows if r.get("acquired_revenue_contribution") is not None)
    total_organic_ebitda = sum(r.get("organic_ebitda_growth") or 0 for r in rows if r.get("organic_ebitda_growth") is not None)
    total_acquired_ebitda = sum(r.get("acquired_ebitda_contribution") or 0 for r in rows if r.get("acquired_ebitda_contribution") is not None)

    total_rev_growth = total_organic_rev + total_acquired_rev
    organic_rev_pct = safe_divide(total_organic_rev, total_rev_growth) if abs(total_rev_growth) > EPS else None

    total_ebitda_growth = total_organic_ebitda + total_acquired_ebitda
    organic_ebitda_pct = safe_divide(total_organic_ebitda, total_ebitda_growth) if abs(total_ebitda_growth) > EPS else None

    return {
        "deal_count": deal_count,
        "total_equity": total_equity,
        "total_value": total_value,
        "weighted_moic": weighted_moic,
        "weighted_irr": weighted_irr,
        "organic_rev_cagr": _wavg(org_rev_cagr_pairs),
        "organic_ebitda_cagr": _wavg(org_ebitda_cagr_pairs),
        "total_rev_cagr": _wavg(total_rev_cagr_pairs),
        "weighted_hold_period": _wavg(hold_pairs),
        "loss_ratio": loss_ratio,
        "avg_growth_quality_score": _wavg(gqs_pairs),
        "avg_acquisition_efficiency": _wavg(eff_pairs),
        "total_organic_rev": total_organic_rev,
        "total_acquired_rev": total_acquired_rev,
        "organic_rev_pct": organic_rev_pct,
        "total_organic_ebitda": total_organic_ebitda,
        "total_acquired_ebitda": total_acquired_ebitda,
        "organic_ebitda_pct": organic_ebitda_pct,
    }


def _fund_sort_key(label):
    """Sort fund labels by Roman numeral: Fund I, Fund II, ..."""
    roman_map = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5,
                 "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10,
                 "XI": 11, "XII": 12, "XIII": 13, "XIV": 14, "XV": 15}
    m = re.search(r'\b([IVX]+)\b', label or "")
    if m and m.group(1) in roman_map:
        return (0, roman_map[m.group(1)], label)
    return (1, 0, label or "")


def _aggregate_by_fund(deal_rows):
    """Aggregate growth metrics by fund."""
    by_fund = defaultdict(list)
    for r in deal_rows:
        fund = r.get("fund_number") or "Unknown Fund"
        by_fund[fund].append(r)

    fund_rows = []
    for fund_name in sorted(by_fund.keys(), key=_fund_sort_key):
        rows = by_fund[fund_name]
        agg = _aggregate_cohort(rows)
        if agg:
            agg["fund"] = fund_name
            # Cohort mix
            cohort_counts = defaultdict(int)
            for r in rows:
                cohort_counts[r.get("cohort", "Unknown")] += 1
            agg["cohort_mix"] = dict(cohort_counts)
            fund_rows.append(agg)

    return fund_rows


# ---------------------------------------------------------------------------
# Chart data builders
# ---------------------------------------------------------------------------

def _build_scatter_data(deal_rows):
    """Build scatter/bubble chart data: organic CAGR vs MOIC."""
    points = []
    for r in deal_rows:
        moic = r.get("moic")
        equity = r.get("equity") or 0
        org_rev_cagr = r.get("organic_revenue_cagr")
        org_ebitda_cagr = r.get("organic_ebitda_cagr")
        if moic is not None:
            points.append({
                "company": r.get("company_name") or "Unknown",
                "moic": moic,
                "equity": equity,
                "cohort": r.get("cohort", COHORT_PURE_ORGANIC),
                "organic_rev_cagr": org_rev_cagr,
                "organic_ebitda_cagr": org_ebitda_cagr,
            })
    return points


def _build_waterfall_data(deal_rows):
    """Build waterfall chart data: portfolio-level growth decomposition."""
    total_entry_rev = 0.0
    total_organic_rev = 0.0
    total_acquired_rev = 0.0
    total_exit_rev = 0.0
    total_entry_ebitda = 0.0
    total_organic_ebitda = 0.0
    total_acquired_ebitda = 0.0
    total_exit_ebitda = 0.0
    rev_deals = 0
    ebitda_deals = 0

    for r in deal_rows:
        if r.get("entry_revenue") is not None and r.get("exit_revenue") is not None:
            total_entry_rev += r["entry_revenue"]
            total_organic_rev += r.get("organic_revenue_growth") or 0
            total_acquired_rev += r.get("acquired_revenue_contribution") or 0
            total_exit_rev += r["exit_revenue"]
            rev_deals += 1
        if r.get("entry_ebitda") is not None and r.get("exit_ebitda") is not None:
            total_entry_ebitda += r["entry_ebitda"]
            total_organic_ebitda += r.get("organic_ebitda_growth") or 0
            total_acquired_ebitda += r.get("acquired_ebitda_contribution") or 0
            total_exit_ebitda += r["exit_ebitda"]
            ebitda_deals += 1

    return {
        "revenue": {
            "entry": total_entry_rev,
            "organic": total_organic_rev,
            "acquired": total_acquired_rev,
            "exit": total_exit_rev,
            "deals": rev_deals,
        },
        "ebitda": {
            "entry": total_entry_ebitda,
            "organic": total_organic_ebitda,
            "acquired": total_acquired_ebitda,
            "exit": total_exit_ebitda,
            "deals": ebitda_deals,
        },
    }


# ---------------------------------------------------------------------------
# Main computation
# ---------------------------------------------------------------------------

def compute_organic_growth_analysis(deals, metrics_by_id=None):
    """Build the full growth attribution payload for ALL deals."""
    if metrics_by_id is None:
        as_of = resolve_analysis_as_of_date(deals)
        metrics_by_id = {d.id: compute_deal_metrics(d, as_of_date=as_of) for d in deals}

    all_deal_count = len(deals)
    deal_rows = []
    bridge_rows = []

    for deal in deals:
        m = metrics_by_id.get(deal.id)
        if m is None:
            continue

        equity = m.get("equity") or 0
        value_total = m.get("value_total") or 0

        row = {
            "deal_id": deal.id,
            "company_name": deal.company_name,
            "fund_number": deal.fund_number,
            "sector": deal.sector,
            "status": deal.status,
            "hold_period": m.get("hold_period"),
            "equity": equity,
            "total_value": value_total,
            "moic": m.get("moic"),
            "irr": m.get("gross_irr"),
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
            "exit_tev_ebitda": m.get("exit_tev_ebitda"),
        }

        # Organic margin
        margin_data = _compute_organic_margin(row)
        row.update(margin_data)

        # Acquisition efficiency
        row["acquisition_efficiency"] = _compute_acquisition_efficiency(row)

        # Cohort classification
        row["cohort"] = _classify_cohort(row)

        deal_rows.append(row)

        # Bridge decomposition (kept from original, for ebitda_additive deals only)
        bridge = m.get("bridge_additive_fund") or {}
        if (
            bridge.get("ready")
            and bridge.get("calculation_method") == "ebitda_additive"
        ):
            drivers = bridge.get("company_drivers_dollar") or {}
            total_rev_driver = drivers.get("revenue")
            entry_ebitda_val = deal.entry_ebitda
            entry_ev = deal.entry_enterprise_value
            acq_ebitda_val = m.get("acquired_ebitda") or 0
            x0 = safe_divide(entry_ev, entry_ebitda_val)
            ownership = bridge.get("ownership_pct") or 1.0

            if x0 is not None and x0 > 0 and total_rev_driver is not None:
                acquired_bridge = acq_ebitda_val * x0 * ownership
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

    # Compute growth quality scores (modifies deal_rows in place)
    _compute_growth_quality_scores(deal_rows)

    # Sort by MOIC desc
    deal_rows.sort(key=lambda r: -(r.get("moic") or 0))

    # Cohort aggregation
    cohort_groups = defaultdict(list)
    for r in deal_rows:
        cohort_groups[r["cohort"]].append(r)

    cohorts = {}
    for cohort_name in [COHORT_PURE_ORGANIC, COHORT_AUGMENTED, COHORT_DEPENDENT]:
        rows = cohort_groups.get(cohort_name, [])
        cohorts[cohort_name] = _aggregate_cohort(rows) if rows else None

    # Total aggregation
    total_agg = _aggregate_cohort(deal_rows) if deal_rows else None

    # Count of deals with acquisitions
    acq_count = sum(1 for r in deal_rows if r.get("acquired_data_status") == "acquired_data_provided")

    # Fund-level aggregation
    fund_comparison = _aggregate_by_fund(deal_rows)

    # Chart data
    scatter_data = _build_scatter_data(deal_rows)
    waterfall_data = _build_waterfall_data(deal_rows)

    # Stacked bar chart data (organic vs acquired per deal)
    chart_organic_acquired = {
        "revenue": {
            "labels": [r["company_name"] or "Unknown" for r in deal_rows],
            "organic": [r.get("organic_revenue_growth") for r in deal_rows],
            "acquired": [r.get("acquired_revenue_contribution") for r in deal_rows],
        },
        "ebitda": {
            "labels": [r["company_name"] or "Unknown" for r in deal_rows],
            "organic": [r.get("organic_ebitda_growth") for r in deal_rows],
            "acquired": [r.get("acquired_ebitda_contribution") for r in deal_rows],
        },
    }

    # Aggregate bridge for deals with acquired data
    acq_deals = [d for d in deals if metrics_by_id.get(d.id, {}).get("acquired_data_status") == "acquired_data_provided"]
    aggregate_bridge = compute_bridge_aggregate(acq_deals, basis="fund") if acq_deals else {}

    as_of_date = resolve_analysis_as_of_date(deals) if deals else None

    return {
        "meta": {
            "title": "Organic vs Acquired Growth",
            "as_of_date": str(as_of_date) if as_of_date else None,
            "has_deals": len(deal_rows) > 0,
            "deals_total": all_deal_count,
            "deals_with_acquisitions": acq_count,
            "deals_pure_organic": all_deal_count - acq_count,
        },
        "deal_rows": deal_rows,
        "cohorts": cohorts,
        "total": total_agg,
        "fund_comparison": fund_comparison,
        "scatter_data": scatter_data,
        "waterfall": waterfall_data,
        "charts": chart_organic_acquired,
        "bridge_decomposition": bridge_rows,
        "aggregate_bridge": aggregate_bridge,
        "methodology_notes": [
            "All deals are included. Deals without bolt-on data are classified as Pure Organic.",
            "Cohort classification: Pure Organic (no acquired data), Acquisition-Augmented "
            "(organic > 50% of growth), Acquisition-Dependent (acquired >= 50% or declining with acquisitions).",
            "Organic growth = Exit Total - Entry Base - Acquired contribution.",
            "Organic CAGR assumes acquired revenue/EBITDA existed for the full hold period.",
            "Growth Quality Score (0-100) uses percentile rank across the portfolio for organic CAGR, "
            "EBITDA CAGR, acquisition independence, and organic margin expansion.",
            "Acquisition Efficiency = blended MOIC - hypothetical organic-only MOIC. "
            "Uses acquired_tev (or acquired_ebitda x exit multiple as fallback).",
            "Organic margin = (exit EBITDA - acquired EBITDA) / (exit revenue - acquired revenue).",
            "Waterfall shows simple dollar-sum decomposition across all deals (not time-weighted).",
            "Bridge decomposition uses acquired_ebitda x entry EV/EBITDA multiple (ebitda_additive method only).",
        ],
    }

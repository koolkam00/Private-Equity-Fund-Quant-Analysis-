"""Data Cuts analytics — slice portfolio performance by qualitative dimensions."""

from __future__ import annotations

from collections import defaultdict
from datetime import date

from services.metrics.common import deal_hold_years, resolve_analysis_as_of_date, safe_divide


# ---------------------------------------------------------------------------
# Dimension registry
# ---------------------------------------------------------------------------

def _deal_vintage_year(deal):
    if deal.year_invested is not None:
        return str(deal.year_invested)
    inv = getattr(deal, "investment_date", None)
    if inv is not None and isinstance(inv, date):
        return str(inv.year)
    return None


import re


def _fund_sort_key(label):
    """Sort fund labels by Roman numeral: Fund I, Fund II, ... Fund X."""
    roman_map = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5,
                 "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10,
                 "XI": 11, "XII": 12, "XIII": 13, "XIV": 14, "XV": 15}
    m = re.search(r'\b([IVX]+)\b', label or "")
    if m and m.group(1) in roman_map:
        return (0, roman_map[m.group(1)], label)
    # Fallback: sort after roman-numbered funds, alphabetically
    return (1, 0, label or "")


def _sort_groups(groups, dim_key):
    """Sort finalized groups by dimension-appropriate order."""
    if dim_key == "vintage_year":
        # Chronological: earliest year first, "Unknown" last
        def _year_key(g):
            try:
                return (0, int(g["label"]))
            except (ValueError, TypeError):
                return (1, 0)
        return sorted(groups, key=_year_key)
    elif dim_key == "fund":
        return sorted(groups, key=lambda g: _fund_sort_key(g["label"]))
    else:
        # Alphabetical, with fallback labels (Unknown, Unassigned, etc.) last
        fallback = DIMENSIONS.get(dim_key, {}).get("fallback", "Unknown")
        return sorted(groups, key=lambda g: (g["label"] == fallback, g["label"] or ""))


DIMENSIONS = {
    "sector": {"field": "sector", "fallback": "Unknown"},
    "geography": {"field": "geography", "fallback": "Unknown"},
    "lead_partner": {"field": "lead_partner", "fallback": "Unassigned"},
    "vintage_year": {"field": None, "resolver": _deal_vintage_year, "fallback": "Unknown"},
    "fund": {"field": "fund_number", "fallback": "Unknown Fund"},
    "status": {"field": "status", "fallback": "Unknown"},
    "deal_type": {"field": "deal_type", "fallback": "Platform"},
    "exit_type": {"field": "exit_type", "fallback": "Not Specified"},
    "entry_channel": {"field": "entry_channel", "fallback": "Unknown"},
}

ALLOWED_METRICS = [
    "weighted_moic",
    "weighted_irr",
    "invested_equity",
    "realized_value",
    "unrealized_value",
    "total_value",
    "value_created",
    "weighted_entry_tev_ebitda",
    "weighted_exit_tev_ebitda",
    "weighted_entry_ebitda_margin",
    "weighted_exit_ebitda_margin",
    "weighted_hold_years",
    "loss_ratio_count",
    "loss_ratio_capital",
    "pct_of_invested",
    "pct_of_total",
]

DIMENSION_LABELS = {
    "sector": "Sector",
    "geography": "Geography",
    "lead_partner": "Lead Partner",
    "vintage_year": "Vintage Year",
    "fund": "Fund",
    "status": "Status",
    "deal_type": "Deal Type",
    "exit_type": "Exit Type",
    "entry_channel": "Entry Channel",
}

# Maximum secondary dimension columns before aggregating into "Other"
MAX_SECONDARY_COLUMNS = 20
# Maximum secondary values shown in chart
MAX_CHART_SECONDARY = 8


# ---------------------------------------------------------------------------
# Bucket helpers
# ---------------------------------------------------------------------------

def _new_bucket():
    return {
        "deal_count": 0,
        "invested_equity": 0.0,
        "realized_value": 0.0,
        "unrealized_value": 0.0,
        "total_value": 0.0,
        "value_created": 0.0,
        # Equity-weighted accumulators (numerator = metric * equity, denominator = equity)
        "_irr_num": 0.0,
        "_irr_den": 0.0,
        "_entry_tev_ebitda_num": 0.0,
        "_entry_tev_ebitda_den": 0.0,
        "_exit_tev_ebitda_num": 0.0,
        "_exit_tev_ebitda_den": 0.0,
        "_entry_ebitda_margin_num": 0.0,
        "_entry_ebitda_margin_den": 0.0,
        "_exit_ebitda_margin_num": 0.0,
        "_exit_ebitda_margin_den": 0.0,
        "_hold_years_num": 0.0,
        "_hold_years_den": 0.0,
        # Loss tracking
        "_loss_count": 0,
        "_loss_capital": 0.0,
        # Deal dicts for drill-down
        "_deals": [],
        # Deal IDs for "Other" recomputation
        "_deal_ids": [],
    }


def _add_to_bucket(bucket, deal, metrics):
    equity = metrics.get("equity") or 0.0
    realized = metrics.get("realized") or 0.0
    unrealized = metrics.get("unrealized") or 0.0
    value_total = metrics.get("value_total") or 0.0
    value_created = metrics.get("value_created") or 0.0
    irr = metrics.get("gross_irr")
    moic = metrics.get("moic")

    bucket["deal_count"] += 1
    bucket["invested_equity"] += equity
    bucket["realized_value"] += realized
    bucket["unrealized_value"] += unrealized
    bucket["total_value"] += value_total
    bucket["value_created"] += value_created
    bucket["_deal_ids"].append(deal.id)

    # All equity-weighted metrics use the same pattern: num += val * equity, den += equity
    if irr is not None and equity > 0:
        bucket["_irr_num"] += irr * equity
        bucket["_irr_den"] += equity

    for key in ("entry_tev_ebitda", "exit_tev_ebitda"):
        val = metrics.get(key)
        if val is not None and equity > 0:
            bucket[f"_{key}_num"] += val * equity
            bucket[f"_{key}_den"] += equity

    # EBITDA margins (already in percentage form 0-100 from deal.py)
    for key in ("entry_ebitda_margin", "exit_ebitda_margin"):
        val = metrics.get(key)
        if val is not None and equity > 0:
            bucket[f"_{key}_num"] += val * equity
            bucket[f"_{key}_den"] += equity

    # Hold period (equity-weighted)
    hold = metrics.get("hold_period")
    if hold is not None and equity > 0:
        bucket["_hold_years_num"] += hold * equity
        bucket["_hold_years_den"] += equity

    # Loss tracking
    if moic is not None and moic < 1.0 and equity > 0:
        bucket["_loss_count"] += 1
        bucket["_loss_capital"] += max(equity - value_total, 0.0)

    # Deal dict for drill-down
    bucket["_deals"].append({
        "id": deal.id,
        "company_name": deal.company_name,
        "fund_number": getattr(deal, "fund_number", None) or "",
        "sector": getattr(deal, "sector", None) or "",
        "geography": getattr(deal, "geography", None) or "",
        "status": getattr(deal, "status", None) or "",
        "equity_invested": equity,
        "moic": moic,
        "irr": irr,
        "hold_years": hold,
        "entry_tev_ebitda": metrics.get("entry_tev_ebitda"),
        "exit_tev_ebitda": metrics.get("exit_tev_ebitda"),
    })


def _finalize_bucket(label, bucket, portfolio_invested=None, portfolio_total_value=None):
    invested = bucket["invested_equity"]
    deal_count = bucket["deal_count"]

    return {
        "label": label,
        "deal_count": deal_count,
        "invested_equity": invested,
        "realized_value": bucket["realized_value"],
        "unrealized_value": bucket["unrealized_value"],
        "total_value": bucket["total_value"],
        "value_created": bucket["value_created"],
        "weighted_moic": safe_divide(bucket["total_value"], invested),
        "weighted_irr": safe_divide(bucket["_irr_num"], bucket["_irr_den"]),
        "weighted_entry_tev_ebitda": safe_divide(bucket["_entry_tev_ebitda_num"], bucket["_entry_tev_ebitda_den"]),
        "weighted_exit_tev_ebitda": safe_divide(bucket["_exit_tev_ebitda_num"], bucket["_exit_tev_ebitda_den"]),
        "weighted_entry_ebitda_margin": safe_divide(bucket["_entry_ebitda_margin_num"], bucket["_entry_ebitda_margin_den"]),
        "weighted_exit_ebitda_margin": safe_divide(bucket["_exit_ebitda_margin_num"], bucket["_exit_ebitda_margin_den"]),
        "weighted_hold_years": safe_divide(bucket["_hold_years_num"], bucket["_hold_years_den"]),
        "loss_ratio_count": safe_divide(bucket["_loss_count"], deal_count) if deal_count > 0 else None,
        "loss_ratio_capital": safe_divide(bucket["_loss_capital"], invested) if invested > 0 else None,
        "pct_of_invested": safe_divide(invested, portfolio_invested) if portfolio_invested else None,
        "pct_of_total": safe_divide(bucket["total_value"], portfolio_total_value) if portfolio_total_value else None,
        "deals": sorted(bucket["_deals"], key=lambda d: d.get("equity_invested") or 0, reverse=True),
        "small_n": deal_count < 3,
    }


# ---------------------------------------------------------------------------
# Dimension resolution
# ---------------------------------------------------------------------------

def _resolve_dim_value(deal, dim_key):
    dim_config = DIMENSIONS.get(dim_key)
    if dim_config is None:
        return "Unknown"
    resolver = dim_config.get("resolver")
    if resolver:
        val = resolver(deal)
        return val if val else dim_config["fallback"]
    field = dim_config["field"]
    val = getattr(deal, field, None)
    return val if val else dim_config["fallback"]


def _validate_dim(dim, default="sector"):
    if dim and dim.lower() in DIMENSIONS:
        return dim.lower()
    return default


# ---------------------------------------------------------------------------
# Main computation
# ---------------------------------------------------------------------------

def compute_data_cuts_analytics(deals, metrics_by_id, primary_dim="sector", secondary_dim=None):
    primary_dim = _validate_dim(primary_dim, "sector")
    if secondary_dim:
        secondary_dim = _validate_dim(secondary_dim, None)
        # Prevent degenerate matrix: same dim on both axes
        if secondary_dim == primary_dim:
            secondary_dim = None

    # Build primary groups
    groups = defaultdict(_new_bucket)
    # Build cross-tab if secondary dim
    cross_tab = defaultdict(lambda: defaultdict(_new_bucket)) if secondary_dim else None
    totals_bucket = _new_bucket()

    for deal in deals:
        m = metrics_by_id.get(deal.id)
        if m is None:
            continue

        primary_val = _resolve_dim_value(deal, primary_dim)
        _add_to_bucket(groups[primary_val], deal, m)
        _add_to_bucket(totals_bucket, deal, m)

        if cross_tab is not None:
            secondary_val = _resolve_dim_value(deal, secondary_dim)
            _add_to_bucket(cross_tab[primary_val][secondary_val], deal, m)

    # Portfolio totals for percentage-of-portfolio calculations
    portfolio_invested = totals_bucket["invested_equity"]
    portfolio_total_value = totals_bucket["total_value"]

    # Finalize primary groups
    finalized_groups = []
    for label, bucket in groups.items():
        finalized_groups.append(_finalize_bucket(label, bucket, portfolio_invested, portfolio_total_value))
    finalized_groups = _sort_groups(finalized_groups, primary_dim)

    totals = _finalize_bucket("Total", totals_bucket, portfolio_invested, portfolio_total_value)

    # Unknown group data quality check
    total_deals = totals["deal_count"]
    unknown_fallback = DIMENSIONS.get(primary_dim, {}).get("fallback", "Unknown")
    unknown_group = next((g for g in finalized_groups if g["label"] == unknown_fallback), None)
    unknown_pct = safe_divide(unknown_group["deal_count"], total_deals) if unknown_group and total_deals > 0 else 0
    data_quality_warning = None
    if unknown_pct and unknown_pct > 0.2:
        data_quality_warning = {
            "dimension": DIMENSION_LABELS.get(primary_dim, primary_dim),
            "count": unknown_group["deal_count"],
            "pct": unknown_pct,
        }

    # As-of date
    as_of_date = resolve_analysis_as_of_date(deals) if deals else None

    # Finalize cross-tab
    finalized_cross_tab = None
    secondary_labels = []
    cross_tab_truncated = False
    truncated_count = 0

    if cross_tab is not None:
        # Collect all secondary labels and sort by total invested equity
        sec_totals = defaultdict(float)
        for primary_label, sec_buckets in cross_tab.items():
            for sec_label, bucket in sec_buckets.items():
                sec_totals[sec_label] += bucket["invested_equity"]

        sorted_sec = sorted(sec_totals.keys(), key=lambda k: sec_totals[k], reverse=True)

        # Cap at MAX_SECONDARY_COLUMNS
        if len(sorted_sec) > MAX_SECONDARY_COLUMNS:
            cross_tab_truncated = True
            truncated_count = len(sorted_sec) - MAX_SECONDARY_COLUMNS
            kept_labels = sorted_sec[:MAX_SECONDARY_COLUMNS]
            overflow_labels = sorted_sec[MAX_SECONDARY_COLUMNS:]
        else:
            kept_labels = sorted_sec
            overflow_labels = []

        secondary_labels = kept_labels + (["Other"] if overflow_labels else [])

        finalized_cross_tab = {}
        # Row totals
        row_totals = {}
        # Column totals
        col_totals = {sl: _new_bucket() for sl in secondary_labels}

        for primary_label in [g["label"] for g in finalized_groups]:
            row = {}
            row_bucket = _new_bucket()
            sec_buckets = cross_tab.get(primary_label, {})

            for sec_label in kept_labels:
                bucket = sec_buckets.get(sec_label)
                if bucket and bucket["deal_count"] > 0:
                    row[sec_label] = _finalize_bucket(sec_label, bucket)
                    # Accumulate into column total
                    for deal in deals:
                        dm = metrics_by_id.get(deal.id)
                        if dm and deal.id in bucket["_deal_ids"]:
                            _add_to_bucket(col_totals[sec_label], deal, dm)
                else:
                    row[sec_label] = None

            # Build "Other" column by re-computing from raw deals
            if overflow_labels:
                other_bucket = _new_bucket()
                for overflow_sec in overflow_labels:
                    overflow_bucket = sec_buckets.get(overflow_sec)
                    if overflow_bucket:
                        for deal in deals:
                            dm = metrics_by_id.get(deal.id)
                            if dm and deal.id in overflow_bucket["_deal_ids"]:
                                _add_to_bucket(other_bucket, deal, dm)
                                _add_to_bucket(col_totals["Other"], deal, dm)
                if other_bucket["deal_count"] > 0:
                    row["Other"] = _finalize_bucket("Other", other_bucket)
                else:
                    row["Other"] = None

            finalized_cross_tab[primary_label] = row

        # Finalize column totals
        finalized_col_totals = {}
        for sl in secondary_labels:
            if col_totals[sl]["deal_count"] > 0:
                finalized_col_totals[sl] = _finalize_bucket(sl, col_totals[sl])
            else:
                finalized_col_totals[sl] = None

    else:
        finalized_col_totals = None

    # Build chart payload (all metrics pre-loaded)
    chart_groups = finalized_groups[:50]  # Cap chart at 50 bars
    chart_labels = [g["label"] for g in chart_groups]

    chart_datasets = {}
    for metric_key in ALLOWED_METRICS:
        chart_datasets[metric_key] = [g.get(metric_key) for g in chart_groups]

    # Secondary dim chart data
    chart_secondary = None
    if cross_tab is not None and finalized_cross_tab:
        chart_sec_labels = secondary_labels[:MAX_CHART_SECONDARY]
        chart_sec_truncated = len(secondary_labels) > MAX_CHART_SECONDARY
        chart_secondary = {
            "secondary_labels": chart_sec_labels,
            "truncated": chart_sec_truncated,
            "total_secondary": len(secondary_labels),
        }

    return {
        "primary_dim": primary_dim,
        "primary_dim_label": DIMENSION_LABELS.get(primary_dim, primary_dim),
        "secondary_dim": secondary_dim,
        "secondary_dim_label": DIMENSION_LABELS.get(secondary_dim, secondary_dim) if secondary_dim else None,
        "groups": finalized_groups,
        "totals": totals,
        "cross_tab": finalized_cross_tab,
        "secondary_labels": secondary_labels if cross_tab is not None else [],
        "col_totals": finalized_col_totals,
        "cross_tab_truncated": cross_tab_truncated,
        "truncated_count": truncated_count,
        "chart_labels": chart_labels,
        "chart_datasets": chart_datasets,
        "chart_secondary": chart_secondary,
        "dimensions": DIMENSIONS,
        "dimension_labels": DIMENSION_LABELS,
        "allowed_metrics": ALLOWED_METRICS,
        "data_quality_warning": data_quality_warning,
        "as_of_date": as_of_date.isoformat() if as_of_date else None,
        "deal_count": totals["deal_count"],
    }

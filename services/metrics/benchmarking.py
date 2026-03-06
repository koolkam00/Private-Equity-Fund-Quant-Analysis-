"""Benchmarking analysis payloads for IC-oriented reporting and print layouts."""

from __future__ import annotations

from peqa.services.filtering import build_fund_vintage_lookup, sort_fund_rows_by_vintage
from services.metrics.common import resolve_analysis_as_of_date, safe_divide
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import compute_deal_track_record

BENCHMARK_METRICS = ("net_irr", "net_moic", "net_dpi")
RANK_CODES = ("top5", "q1", "q2", "q3", "q4", "na")
RANK_LABELS = {
    "top5": "Top 5%",
    "q1": "1st Quartile",
    "q2": "2nd Quartile",
    "q3": "3rd Quartile",
    "q4": "4th Quartile",
    "na": "N/A",
}
RANK_SCORE_MAP = {
    "top5": 5.0,
    "q1": 4.0,
    "q2": 3.0,
    "q3": 2.0,
    "q4": 1.0,
    "na": 0.0,
}


def _deal_vintage_year(deal):
    if getattr(deal, "year_invested", None) is not None:
        try:
            return int(deal.year_invested)
        except (TypeError, ValueError):
            return None
    if getattr(deal, "investment_date", None) is not None:
        return int(deal.investment_date.year)
    return None


def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _composite_rank(score):
    if score is None:
        return {"rank_code": "na", "label": RANK_LABELS["na"]}
    if score >= 4.75:
        return {"rank_code": "top5", "label": RANK_LABELS["top5"]}
    if score >= 3.75:
        return {"rank_code": "q1", "label": RANK_LABELS["q1"]}
    if score >= 2.75:
        return {"rank_code": "q2", "label": RANK_LABELS["q2"]}
    if score >= 1.75:
        return {"rank_code": "q3", "label": RANK_LABELS["q3"]}
    return {"rank_code": "q4", "label": RANK_LABELS["q4"]}


def rank_benchmark_metric(metric_value, vintage_year, metric_name, thresholds, asset_class_selected):
    asset = (asset_class_selected or "").strip()
    if not asset:
        return {"label": "N/A", "rank_code": "na", "reason": "no_asset_class_selected"}
    if metric_value is None:
        return {"label": "N/A", "rank_code": "na", "reason": "missing_metric"}
    if vintage_year is None:
        return {"label": "N/A", "rank_code": "na", "reason": "missing_vintage"}

    metric_thresholds = ((thresholds.get(int(vintage_year)) or {}).get(metric_name)) or {}
    lower = _safe_float(metric_thresholds.get("lower_quartile"))
    median = _safe_float(metric_thresholds.get("median"))
    upper = _safe_float(metric_thresholds.get("upper_quartile"))
    top_5 = _safe_float(metric_thresholds.get("top_5"))

    if lower is None or median is None or upper is None:
        return {"label": "N/A", "rank_code": "na", "reason": "missing_thresholds"}

    value = _safe_float(metric_value)
    if value is None:
        return {"label": "N/A", "rank_code": "na", "reason": "missing_metric"}

    if top_5 is not None and value >= top_5:
        return {"label": RANK_LABELS["top5"], "rank_code": "top5", "reason": None}
    if value >= upper:
        return {"label": RANK_LABELS["q1"], "rank_code": "q1", "reason": None}
    if value >= median:
        return {"label": RANK_LABELS["q2"], "rank_code": "q2", "reason": None}
    if value >= lower:
        return {"label": RANK_LABELS["q3"], "rank_code": "q3", "reason": None}
    return {"label": RANK_LABELS["q4"], "rank_code": "q4", "reason": None}


def compute_benchmarking_analysis(
    deals,
    benchmark_thresholds=None,
    benchmark_asset_class="",
    metrics_by_id=None,
    as_of_date=None,
    fund_vintage_lookup=None,
):
    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    thresholds = benchmark_thresholds or {}
    selected_asset = (benchmark_asset_class or "").strip()
    as_of = as_of_date or resolve_analysis_as_of_date(deals)
    fund_vintage_lookup = fund_vintage_lookup or build_fund_vintage_lookup(deals)

    track_record = compute_deal_track_record(deals, metrics_by_id=metrics_by_id, fund_vintage_lookup=fund_vintage_lookup)

    fund_vintage_years = dict(fund_vintage_lookup)

    rank_distribution = {
        metric: {"label": metric.replace("_", " ").upper(), "counts": {code: 0 for code in RANK_CODES}}
        for metric in BENCHMARK_METRICS
    }

    fund_rows = []
    for fund in track_record.get("funds", []):
        fund_name = fund.get("fund_name") or "Unknown Fund"
        fund_net = fund.get("net_performance") or {}
        conflicts = fund_net.get("conflicts") or {}
        vintage_year = fund_vintage_years.get(fund_name)

        row = {
            "fund_name": fund_name,
            "vintage_year": vintage_year,
            "fund_size": fund.get("fund_size"),
            "fund_size_conflict": bool(fund.get("fund_size_conflict")),
            "net_irr": fund_net.get("net_irr"),
            "net_moic": fund_net.get("net_moic"),
            "net_dpi": fund_net.get("net_dpi"),
            "net_irr_conflict": bool(conflicts.get("net_irr")),
            "net_moic_conflict": bool(conflicts.get("net_moic")),
            "net_dpi_conflict": bool(conflicts.get("net_dpi")),
        }

        available_scores = []
        for metric in BENCHMARK_METRICS:
            conflict_flag = bool(conflicts.get(metric))
            metric_value = fund_net.get(metric)
            ranking_value = None if conflict_flag else metric_value
            rank = rank_benchmark_metric(
                ranking_value,
                vintage_year,
                metric,
                thresholds,
                selected_asset,
            )
            rank_distribution[metric]["counts"][rank["rank_code"]] += 1

            threshold_bucket = ((thresholds.get(vintage_year) or {}).get(metric)) or {}
            median = _safe_float(threshold_bucket.get("median"))
            delta_to_median = None
            rank_value = _safe_float(ranking_value)
            if rank_value is not None and median is not None:
                delta_to_median = rank_value - median

            row[f"benchmark_{metric}"] = rank
            row[f"{metric}_delta_to_median"] = delta_to_median
            row[f"{metric}_median"] = median

            if rank["rank_code"] != "na":
                available_scores.append(RANK_SCORE_MAP[rank["rank_code"]])

        composite_score = safe_divide(sum(available_scores), len(available_scores)) if available_scores else None
        row["composite_score"] = composite_score
        row["composite_rank"] = _composite_rank(composite_score)
        row["any_coverage"] = bool(available_scores)
        row["full_coverage"] = len(available_scores) == len(BENCHMARK_METRICS)
        fund_rows.append(row)

    fund_rows = sort_fund_rows_by_vintage(
        fund_rows,
        vintage_lookup=fund_vintage_lookup,
        fund_key_candidates=("fund_name",),
    )

    threshold_rows = []
    vintage_years = sorted(int(year) for year in thresholds.keys())
    for year in vintage_years:
        vintage_bucket = thresholds.get(year) or {}
        threshold_rows.append(
            {
                "vintage_year": year,
                "net_irr_lower_quartile": ((vintage_bucket.get("net_irr") or {}).get("lower_quartile")),
                "net_irr_median": ((vintage_bucket.get("net_irr") or {}).get("median")),
                "net_irr_upper_quartile": ((vintage_bucket.get("net_irr") or {}).get("upper_quartile")),
                "net_irr_top_5": ((vintage_bucket.get("net_irr") or {}).get("top_5")),
                "net_moic_lower_quartile": ((vintage_bucket.get("net_moic") or {}).get("lower_quartile")),
                "net_moic_median": ((vintage_bucket.get("net_moic") or {}).get("median")),
                "net_moic_upper_quartile": ((vintage_bucket.get("net_moic") or {}).get("upper_quartile")),
                "net_moic_top_5": ((vintage_bucket.get("net_moic") or {}).get("top_5")),
                "net_dpi_lower_quartile": ((vintage_bucket.get("net_dpi") or {}).get("lower_quartile")),
                "net_dpi_median": ((vintage_bucket.get("net_dpi") or {}).get("median")),
                "net_dpi_upper_quartile": ((vintage_bucket.get("net_dpi") or {}).get("upper_quartile")),
                "net_dpi_top_5": ((vintage_bucket.get("net_dpi") or {}).get("top_5")),
            }
        )

    fund_count = len(fund_rows)
    any_coverage_count = sum(1 for row in fund_rows if row.get("any_coverage"))
    full_coverage_count = sum(1 for row in fund_rows if row.get("full_coverage"))

    composite_values = [row["composite_score"] for row in fund_rows if row.get("composite_score") is not None]
    avg_composite_score = safe_divide(sum(composite_values), len(composite_values)) if composite_values else None

    coverage_note = "Select a Benchmark Asset Class to populate quartile rankings."
    if selected_asset and not threshold_rows:
        coverage_note = "No benchmark thresholds found for the selected asset class."
    elif selected_asset and threshold_rows:
        coverage_note = "Quartile rankings require exact vintage-year threshold matches."

    return {
        "meta": {
            "as_of_date": as_of,
            "benchmark_asset_class": selected_asset,
            "coverage_note": coverage_note,
            "vintage_min": vintage_years[0] if vintage_years else None,
            "vintage_max": vintage_years[-1] if vintage_years else None,
            "legend": [
                "Top 5% >= top_5 threshold",
                "Quartile ranks use exact vintage-year matches",
                "Composite score averages available metric rank scores (Top5=5 to Q4=1)",
            ],
            "rank_labels": RANK_LABELS,
        },
        "kpis": {
            "fund_count": fund_count,
            "any_coverage_pct": safe_divide(any_coverage_count, fund_count),
            "full_coverage_pct": safe_divide(full_coverage_count, fund_count),
            "avg_composite_score": avg_composite_score,
        },
        "rank_distribution": rank_distribution,
        "fund_rows": fund_rows,
        "threshold_rows": threshold_rows,
    }

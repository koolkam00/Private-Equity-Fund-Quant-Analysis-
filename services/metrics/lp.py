"""LP-oriented analysis payloads built from fund metadata, fund cashflows, and benchmark series."""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import date
import math
from statistics import median, pstdev

from models import FundCashflow, FundMetadata, FundQuarterSnapshot, PublicMarketIndexLevel
from peqa.services.context import load_team_benchmark_thresholds
from peqa.services.metrics.status import normalize_realization_status
from peqa.services.filtering import deal_vintage_year
from services.metrics.analysis import compute_valuation_quality_analysis
from services.metrics.benchmarking import rank_benchmark_metric
from services.metrics.common import resolve_analysis_as_of_date, safe_divide
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import compute_deal_track_record


BENCHMARK_METRICS = ("net_irr", "net_moic", "net_dpi")
TOP_HALF_RANKS = {"top5", "q1", "q2"}


def _fund_names_from_deals(deals):
    return sorted({d.fund_number or "Unknown Fund" for d in deals})


def _metadata_by_fund(team_id, firm_id, fund_names):
    if team_id is None or firm_id is None or not fund_names:
        return {}
    rows = (
        FundMetadata.query.filter(
            FundMetadata.team_id == team_id,
            FundMetadata.firm_id == firm_id,
            FundMetadata.fund_number.in_(fund_names),
        )
        .order_by(FundMetadata.fund_number.asc(), FundMetadata.id.asc())
        .all()
    )
    return {row.fund_number: row for row in rows}


def _fund_quarter_rows(firm_id, fund_names):
    if firm_id is None or not fund_names:
        return []
    return (
        FundQuarterSnapshot.query.filter(
            FundQuarterSnapshot.firm_id == firm_id,
            FundQuarterSnapshot.fund_number.in_(fund_names),
        )
        .order_by(FundQuarterSnapshot.fund_number.asc(), FundQuarterSnapshot.quarter_end.asc())
        .all()
    )


def _fund_cashflow_rows(team_id, firm_id, fund_names):
    if team_id is None or firm_id is None or not fund_names:
        return []
    return (
        FundCashflow.query.filter(
            FundCashflow.team_id == team_id,
            FundCashflow.firm_id == firm_id,
            FundCashflow.fund_number.in_(fund_names),
        )
        .order_by(FundCashflow.fund_number.asc(), FundCashflow.event_date.asc(), FundCashflow.id.asc())
        .all()
    )


def _public_market_rows(team_id, benchmark_codes):
    if team_id is None or not benchmark_codes:
        return []
    return (
        PublicMarketIndexLevel.query.filter(
            PublicMarketIndexLevel.team_id == team_id,
            PublicMarketIndexLevel.benchmark_code.in_(benchmark_codes),
        )
        .order_by(PublicMarketIndexLevel.benchmark_code.asc(), PublicMarketIndexLevel.level_date.asc())
        .all()
    )


def _latest_fund_rows_by_fund(rows):
    latest = {}
    for row in rows:
        latest[row.fund_number] = row
    return latest


def _milestone_date(series_rows, threshold):
    for row in series_rows:
        dpi = safe_divide(row.distributed_capital, row.paid_in_capital)
        if dpi is not None and dpi >= threshold:
            return row.quarter_end
    return None


def _aggregate_latest_fund_snapshot(latest_rows):
    totals = {
        "paid_in_capital": 0.0,
        "distributed_capital": 0.0,
        "nav": 0.0,
        "committed_capital": 0.0,
    }
    any_data = False
    for row in latest_rows.values():
        any_data = True
        totals["paid_in_capital"] += row.paid_in_capital or 0.0
        totals["distributed_capital"] += row.distributed_capital or 0.0
        totals["nav"] += row.nav or 0.0
        totals["committed_capital"] += row.committed_capital or 0.0
    return totals if any_data else None


def _fund_size_bucket(metadata):
    if metadata is None or metadata.fund_size is None:
        return None
    size = metadata.fund_size or 0.0
    if size < 500:
        return "<500"
    if size < 1000:
        return "500-999"
    if size < 2500:
        return "1000-2499"
    return "2500+"


def _fund_vintage_year_from_lookup(fund_name, fund_metadata, fund_deals):
    metadata = fund_metadata.get(fund_name)
    if metadata is not None and metadata.vintage_year is not None:
        try:
            return int(metadata.vintage_year)
        except (TypeError, ValueError):
            pass
    years = [deal_vintage_year(d) for d in fund_deals]
    years = [y for y in years if y is not None]
    return min(years) if years else None


def _fund_manager_name(fund_name, fund_metadata):
    metadata = fund_metadata.get(fund_name)
    if metadata is not None and metadata.manager_name:
        return metadata.manager_name
    return "Manager Unspecified"


def compute_lp_liquidity_quality_analysis(deals, firm_id=None, team_id=None, as_of_date=None):
    fund_names = _fund_names_from_deals(deals)
    quarter_rows = _fund_quarter_rows(firm_id, fund_names)
    by_fund_quarters = defaultdict(list)
    for row in quarter_rows:
        by_fund_quarters[row.fund_number].append(row)

    latest_by_fund = _latest_fund_rows_by_fund(quarter_rows)
    valuation_quality = compute_valuation_quality_analysis(deals, as_of_date=as_of_date)
    metrics_by_id = {deal.id: compute_deal_metrics(deal) for deal in deals}
    aged_deals = []
    tail_nav_total = 0.0
    total_nav_from_deals = 0.0
    for deal in deals:
        hold_years = (metrics_by_id.get(deal.id) or {}).get("hold_period")
        unrealized = deal.unrealized_value or 0.0
        if unrealized > 0:
            total_nav_from_deals += unrealized
        if unrealized > 0 and hold_years is not None and hold_years >= 5:
            aged_deals.append(deal)
            tail_nav_total += unrealized

    aggregate_latest = _aggregate_latest_fund_snapshot(latest_by_fund)
    if aggregate_latest:
        paid_in = aggregate_latest["paid_in_capital"]
        distributed = aggregate_latest["distributed_capital"]
        nav = aggregate_latest["nav"]
    else:
        paid_in = distributed = nav = None

    fund_rows = []
    milestone_rollup_source = []
    for fund_name in fund_names:
        series_rows = by_fund_quarters.get(fund_name, [])
        latest = latest_by_fund.get(fund_name)
        fund_deals = [deal for deal in deals if (deal.fund_number or "Unknown Fund") == fund_name]
        fund_tail_nav = sum((deal.unrealized_value or 0.0) for deal in fund_deals if (metrics_by_id.get(deal.id) or {}).get("hold_period") is not None and (metrics_by_id.get(deal.id) or {}).get("hold_period") >= 5)
        fund_total_nav = sum(deal.unrealized_value or 0.0 for deal in fund_deals)
        milestone_rollup_source.extend(series_rows)
        if latest is None:
            fund_rows.append(
                {
                    "fund_number": fund_name,
                    "quarter_end": None,
                    "dpi_current": None,
                    "tvpi_current": None,
                    "rvpi_current": None,
                    "unrealized_dependency": None,
                    "tail_nav_pct": safe_divide(fund_tail_nav, fund_total_nav),
                    "aged_unrealized_count": sum(1 for deal in fund_deals if (deal.unrealized_value or 0.0) > 0 and (metrics_by_id.get(deal.id) or {}).get("hold_period") is not None and (metrics_by_id.get(deal.id) or {}).get("hold_period") >= 5),
                    "dpi_025_date": None,
                    "dpi_050_date": None,
                    "dpi_100_date": None,
                }
            )
            continue

        dpi = safe_divide(latest.distributed_capital, latest.paid_in_capital)
        tvpi = safe_divide((latest.distributed_capital or 0.0) + (latest.nav or 0.0), latest.paid_in_capital)
        rvpi = safe_divide(latest.nav, latest.paid_in_capital)
        fund_rows.append(
            {
                "fund_number": fund_name,
                "quarter_end": latest.quarter_end,
                "dpi_current": dpi,
                "tvpi_current": tvpi,
                "rvpi_current": rvpi,
                "unrealized_dependency": safe_divide(latest.nav, (latest.distributed_capital or 0.0) + (latest.nav or 0.0)),
                "tail_nav_pct": safe_divide(fund_tail_nav, latest.nav if latest.nav not in (None, 0) else fund_total_nav),
                "aged_unrealized_count": sum(1 for deal in fund_deals if (deal.unrealized_value or 0.0) > 0 and (metrics_by_id.get(deal.id) or {}).get("hold_period") is not None and (metrics_by_id.get(deal.id) or {}).get("hold_period") >= 5),
                "dpi_025_date": _milestone_date(series_rows, 0.25),
                "dpi_050_date": _milestone_date(series_rows, 0.50),
                "dpi_100_date": _milestone_date(series_rows, 1.00),
            }
        )

    milestone_rollup_source.sort(key=lambda row: row.quarter_end or date.min)
    quality_flags = []
    unrealized_dependency = safe_divide(nav, (distributed or 0.0) + (nav or 0.0))
    tail_nav_pct = safe_divide(tail_nav_total, nav if nav not in (None, 0) else total_nav_from_deals)
    if unrealized_dependency is not None and unrealized_dependency >= 0.60:
        quality_flags.append("Heavy unrealized value dependence in current TVPI.")
    if (valuation_quality.get("summary") or {}).get("avg_staleness_days") not in (None, 0) and (valuation_quality["summary"]["avg_staleness_days"] or 0) > 180:
        quality_flags.append("Valuation coverage shows stale unrealized marks.")
    if tail_nav_pct is not None and tail_nav_pct >= 0.40:
        quality_flags.append("Aged unrealized NAV concentration is elevated.")

    return {
        "meta": {
            "as_of_date": as_of_date or resolve_analysis_as_of_date(deals),
            "fund_count": len(fund_names),
            "has_quarter_data": bool(quarter_rows),
        },
        "kpis": {
            "dpi_current": safe_divide(distributed, paid_in),
            "tvpi_current": safe_divide((distributed or 0.0) + (nav or 0.0), paid_in),
            "rvpi_current": safe_divide(nav, paid_in),
            "unrealized_dependency": unrealized_dependency,
            "tail_nav_pct": tail_nav_pct,
            "aged_unrealized_count": len(aged_deals),
        },
        "milestones": {
            "dpi_025_date": _milestone_date(milestone_rollup_source, 0.25),
            "dpi_050_date": _milestone_date(milestone_rollup_source, 0.50),
            "dpi_100_date": _milestone_date(milestone_rollup_source, 1.00),
        },
        "aging": [
            {
                "label": "Aged Unrealized Deals (5y+)",
                "count": len(aged_deals),
                "nav": tail_nav_total,
            },
            {
                "label": "Total Unrealized NAV",
                "count": sum(1 for deal in deals if (deal.unrealized_value or 0.0) > 0),
                "nav": total_nav_from_deals,
            },
        ],
        "quality_flags": quality_flags,
        "fund_rows": fund_rows,
    }


def _fund_loss_ratio(fund_deals, metrics_by_id):
    invested_total = 0.0
    loss_total = 0.0
    for deal in fund_deals:
        metrics = metrics_by_id.get(deal.id) or {}
        invested = metrics.get("equity") or 0.0
        value_total = metrics.get("value_total") or 0.0
        invested_total += invested
        if invested > 0 and value_total < invested:
            loss_total += invested
    return safe_divide(loss_total, invested_total)


def _fund_realized_share(fund_deals):
    realized = sum(deal.realized_value or 0.0 for deal in fund_deals)
    total_value = realized + sum(deal.unrealized_value or 0.0 for deal in fund_deals)
    return safe_divide(realized, total_value)


def compute_manager_consistency_analysis(
    deals,
    team_id=None,
    firm_id=None,
    benchmark_asset_class="",
    metrics_by_id=None,
    as_of_date=None,
):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals}
    track_record = compute_deal_track_record(deals, metrics_by_id=metrics_by_id)
    fund_names = [fund.get("fund_name") or "Unknown Fund" for fund in track_record.get("funds", [])]
    fund_metadata = _metadata_by_fund(team_id, firm_id, fund_names)
    deals_by_fund = defaultdict(list)
    for deal in deals:
        deals_by_fund[deal.fund_number or "Unknown Fund"].append(deal)

    fund_rows = []
    rank_observations = []
    for fund in track_record.get("funds", []):
        fund_name = fund.get("fund_name") or "Unknown Fund"
        net = fund.get("net_performance") or {}
        metadata = fund_metadata.get(fund_name)
        vintage_year = _fund_vintage_year_from_lookup(fund_name, fund_metadata, deals_by_fund.get(fund_name, []))
        thresholds = load_team_benchmark_thresholds(
            team_id,
            benchmark_asset_class,
            strategy=getattr(metadata, "strategy", None),
            region=getattr(metadata, "region_focus", None),
            size_bucket=_fund_size_bucket(metadata),
        )

        quartile_history = {}
        delta_values = []
        top_half_hits = 0
        q4_hits = 0
        coverage = 0
        for metric in BENCHMARK_METRICS:
            metric_value = net.get(metric)
            rank = rank_benchmark_metric(metric_value, vintage_year, metric, thresholds, benchmark_asset_class)
            median_value = ((thresholds.get(vintage_year) or {}).get(metric) or {}).get("median")
            delta = metric_value - median_value if metric_value is not None and median_value is not None else None
            quartile_history[metric] = {
                "rank": rank,
                "median": median_value,
                "delta_to_median": delta,
            }
            if rank["rank_code"] != "na":
                coverage += 1
                rank_observations.append(rank["rank_code"])
                if rank["rank_code"] in TOP_HALF_RANKS:
                    top_half_hits += 1
                if rank["rank_code"] == "q4":
                    q4_hits += 1
            if delta is not None:
                delta_values.append(delta)

        fund_metric_values = {
            "net_irr": net.get("net_irr"),
            "net_moic": net.get("net_moic"),
            "net_dpi": net.get("net_dpi"),
        }
        fund_rows.append(
            {
                "fund_name": fund_name,
                "manager_name": _fund_manager_name(fund_name, fund_metadata),
                "benchmark_peer_group": getattr(metadata, "benchmark_peer_group", None),
                "strategy": getattr(metadata, "strategy", None),
                "region_focus": getattr(metadata, "region_focus", None),
                "vintage_year": vintage_year,
                "quartile_history": quartile_history,
                "median_delta_to_peer_median": median(delta_values) if delta_values else None,
                "top_half_rate": safe_divide(top_half_hits, coverage),
                "q4_rate": safe_divide(q4_hits, coverage),
                "realized_value_share": _fund_realized_share(deals_by_fund.get(fund_name, [])),
                "loss_ratio": _fund_loss_ratio(deals_by_fund.get(fund_name, []), metrics_by_id),
                "coverage_count": coverage,
                "metrics": fund_metric_values,
            }
        )

    grouped = defaultdict(list)
    for row in fund_rows:
        grouped[row["manager_name"]].append(row)

    manager_rows = []
    for manager_name, rows in sorted(grouped.items(), key=lambda item: item[0].lower()):
        irr_values = [r["metrics"]["net_irr"] for r in rows if r["metrics"].get("net_irr") is not None]
        moic_values = [r["metrics"]["net_moic"] for r in rows if r["metrics"].get("net_moic") is not None]
        dpi_values = [r["metrics"]["net_dpi"] for r in rows if r["metrics"].get("net_dpi") is not None]
        manager_rows.append(
            {
                "manager_name": manager_name,
                "fund_count": len(rows),
                "top_half_rate": safe_divide(sum((r.get("top_half_rate") or 0.0) for r in rows), len(rows)),
                "q4_rate": safe_divide(sum((r.get("q4_rate") or 0.0) for r in rows), len(rows)),
                "realized_value_share": safe_divide(sum((r.get("realized_value_share") or 0.0) for r in rows), len(rows)),
                "loss_ratio": safe_divide(sum((r.get("loss_ratio") or 0.0) for r in rows), len(rows)),
                "median_delta_to_peer_median": median([r["median_delta_to_peer_median"] for r in rows if r.get("median_delta_to_peer_median") is not None]) if any(r.get("median_delta_to_peer_median") is not None for r in rows) else None,
                "dispersion_net_irr": pstdev(irr_values) if len(irr_values) > 1 else None,
                "dispersion_net_moic": pstdev(moic_values) if len(moic_values) > 1 else None,
                "dispersion_net_dpi": pstdev(dpi_values) if len(dpi_values) > 1 else None,
            }
        )

    coverage_count = sum(1 for row in fund_rows if (row.get("coverage_count") or 0) > 0)
    full_coverage_count = sum(1 for row in fund_rows if row.get("coverage_count") == len(BENCHMARK_METRICS))
    return {
        "meta": {
            "as_of_date": as_of_date or resolve_analysis_as_of_date(deals),
            "benchmark_asset_class": benchmark_asset_class,
            "manager_count": len(manager_rows),
            "fund_count": len(fund_rows),
        },
        "kpis": {
            "manager_count": len(manager_rows),
            "fund_count": len(fund_rows),
            "top_half_rate": safe_divide(sum(1 for code in rank_observations if code in TOP_HALF_RANKS), len(rank_observations)),
            "q4_rate": safe_divide(sum(1 for code in rank_observations if code == "q4"), len(rank_observations)),
            "realized_value_share": safe_divide(sum((row.get("realized_value_share") or 0.0) for row in fund_rows), len(fund_rows)),
        },
        "manager_rows": manager_rows,
        "fund_rows": fund_rows,
        "benchmark_coverage": {
            "any_coverage_count": coverage_count,
            "full_coverage_count": full_coverage_count,
            "any_coverage_pct": safe_divide(coverage_count, len(fund_rows)),
            "full_coverage_pct": safe_divide(full_coverage_count, len(fund_rows)),
        },
    }


def _classify_cashflow(amount, event_type):
    normalized_type = (event_type or "").strip().lower()
    amt = float(amount or 0.0)
    if any(token in normalized_type for token in ("call", "contribution", "invest", "draw")):
        return "contribution", abs(amt)
    if any(token in normalized_type for token in ("distribution", "return", "proceed", "realization")):
        return "distribution", abs(amt)
    if amt < 0:
        return "contribution", abs(amt)
    return "distribution", abs(amt)


def _index_lookup(rows):
    by_code = defaultdict(list)
    for row in rows:
        by_code[row.benchmark_code].append(row)
    return {
        code: {
            "dates": [item.level_date for item in values],
            "levels": [item.level for item in values],
            "rows": values,
        }
        for code, values in by_code.items()
    }


def _level_on_or_before(index_series, target_date):
    dates = index_series["dates"]
    pos = bisect_right(dates, target_date) - 1
    if pos < 0:
        return None
    return index_series["levels"][pos]


def _benchmark_code_for_fund(fund_name, metadata_by_fund, benchmark_asset_class, available_codes):
    metadata = metadata_by_fund.get(fund_name)
    candidates = [
        getattr(metadata, "benchmark_peer_group", None) if metadata is not None else None,
        benchmark_asset_class,
    ]
    for candidate in candidates:
        if candidate and candidate in available_codes:
            return candidate
    return next(iter(sorted(available_codes)), None) if available_codes else None


def compute_public_market_comparison_analysis(deals, team_id=None, firm_id=None, benchmark_asset_class="", as_of_date=None):
    fund_names = _fund_names_from_deals(deals)
    metadata_by_fund = _metadata_by_fund(team_id, firm_id, fund_names)
    cashflow_rows = _fund_cashflow_rows(team_id, firm_id, fund_names)
    quarter_rows = _fund_quarter_rows(firm_id, fund_names)
    latest_quarter_by_fund = _latest_fund_rows_by_fund(quarter_rows)
    benchmark_codes = set()
    for metadata in metadata_by_fund.values():
        if metadata.benchmark_peer_group:
            benchmark_codes.add(metadata.benchmark_peer_group)
    if benchmark_asset_class:
        benchmark_codes.add(benchmark_asset_class)
    public_rows = _public_market_rows(team_id, benchmark_codes)
    index_by_code = _index_lookup(public_rows)

    cashflows_by_fund = defaultdict(list)
    for row in cashflow_rows:
        cashflows_by_fund[row.fund_number].append(row)

    fund_rows = []
    series = []
    coverage_counts = {"funds_with_complete_coverage": 0, "funds_with_partial_coverage": 0, "funds_with_no_coverage": 0}
    for fund_name in fund_names:
        benchmark_code = _benchmark_code_for_fund(fund_name, metadata_by_fund, benchmark_asset_class, set(index_by_code.keys()))
        index_series = index_by_code.get(benchmark_code)
        flows = cashflows_by_fund.get(fund_name, [])
        latest_nav_row = latest_quarter_by_fund.get(fund_name)
        if not flows or index_series is None:
            coverage_counts["funds_with_no_coverage"] += 1
            fund_rows.append(
                {
                    "fund_number": fund_name,
                    "benchmark_code": benchmark_code,
                    "ks_pme": None,
                    "direct_alpha": None,
                    "coverage": "insufficient",
                    "missing_benchmark_dates": True,
                    "nav_used": latest_nav_row.nav if latest_nav_row is not None else None,
                }
            )
            continue

        end_date = max(
            [flow.event_date for flow in flows if flow.event_date is not None]
            + ([latest_nav_row.quarter_end] if latest_nav_row is not None and latest_nav_row.quarter_end is not None else [])
        )
        terminal_level = _level_on_or_before(index_series, end_date)
        if terminal_level in (None, 0):
            coverage_counts["funds_with_no_coverage"] += 1
            fund_rows.append(
                {
                    "fund_number": fund_name,
                    "benchmark_code": benchmark_code,
                    "ks_pme": None,
                    "direct_alpha": None,
                    "coverage": "insufficient",
                    "missing_benchmark_dates": True,
                    "nav_used": latest_nav_row.nav if latest_nav_row is not None else None,
                }
            )
            continue

        contrib_fv = 0.0
        distrib_fv = 0.0
        missing_dates = 0
        event_series = []
        for flow in flows:
            level = _level_on_or_before(index_series, flow.event_date)
            if level in (None, 0):
                missing_dates += 1
                continue
            cashflow_type, absolute_amount = _classify_cashflow(flow.amount, flow.event_type)
            factor = terminal_level / level
            future_value = absolute_amount * factor
            event_series.append(
                {
                    "fund_number": fund_name,
                    "benchmark_code": benchmark_code,
                    "event_date": flow.event_date,
                    "event_type": flow.event_type,
                    "amount": flow.amount,
                    "future_value": future_value,
                    "index_level_used": level,
                }
            )
            if cashflow_type == "contribution":
                contrib_fv += future_value
            else:
                distrib_fv += future_value

        nav_value = 0.0
        if latest_nav_row is not None and latest_nav_row.nav is not None:
            nav_value = latest_nav_row.nav
        else:
            nav_candidates = [row.nav_after_event for row in flows if row.nav_after_event is not None]
            nav_value = nav_candidates[-1] if nav_candidates else 0.0

        ks_pme = safe_divide(distrib_fv + nav_value, contrib_fv)
        flow_dates = [flow.event_date for flow in flows if flow.event_date is not None]
        duration_years = ((end_date - min(flow_dates)).days / 365.25) if len(flow_dates) >= 2 else None
        direct_alpha = math.log(ks_pme) / duration_years if ks_pme not in (None, 0) and duration_years not in (None, 0) and ks_pme > 0 else None
        coverage_label = "complete" if missing_dates == 0 else "partial"
        if coverage_label == "complete":
            coverage_counts["funds_with_complete_coverage"] += 1
        else:
            coverage_counts["funds_with_partial_coverage"] += 1

        fund_rows.append(
            {
                "fund_number": fund_name,
                "benchmark_code": benchmark_code,
                "ks_pme": ks_pme,
                "direct_alpha": direct_alpha,
                "coverage": coverage_label,
                "missing_benchmark_dates": missing_dates > 0,
                "nav_used": nav_value,
                "event_count": len(flows),
            }
        )
        series.extend(event_series)

    benchmark_rows = []
    for benchmark_code, payload in sorted(index_by_code.items()):
        rows = payload["rows"]
        benchmark_rows.append(
            {
                "benchmark_code": benchmark_code,
                "series_start": rows[0].level_date if rows else None,
                "series_end": rows[-1].level_date if rows else None,
                "row_count": len(rows),
                "source": rows[-1].source if rows else None,
                "currency_code": rows[-1].currency_code if rows else None,
            }
        )

    return {
        "meta": {
            "as_of_date": as_of_date or resolve_analysis_as_of_date(deals),
            "benchmark_asset_class": benchmark_asset_class,
            "fund_count": len(fund_rows),
        },
        "coverage": coverage_counts,
        "fund_rows": fund_rows,
        "benchmark_rows": benchmark_rows,
        "series": series,
    }


def compute_lp_due_diligence_memo(
    deals,
    team_id=None,
    firm_id=None,
    benchmark_asset_class="",
    metrics_by_id=None,
    as_of_date=None,
):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals}
    fund_names = _fund_names_from_deals(deals)
    fund_metadata_lookup = _metadata_by_fund(team_id, firm_id, fund_names)
    liquidity = compute_lp_liquidity_quality_analysis(
        deals,
        firm_id=firm_id,
        team_id=team_id,
        as_of_date=as_of_date,
    )
    manager = compute_manager_consistency_analysis(
        deals,
        team_id=team_id,
        firm_id=firm_id,
        benchmark_asset_class=benchmark_asset_class,
        metrics_by_id=metrics_by_id,
        as_of_date=as_of_date,
    )
    public_market = compute_public_market_comparison_analysis(
        deals,
        team_id=team_id,
        firm_id=firm_id,
        benchmark_asset_class=benchmark_asset_class,
        as_of_date=as_of_date,
    )
    benchmark_summary = {
        "benchmark_asset_class": benchmark_asset_class,
        "fund_count": len(fund_names),
    }
    fund_metadata_rows = []
    for fund_name in fund_names:
        metadata = fund_metadata_lookup.get(fund_name)
        fund_metadata_rows.append(
            {
                "fund_number": fund_name,
                "vintage_year": getattr(metadata, "vintage_year", None),
                "strategy": getattr(metadata, "strategy", None),
                "region_focus": getattr(metadata, "region_focus", None),
                "fund_size": getattr(metadata, "fund_size", None),
                "manager_name": getattr(metadata, "manager_name", None),
                "benchmark_peer_group": getattr(metadata, "benchmark_peer_group", None),
                "status": getattr(metadata, "status", None),
            }
        )

    coverage_flags = []
    if public_market.get("coverage", {}).get("funds_with_no_coverage"):
        coverage_flags.append("Public market comparison has incomplete benchmark coverage.")
    if liquidity.get("quality_flags"):
        coverage_flags.extend(liquidity["quality_flags"])

    return {
        "meta": {
            "as_of_date": as_of_date or resolve_analysis_as_of_date(deals),
            "benchmark_asset_class": benchmark_asset_class,
            "methodology_version": "lp-ddq-v1",
            "peer_cohort": benchmark_asset_class or "Not Selected",
            "source_coverage": coverage_flags,
        },
        "fund_metadata": fund_metadata_rows,
        "benchmarking_summary": benchmark_summary,
        "liquidity_quality": liquidity,
        "manager_consistency": manager,
        "public_market_comparison": public_market,
        "data_coverage_flags": coverage_flags,
    }

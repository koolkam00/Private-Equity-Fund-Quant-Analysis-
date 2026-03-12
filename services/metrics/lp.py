"""LP-oriented analysis payloads built from fund metadata, fund cashflows, and benchmark series."""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import date
import math
from statistics import median, pstdev

from models import (
    BenchmarkPoint,
    DealQuarterSnapshot,
    DealUnderwriteBaseline,
    FundCashflow,
    FundMetadata,
    FundQuarterSnapshot,
    PublicMarketIndexLevel,
    UploadIssue,
)
from peqa.services.context import load_team_benchmark_thresholds
from peqa.services.metrics.status import normalize_realization_status
from peqa.services.filtering import build_fund_vintage_lookup, deal_vintage_year, sort_fund_rows_by_vintage
from services.metrics.analysis import compute_fund_liquidity_analysis, compute_valuation_quality_analysis
from services.metrics.benchmarking import rank_benchmark_metric
from services.metrics.common import resolve_analysis_as_of_date, safe_divide
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import compute_deal_track_record


BENCHMARK_METRICS = ("net_irr", "net_moic", "net_dpi")
TOP_HALF_RANKS = {"top5", "q1", "q2"}
REQUIRED_BENCHMARK_QUARTILES = ("lower_quartile", "median", "upper_quartile")


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


def _sort_lp_fund_rows(fund_rows, deals, team_id=None, firm_id=None, fund_metadata=None, fund_key_candidates=("fund_number", "fund_name")):
    vintage_lookup = build_fund_vintage_lookup(
        deals,
        team_id=team_id,
        firm_id=firm_id,
        fund_metadata=fund_metadata,
    )
    return sort_fund_rows_by_vintage(
        fund_rows,
        vintage_lookup=vintage_lookup,
        fund_key_candidates=fund_key_candidates,
    )


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
        "takeaway": {
            "text": "Liquidity quality is strongest when DPI is real, tail NAV is manageable, and marks are current. Elevated dependence on unrealized value should be treated as a diligence question, not just a portfolio feature.",
            "tone": "warning" if quality_flags else "positive",
            "why_it_matters": "LPs care about how much of value has converted to cash, how old the remaining NAV is, and whether marks are still credible.",
        },
        "coverage": {
            "items": [
                _coverage_item("Funds", len(fund_names)),
                _coverage_item("Quarter Data", len(latest_by_fund), "positive" if latest_by_fund else "warning"),
                _coverage_item("Aged Deals", len(aged_deals), "warning" if aged_deals else "neutral"),
                _coverage_item("Flags", len(quality_flags), "warning" if quality_flags else "neutral"),
            ]
        },
        "confidence": _confidence_payload(
            "high" if quarter_rows else "low",
            "Confidence is highest when current fund-quarter snapshots are available for all funds in scope.",
        ),
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
        "risk_flags": quality_flags,
        "fund_rows": _sort_lp_fund_rows(fund_rows, deals, team_id=team_id, firm_id=firm_id),
        "sections": ["kpis", "milestones", "fund_rows"],
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
    risk_flags = []
    if safe_divide(full_coverage_count, len(fund_rows)) is not None and safe_divide(full_coverage_count, len(fund_rows)) < 0.5:
        risk_flags.append("Less than half of funds have full benchmark coverage across net IRR, MOIC, and DPI.")
    if safe_divide(sum(1 for code in rank_observations if code == "q4"), len(rank_observations)) is not None and safe_divide(sum(1 for code in rank_observations if code == "q4"), len(rank_observations)) > 0.25:
        risk_flags.append("Quartile history shows a meaningful share of Q4 outcomes.")
    return {
        "meta": {
            "as_of_date": as_of_date or resolve_analysis_as_of_date(deals),
            "benchmark_asset_class": benchmark_asset_class,
            "manager_count": len(manager_rows),
            "fund_count": len(fund_rows),
        },
        "takeaway": {
            "text": "Manager consistency matters more than a single standout fund. Focus on repeat top-half behavior, realized value share, and dispersion across the full manager history.",
            "tone": "warning" if risk_flags else "positive",
            "why_it_matters": "LPs underwrite repeatability, not just the best-case fund in the manager track record.",
        },
        "coverage": {
            "items": [
                _coverage_item("Managers", len(manager_rows)),
                _coverage_item("Funds", len(fund_rows)),
                _coverage_item("Any Coverage", safe_divide(coverage_count, len(fund_rows))),
                _coverage_item("Full Coverage", safe_divide(full_coverage_count, len(fund_rows))),
            ]
        },
        "confidence": _confidence_payload(
            "high" if full_coverage_count == len(fund_rows) and fund_rows else ("medium" if coverage_count else "low"),
            "Confidence depends on benchmark completeness and the depth of manager history in the current filter scope.",
        ),
        "kpis": {
            "manager_count": len(manager_rows),
            "fund_count": len(fund_rows),
            "top_half_rate": safe_divide(sum(1 for code in rank_observations if code in TOP_HALF_RANKS), len(rank_observations)),
            "q4_rate": safe_divide(sum(1 for code in rank_observations if code == "q4"), len(rank_observations)),
            "realized_value_share": safe_divide(sum((row.get("realized_value_share") or 0.0) for row in fund_rows), len(fund_rows)),
        },
        "manager_rows": manager_rows,
        "fund_rows": _sort_lp_fund_rows(fund_rows, deals, team_id=team_id, firm_id=firm_id, fund_metadata=fund_metadata, fund_key_candidates=("fund_name",)),
        "benchmark_coverage": {
            "any_coverage_count": coverage_count,
            "full_coverage_count": full_coverage_count,
            "any_coverage_pct": safe_divide(coverage_count, len(fund_rows)),
            "full_coverage_pct": safe_divide(full_coverage_count, len(fund_rows)),
        },
        "risk_flags": risk_flags,
        "sections": ["kpis", "manager_rows", "fund_rows"],
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

    risk_flags = []
    if coverage_counts["funds_with_no_coverage"]:
        risk_flags.append("Some funds have no usable public market benchmark coverage.")
    if coverage_counts["funds_with_partial_coverage"]:
        risk_flags.append("Some PME calculations rely on previous-available benchmark levels rather than exact dates.")

    return {
        "meta": {
            "as_of_date": as_of_date or resolve_analysis_as_of_date(deals),
            "benchmark_asset_class": benchmark_asset_class,
            "fund_count": len(fund_rows),
        },
        "takeaway": {
            "text": "Public market comparison is most useful when cash flows are complete and benchmark dates line up cleanly. Treat partial coverage as informative, but not definitive.",
            "tone": "warning" if risk_flags else "positive",
            "why_it_matters": "PME and Direct Alpha help LPs compare private market outcomes against a transparent public benchmark with matching timing.",
        },
        "coverage": {
            **coverage_counts,
            "items": [
                _coverage_item("Complete", coverage_counts["funds_with_complete_coverage"], "positive"),
                _coverage_item("Partial", coverage_counts["funds_with_partial_coverage"], "warning"),
                _coverage_item("No Coverage", coverage_counts["funds_with_no_coverage"], "risk"),
                _coverage_item("Benchmarks", len(benchmark_rows)),
            ],
        },
        "confidence": _confidence_payload(
            "high" if coverage_counts["funds_with_no_coverage"] == 0 and fund_rows else ("medium" if coverage_counts["funds_with_complete_coverage"] else "low"),
            "Confidence depends on exact benchmark-date coverage and whether current NAV can be tied to the index series.",
        ),
        "fund_rows": _sort_lp_fund_rows(fund_rows, deals, team_id=team_id, firm_id=firm_id, fund_metadata=metadata_by_fund),
        "benchmark_rows": benchmark_rows,
        "series": series,
        "risk_flags": risk_flags,
        "sections": ["fund_rows", "benchmark_rows"],
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
    as_of = as_of_date or resolve_analysis_as_of_date(deals)
    fund_names = _fund_names_from_deals(deals)
    fund_metadata_lookup = _metadata_by_fund(team_id, firm_id, fund_names)

    fund_liquidity = compute_fund_liquidity_analysis(deals, firm_id=firm_id)
    nav_at_risk = compute_nav_at_risk_analysis(
        deals,
        firm_id=firm_id,
        team_id=team_id,
        metrics_by_id=metrics_by_id,
        as_of_date=as_of,
    )
    public_market = compute_public_market_comparison_analysis(
        deals,
        team_id=team_id,
        firm_id=firm_id,
        benchmark_asset_class=benchmark_asset_class,
        as_of_date=as_of,
    )

    benchmark_summary = {
        "benchmark_asset_class": benchmark_asset_class,
        "fund_count": len(fund_names),
        "pme_complete_funds": (public_market.get("coverage") or {}).get("funds_with_complete_coverage"),
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
    fund_metadata_rows = _sort_lp_fund_rows(
        fund_metadata_rows,
        deals,
        team_id=team_id,
        firm_id=firm_id,
        fund_metadata=fund_metadata_lookup,
    )

    coverage_flags = []
    if not fund_liquidity.get("has_data"):
        coverage_flags.append("Fund quarter snapshots are missing, so current liquidity metrics are incomplete.")
    for source in (nav_at_risk, public_market):
        coverage_flags.extend(source.get("risk_flags") or [])
    deduped_flags = []
    seen_flags = set()
    for flag in coverage_flags:
        if not flag or flag in seen_flags:
            continue
        seen_flags.add(flag)
        deduped_flags.append(flag)

    return {
        "meta": {
            "as_of_date": as_of,
            "benchmark_asset_class": benchmark_asset_class,
            "methodology_version": "lp-ddq-v2",
            "peer_cohort": benchmark_asset_class or "Not Selected",
            "source_coverage": deduped_flags,
        },
        "takeaway": {
            "text": (
                "LP diligence is strongest when current fund-liquidity trends, public market coverage, and NAV concentration signals align. "
                "Use this memo as the summary surface before drilling into individual analysis pages."
            ),
            "tone": "neutral",
            "why_it_matters": "This combines current fund liquidity, NAV concentration, and public market comparison into one briefing layer.",
        },
        "coverage": {
            "items": [
                _coverage_item("Funds", len(fund_names)),
                _coverage_item("Current DPI", (fund_liquidity.get("latest") or {}).get("dpi")),
                _coverage_item("Current TVPI", (fund_liquidity.get("latest") or {}).get("tvpi")),
                _coverage_item("PME Complete", public_market.get("coverage", {}).get("funds_with_complete_coverage")),
                _coverage_item("Top-10 NAV %", (nav_at_risk.get("summary") or {}).get("top_10_nav_pct")),
            ]
        },
        "confidence": _confidence_payload(
            "medium" if deduped_flags else "high",
            "Memo confidence combines fund liquidity coverage, public market coverage, and NAV concentration.",
        ),
        "fund_metadata": fund_metadata_rows,
        "benchmarking_summary": benchmark_summary,
        "fund_liquidity": fund_liquidity,
        "nav_at_risk": nav_at_risk,
        "public_market_comparison": public_market,
        "data_coverage_flags": deduped_flags,
        "risk_flags": deduped_flags,
    }


def _confidence_payload(level, note=None):
    normalized = (level or "medium").strip().lower()
    if normalized not in {"high", "medium", "low"}:
        normalized = "medium"
    labels = {
        "high": "High Confidence",
        "medium": "Moderate Confidence",
        "low": "Low Confidence",
    }
    tones = {
        "high": "positive",
        "medium": "warning",
        "low": "risk",
    }
    return {
        "level": normalized,
        "label": labels[normalized],
        "tone": tones[normalized],
        "note": note,
    }


def _coverage_item(label, value, tone="neutral"):
    return {
        "label": label,
        "value": value,
        "tone": tone,
    }


def _fund_track_record_lookup(track_record):
    lookup = {}
    for fund in track_record.get("funds", []):
        lookup[fund.get("fund_name") or "Unknown Fund"] = fund
    return lookup


def _deal_groups_by_fund(deals):
    grouped = defaultdict(list)
    for deal in deals:
        grouped[deal.fund_number or "Unknown Fund"].append(deal)
    return grouped


def _latest_deal_snapshots_by_id(deal_ids, firm_id=None):
    if not deal_ids:
        return {}
    query = DealQuarterSnapshot.query.filter(DealQuarterSnapshot.deal_id.in_(deal_ids))
    if firm_id is not None:
        query = query.filter(DealQuarterSnapshot.firm_id == firm_id)
    rows = query.order_by(DealQuarterSnapshot.deal_id.asc(), DealQuarterSnapshot.quarter_end.asc()).all()
    latest = {}
    for row in rows:
        latest[row.deal_id] = row
    return latest


def _underwrite_by_deal_id(deal_ids, firm_id=None):
    if not deal_ids:
        return {}
    query = DealUnderwriteBaseline.query.filter(DealUnderwriteBaseline.deal_id.in_(deal_ids))
    if firm_id is not None:
        query = query.filter(DealUnderwriteBaseline.firm_id == firm_id)
    rows = query.order_by(DealUnderwriteBaseline.deal_id.asc(), DealUnderwriteBaseline.id.asc()).all()
    result = {}
    for row in rows:
        result[row.deal_id] = row
    return result


def _quarter_end_from_date(value):
    if value is None:
        return None
    quarter = ((value.month - 1) // 3) + 1
    month = quarter * 3
    day = 31 if month in {3, 12} else 30
    return date(value.year, month, day)


def _next_quarter_end(value, offset=1):
    if value is None:
        return None
    base_quarter = ((value.month - 1) // 3) + 1
    encoded = (value.year * 4) + (base_quarter - 1) + int(offset)
    year = encoded // 4
    quarter = (encoded % 4) + 1
    month = quarter * 3
    day = 31 if month in {3, 12} else 30
    return date(year, month, day)


def _cashflow_quarter_buckets(rows):
    buckets = defaultdict(lambda: {"contributions": 0.0, "distributions": 0.0})
    for row in rows:
        quarter_end = _quarter_end_from_date(row.event_date)
        if quarter_end is None:
            continue
        flow_type, amount = _classify_cashflow(row.amount, row.event_type)
        buckets[quarter_end][f"{flow_type}s"] += amount
    return buckets


def _derived_cadence_from_quarters(rows):
    rows = sorted(rows, key=lambda row: row.quarter_end or date.min)
    if len(rows) < 2:
        return {"call_run_rate": 0.0, "distribution_run_rate": 0.0, "nav_delta_run_rate": 0.0}

    deltas = []
    for previous, current in zip(rows, rows[1:]):
        deltas.append(
            {
                "calls": max(0.0, (current.paid_in_capital or 0.0) - (previous.paid_in_capital or 0.0)),
                "distributions": max(0.0, (current.distributed_capital or 0.0) - (previous.distributed_capital or 0.0)),
                "nav_delta": (current.nav or 0.0) - (previous.nav or 0.0),
            }
        )
    return {
        "call_run_rate": safe_divide(sum(delta["calls"] for delta in deltas), len(deltas)) or 0.0,
        "distribution_run_rate": safe_divide(sum(delta["distributions"] for delta in deltas), len(deltas)) or 0.0,
        "nav_delta_run_rate": safe_divide(sum(delta["nav_delta"] for delta in deltas), len(deltas)) or 0.0,
    }


def _liquidity_forecast_confidence(quarter_count, event_count):
    if quarter_count >= 4 and event_count >= 4:
        return _confidence_payload("high", "Forecast uses both recent fund-quarter history and multiple dated cash flow events.")
    if quarter_count >= 2 or event_count >= 2:
        return _confidence_payload("medium", "Forecast uses limited recent history and should be treated as directional.")
    return _confidence_payload("low", "Forecast has sparse historical support and should be used only as a rough pacing signal.")


def _benchmark_lookup_for_asset(team_id, asset_class):
    asset = (asset_class or "").strip()
    if team_id is None or not asset:
        return {}
    rows = (
        BenchmarkPoint.query.filter(
            BenchmarkPoint.team_id == team_id,
            BenchmarkPoint.asset_class == asset,
        )
        .order_by(BenchmarkPoint.vintage_year.asc(), BenchmarkPoint.metric.asc(), BenchmarkPoint.quartile.asc())
        .all()
    )
    grouped = defaultdict(list)
    for row in rows:
        grouped[(int(row.vintage_year), row.metric)].append(row)
    return grouped


def _best_benchmark_match(rows, requested_dims):
    best_specificity = -1
    selected = []
    for row in rows:
        matched = True
        specificity = 0
        for key, requested_value in requested_dims.items():
            row_value = getattr(row, key, None)
            if row_value:
                if requested_value is None or row_value.strip().lower() != requested_value.strip().lower():
                    matched = False
                    break
                specificity += 1
        if not matched:
            continue
        if specificity > best_specificity:
            best_specificity = specificity
            selected = [row]
        elif specificity == best_specificity:
            selected.append(row)

    if not selected:
        return {
            "match_type": "no_match",
            "specificity": None,
            "quartile_coverage": "none",
            "quartiles": [],
        }

    quartiles = sorted({row.quartile for row in selected})
    quartile_coverage = "complete" if all(item in quartiles for item in REQUIRED_BENCHMARK_QUARTILES) else "partial"
    return {
        "match_type": "exact" if best_specificity == 3 else "wildcard",
        "specificity": best_specificity,
        "quartile_coverage": quartile_coverage,
        "quartiles": quartiles,
    }


def _issue_rows_for_scope(team_id, firm_id, limit=40):
    if team_id is None:
        return []
    query = UploadIssue.query.filter(UploadIssue.team_id == team_id)
    if firm_id is not None:
        query = query.filter(UploadIssue.firm_id == firm_id)
    rows = query.order_by(UploadIssue.created_at.desc(), UploadIssue.id.desc()).limit(limit).all()
    return [
        {
            "severity": row.severity,
            "message": row.message,
            "row_number": row.row_number,
            "company_name": row.company_name,
            "upload_batch": row.upload_batch,
            "created_at": row.created_at,
        }
        for row in rows
    ]


def compute_reporting_quality_analysis(
    deals,
    team_id=None,
    firm_id=None,
    benchmark_asset_class="",
    metrics_by_id=None,
    as_of_date=None,
):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals}
    as_of = as_of_date or resolve_analysis_as_of_date(deals)
    fund_names = _fund_names_from_deals(deals)
    deals_by_fund = _deal_groups_by_fund(deals)
    metadata_by_fund = _metadata_by_fund(team_id, firm_id, fund_names)
    fund_quarter_rows = _fund_quarter_rows(firm_id, fund_names)
    fund_cashflow_rows = _fund_cashflow_rows(team_id, firm_id, fund_names)
    latest_quarter_by_fund = _latest_fund_rows_by_fund(fund_quarter_rows)
    by_fund_quarters = defaultdict(list)
    for row in fund_quarter_rows:
        by_fund_quarters[row.fund_number].append(row)
    by_fund_cashflows = defaultdict(list)
    for row in fund_cashflow_rows:
        by_fund_cashflows[row.fund_number].append(row)

    deal_ids = [deal.id for deal in deals]
    latest_marks_by_deal = _latest_deal_snapshots_by_id(deal_ids, firm_id=firm_id)
    benchmark_confidence = compute_benchmark_confidence_analysis(
        deals,
        team_id=team_id,
        firm_id=firm_id,
        benchmark_asset_class=benchmark_asset_class,
        as_of_date=as_of,
    )
    benchmark_rows_by_fund = {
        row["fund_number"]: row for row in benchmark_confidence.get("fund_rows", [])
    }
    public_market = compute_public_market_comparison_analysis(
        deals,
        team_id=team_id,
        firm_id=firm_id,
        benchmark_asset_class=benchmark_asset_class,
        as_of_date=as_of,
    )
    public_market_by_fund = {
        row["fund_number"]: row for row in public_market.get("fund_rows", [])
    }
    track_record = compute_deal_track_record(deals, metrics_by_id=metrics_by_id)
    track_record_by_fund = _fund_track_record_lookup(track_record)

    fund_rows = []
    missing_counts = {
        "missing_metadata": 0,
        "missing_fund_quarters": 0,
        "missing_fund_cashflows": 0,
        "missing_net_metrics": 0,
        "stale_marks": 0,
    }
    decision_ready_count = 0
    stale_days_observations = []
    stale_nav_total = 0.0
    unrealized_nav_total = 0.0
    total_issue_severity = 0.0

    for fund_name in fund_names:
        fund_deals = deals_by_fund.get(fund_name, [])
        metadata = metadata_by_fund.get(fund_name)
        quarter_count = len(by_fund_quarters.get(fund_name, []))
        cashflow_count = len(by_fund_cashflows.get(fund_name, []))
        benchmark_row = benchmark_rows_by_fund.get(fund_name) or {}
        public_market_row = public_market_by_fund.get(fund_name) or {}
        track_fund = track_record_by_fund.get(fund_name) or {}
        net = track_fund.get("net_performance") or {}
        conflicts = net.get("conflicts") or {}
        net_metrics_complete = (
            net.get("net_irr") is not None
            and net.get("net_moic") is not None
            and net.get("net_dpi") is not None
            and not any(conflicts.values())
        )
        if not metadata:
            missing_counts["missing_metadata"] += 1
        if quarter_count == 0:
            missing_counts["missing_fund_quarters"] += 1
        if cashflow_count == 0:
            missing_counts["missing_fund_cashflows"] += 1
        if not net_metrics_complete:
            missing_counts["missing_net_metrics"] += 1

        stale_mark_count = 0
        stale_mark_nav = 0.0
        fund_issue_severity = 0.0
        for deal in fund_deals:
            if (deal.unrealized_value or 0.0) <= 0:
                continue
            unrealized_nav_total += deal.unrealized_value or 0.0
            latest_mark = latest_marks_by_deal.get(deal.id)
            if latest_mark is None or latest_mark.quarter_end is None:
                stale_mark_count += 1
                stale_mark_nav += deal.unrealized_value or 0.0
                continue
            age_days = (as_of - latest_mark.quarter_end).days if as_of and latest_mark.quarter_end else None
            if age_days is not None:
                stale_days_observations.append(age_days)
            if age_days is None or age_days > 180:
                stale_mark_count += 1
                stale_mark_nav += deal.unrealized_value or 0.0

        severity_score = 0.0
        severity_score += 15.0 if metadata is None else 0.0
        severity_score += 20.0 if quarter_count == 0 else 0.0
        severity_score += 20.0 if cashflow_count == 0 else 0.0
        severity_score += 15.0 if benchmark_row.get("match_quality") == "no_match" else 0.0
        severity_score += 10.0 if public_market_row.get("coverage") == "insufficient" else 0.0
        severity_score += 10.0 if not net_metrics_complete else 0.0
        severity_score += min(10.0, stale_mark_count * 3.0)
        if stale_mark_count:
            missing_counts["stale_marks"] += 1
        decision_ready = severity_score <= 25.0 and cashflow_count > 0 and quarter_count > 0
        if decision_ready:
            decision_ready_count += 1
        stale_nav_total += stale_mark_nav
        total_issue_severity += severity_score

        fund_rows.append(
            {
                "fund_number": fund_name,
                "has_metadata": metadata is not None,
                "fund_quarter_count": quarter_count,
                "fund_cashflow_count": cashflow_count,
                "latest_quarter_end": getattr(latest_quarter_by_fund.get(fund_name), "quarter_end", None),
                "benchmark_ready": benchmark_row.get("match_quality") not in {None, "no_match"},
                "pme_ready": public_market_row.get("coverage") in {"complete", "partial"},
                "complete_quartile_coverage": bool(benchmark_row.get("complete_quartile_coverage")),
                "net_metrics_complete": net_metrics_complete,
                "stale_mark_count": stale_mark_count,
                "stale_mark_nav": stale_mark_nav,
                "severity_score": severity_score,
                "decision_ready": decision_ready,
            }
        )

    funds_in_scope = len(fund_names)
    avg_severity_score = safe_divide(total_issue_severity, funds_in_scope)
    stale_mark_nav_pct = safe_divide(stale_nav_total, unrealized_nav_total)
    issue_rows = _issue_rows_for_scope(team_id, firm_id, limit=30)
    risk_flags = []
    if safe_divide(missing_counts["missing_fund_cashflows"], funds_in_scope) and safe_divide(missing_counts["missing_fund_cashflows"], funds_in_scope) >= 0.5:
        risk_flags.append("Half or more funds are missing dated fund cash flow history.")
    if stale_mark_nav_pct is not None and stale_mark_nav_pct >= 0.25:
        risk_flags.append("A material share of unrealized NAV relies on stale or missing quarter marks.")
    if safe_divide(decision_ready_count, funds_in_scope) is not None and safe_divide(decision_ready_count, funds_in_scope) < 0.5:
        risk_flags.append("Less than half of funds in scope are decision-ready for LP diligence.")

    return {
        "meta": {
            "as_of_date": as_of,
            "fund_count": funds_in_scope,
            "benchmark_asset_class": benchmark_asset_class,
        },
        "takeaway": {
            "text": (
                "Most downstream LP analyses are only as strong as the reporting package. "
                "Use this page to confirm whether the current dataset is decision-ready before leaning on the benchmark or PME outputs."
            ),
            "tone": "warning" if risk_flags else "positive",
            "why_it_matters": "LP diligence depends as much on reporting completeness and freshness as it does on the performance numbers themselves.",
        },
        "coverage": {
            "items": [
                _coverage_item("Decision-Ready Funds", decision_ready_count, "positive" if decision_ready_count else "warning"),
                _coverage_item("Benchmarkable", benchmark_confidence.get("summary", {}).get("funds_with_any_quartile_coverage")),
                _coverage_item("PME-Ready", public_market.get("coverage", {}).get("funds_with_complete_coverage")),
                _coverage_item("Recent Issues", len(issue_rows), "warning" if issue_rows else "neutral"),
            ]
        },
        "confidence": _confidence_payload(
            "high" if not risk_flags and funds_in_scope else ("medium" if funds_in_scope else "low"),
            "Confidence is based on completeness of fund-quarter data, fund cash flows, benchmark coverage, and mark freshness.",
        ),
        "coverage_summary": {
            "fund_count": funds_in_scope,
            "decision_ready_count": decision_ready_count,
            "decision_ready_pct": safe_divide(decision_ready_count, funds_in_scope),
            "avg_severity_score": avg_severity_score,
        },
        "missingness": missing_counts,
        "freshness": {
            "avg_staleness_days": safe_divide(sum(stale_days_observations), len(stale_days_observations)),
            "stale_mark_nav_pct": stale_mark_nav_pct,
            "funds_with_stale_marks": missing_counts["stale_marks"],
        },
        "benchmark_coverage": benchmark_confidence.get("summary") or {},
        "fund_rows": _sort_lp_fund_rows(fund_rows, deals, team_id=team_id, firm_id=firm_id, fund_metadata=metadata_by_fund),
        "issue_rows": issue_rows,
        "risk_flags": risk_flags,
        "sections": ["coverage", "freshness", "fund_rows", "issue_rows"],
    }


def compute_nav_at_risk_analysis(
    deals,
    firm_id=None,
    team_id=None,
    metrics_by_id=None,
    as_of_date=None,
):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals}
    as_of = as_of_date or resolve_analysis_as_of_date(deals)
    deal_ids = [deal.id for deal in deals]
    latest_marks_by_deal = _latest_deal_snapshots_by_id(deal_ids, firm_id=firm_id)
    underwrite_by_deal = _underwrite_by_deal_id(deal_ids, firm_id=firm_id)
    deals_by_fund = _deal_groups_by_fund(deals)

    unrealized_rows = []
    total_nav = 0.0
    stale_nav = 0.0
    aged_nav = 0.0
    below_plan_nav = 0.0
    sector_nav = defaultdict(float)
    vintage_nav = defaultdict(float)

    for deal in deals:
        nav = deal.unrealized_value or 0.0
        if nav <= 0:
            continue
        total_nav += nav
        metrics = metrics_by_id.get(deal.id) or {}
        hold_years = metrics.get("hold_period")
        latest_mark = latest_marks_by_deal.get(deal.id)
        latest_mark_date = latest_mark.quarter_end if latest_mark is not None else None
        staleness_days = (as_of - latest_mark_date).days if latest_mark_date else None
        is_stale = staleness_days is None or staleness_days > 180
        if is_stale:
            stale_nav += nav
        is_aged = hold_years is not None and hold_years >= 5
        if is_aged:
            aged_nav += nav

        baseline = underwrite_by_deal.get(deal.id)
        current_moic = metrics.get("moic")
        below_plan = (
            baseline is not None
            and getattr(baseline, "target_moic", None) is not None
            and current_moic is not None
            and current_moic < baseline.target_moic
        )
        if below_plan:
            below_plan_nav += nav

        entry_multiple = metrics.get("entry_tev_ebitda")
        current_multiple = metrics.get("exit_tev_ebitda")
        multiple_compression = (
            entry_multiple is not None
            and current_multiple is not None
            and current_multiple < entry_multiple
        )

        sector_nav[deal.sector or "Unknown"] += nav
        vintage_nav[str(deal_vintage_year(deal) or "Unknown")] += nav
        unrealized_rows.append(
            {
                "company_name": deal.company_name,
                "fund_number": deal.fund_number or "Unknown Fund",
                "sector": deal.sector or "Unknown",
                "vintage_year": deal_vintage_year(deal),
                "unrealized_value": nav,
                "hold_years": hold_years,
                "latest_mark_date": latest_mark_date,
                "staleness_days": staleness_days,
                "current_moic": current_moic,
                "target_moic": getattr(baseline, "target_moic", None),
                "below_plan": below_plan,
                "multiple_compression": multiple_compression,
                "risk_score": sum(
                    [
                        1 if is_aged else 0,
                        1 if is_stale else 0,
                        1 if below_plan else 0,
                        1 if multiple_compression else 0,
                    ]
                ),
            }
        )

    unrealized_rows.sort(key=lambda row: ((row.get("unrealized_value") or 0.0) * -1, row["company_name"].lower()))
    top_10_nav_pct = safe_divide(
        sum(row.get("unrealized_value") or 0.0 for row in unrealized_rows[:10]),
        total_nav,
    )

    aging_buckets = [
        {"label": "0-3 Years", "count": 0, "nav": 0.0},
        {"label": "3-5 Years", "count": 0, "nav": 0.0},
        {"label": "5-7 Years", "count": 0, "nav": 0.0},
        {"label": "7+ Years", "count": 0, "nav": 0.0},
    ]
    for row in unrealized_rows:
        hold_years = row.get("hold_years")
        nav = row.get("unrealized_value") or 0.0
        if hold_years is None or hold_years < 3:
            bucket = aging_buckets[0]
        elif hold_years < 5:
            bucket = aging_buckets[1]
        elif hold_years < 7:
            bucket = aging_buckets[2]
        else:
            bucket = aging_buckets[3]
        bucket["count"] += 1
        bucket["nav"] += nav

    fund_rows = []
    for fund_name, fund_deals in sorted(deals_by_fund.items(), key=lambda item: item[0].lower()):
        fund_unrealized = [row for row in unrealized_rows if row["fund_number"] == fund_name]
        fund_nav = sum(row.get("unrealized_value") or 0.0 for row in fund_unrealized)
        if fund_nav <= 0:
            continue
        fund_rows.append(
            {
                "fund_number": fund_name,
                "nav": fund_nav,
                "aged_nav_pct": safe_divide(sum(row.get("unrealized_value") or 0.0 for row in fund_unrealized if (row.get("hold_years") or 0.0) >= 5), fund_nav),
                "stale_nav_pct": safe_divide(sum(row.get("unrealized_value") or 0.0 for row in fund_unrealized if row.get("staleness_days") in (None,) or (row.get("staleness_days") or 0) > 180), fund_nav),
                "below_plan_nav_pct": safe_divide(sum(row.get("unrealized_value") or 0.0 for row in fund_unrealized if row.get("below_plan")), fund_nav),
                "deal_count": len(fund_unrealized),
            }
        )

    risk_flags = []
    if top_10_nav_pct is not None and top_10_nav_pct >= 0.60:
        risk_flags.append("Top-10 unrealized positions make up more than 60% of current NAV.")
    if safe_divide(aged_nav, total_nav) is not None and safe_divide(aged_nav, total_nav) >= 0.40:
        risk_flags.append("Aged unrealized deals represent more than 40% of current NAV.")
    if safe_divide(stale_nav, total_nav) is not None and safe_divide(stale_nav, total_nav) >= 0.25:
        risk_flags.append("More than one-quarter of NAV relies on stale quarter marks.")

    return {
        "meta": {
            "as_of_date": as_of,
            "deal_count": len(unrealized_rows),
        },
        "takeaway": {
            "text": "NAV at risk is driven by concentration, aged unrealized exposure, and stale-mark reliance. Focus first on where those conditions overlap.",
            "tone": "warning" if risk_flags else "positive",
            "why_it_matters": "LP downside is often concentrated in a small set of unrealized assets rather than spread evenly across the portfolio.",
        },
        "coverage": {
            "items": [
                _coverage_item("Unrealized Deals", len(unrealized_rows)),
                _coverage_item("Top-10 NAV %", top_10_nav_pct, "warning" if top_10_nav_pct and top_10_nav_pct >= 0.60 else "neutral"),
                _coverage_item("Aged NAV %", safe_divide(aged_nav, total_nav), "warning" if safe_divide(aged_nav, total_nav) and safe_divide(aged_nav, total_nav) >= 0.40 else "neutral"),
                _coverage_item("Stale NAV %", safe_divide(stale_nav, total_nav), "risk" if safe_divide(stale_nav, total_nav) and safe_divide(stale_nav, total_nav) >= 0.25 else "neutral"),
            ]
        },
        "confidence": _confidence_payload(
            "high" if any(row.get("latest_mark_date") for row in unrealized_rows) else "medium",
            "Confidence reflects availability of quarter marks and underwrite benchmarks for unrealized positions.",
        ),
        "summary": {
            "total_nav": total_nav,
            "top_10_nav_pct": top_10_nav_pct,
            "aged_nav_pct": safe_divide(aged_nav, total_nav),
            "stale_nav_pct": safe_divide(stale_nav, total_nav),
            "below_plan_nav_pct": safe_divide(below_plan_nav, total_nav),
        },
        "concentration": {
            "by_sector": sorted(
                [{"group": key, "nav": value, "nav_pct": safe_divide(value, total_nav)} for key, value in sector_nav.items()],
                key=lambda row: (row["nav"] or 0.0) * -1,
            ),
            "by_vintage": sorted(
                [{"group": key, "nav": value, "nav_pct": safe_divide(value, total_nav)} for key, value in vintage_nav.items()],
                key=lambda row: (row["nav"] or 0.0) * -1,
            ),
        },
        "aging_buckets": aging_buckets,
        "risk_flags": risk_flags,
        "deal_rows": unrealized_rows,
        "fund_rows": _sort_lp_fund_rows(fund_rows, deals, team_id=team_id, firm_id=firm_id),
        "sections": ["summary", "aging_buckets", "deal_rows", "fund_rows"],
    }


def compute_benchmark_confidence_analysis(
    deals,
    team_id=None,
    firm_id=None,
    benchmark_asset_class="",
    as_of_date=None,
):
    as_of = as_of_date or resolve_analysis_as_of_date(deals)
    fund_names = _fund_names_from_deals(deals)
    deals_by_fund = _deal_groups_by_fund(deals)
    metadata_by_fund = _metadata_by_fund(team_id, firm_id, fund_names)
    benchmark_lookup = _benchmark_lookup_for_asset(team_id, benchmark_asset_class)
    public_market = compute_public_market_comparison_analysis(
        deals,
        team_id=team_id,
        firm_id=firm_id,
        benchmark_asset_class=benchmark_asset_class,
        as_of_date=as_of,
    )
    public_market_by_fund = {row["fund_number"]: row for row in public_market.get("fund_rows", [])}

    exact_count = 0
    wildcard_count = 0
    no_match_count = 0
    partial_coverage_count = 0
    funds_with_complete_quartile_coverage = 0
    funds_with_any_quartile_coverage = 0
    benchmark_gaps = []
    fund_rows = []

    for fund_name in fund_names:
        metadata = metadata_by_fund.get(fund_name)
        vintage_year = _fund_vintage_year_from_lookup(fund_name, metadata_by_fund, deals_by_fund.get(fund_name, []))
        requested_dims = {
            "strategy": (getattr(metadata, "strategy", None) or "").strip().lower() or None,
            "region": (getattr(metadata, "region_focus", None) or "").strip().lower() or None,
            "size_bucket": (_fund_size_bucket(metadata) or "").strip().lower() or None,
        }
        metric_rows = {}
        exact_metric_count = 0
        wildcard_metric_count = 0
        no_match_metric_count = 0
        partial_metric_count = 0
        complete_metric_count = 0

        for metric in BENCHMARK_METRICS:
            match = _best_benchmark_match(benchmark_lookup.get((vintage_year, metric), []), requested_dims)
            metric_rows[metric] = match
            if match["match_type"] == "exact":
                exact_metric_count += 1
                exact_count += 1
            elif match["match_type"] == "wildcard":
                wildcard_metric_count += 1
                wildcard_count += 1
            else:
                no_match_metric_count += 1
                no_match_count += 1

            if match["quartile_coverage"] == "partial":
                partial_metric_count += 1
                partial_coverage_count += 1
            if match["quartile_coverage"] == "complete":
                complete_metric_count += 1

            if match["match_type"] == "no_match" or match["quartile_coverage"] == "partial":
                benchmark_gaps.append(
                    {
                        "fund_number": fund_name,
                        "metric": metric,
                        "match_quality": match["match_type"],
                        "quartile_coverage": match["quartile_coverage"],
                    }
                )

        complete_quartile_coverage = complete_metric_count == len(BENCHMARK_METRICS)
        if complete_quartile_coverage:
            funds_with_complete_quartile_coverage += 1
        if exact_metric_count or wildcard_metric_count:
            funds_with_any_quartile_coverage += 1

        pme_row = public_market_by_fund.get(fund_name) or {}
        match_quality = "no_match"
        if no_match_metric_count < len(BENCHMARK_METRICS):
            match_quality = "exact" if exact_metric_count == len(BENCHMARK_METRICS) else ("wildcard" if wildcard_metric_count else "mixed")
        fund_rows.append(
            {
                "fund_number": fund_name,
                "vintage_year": vintage_year,
                "strategy": getattr(metadata, "strategy", None),
                "region_focus": getattr(metadata, "region_focus", None),
                "size_bucket": _fund_size_bucket(metadata),
                "exact_match_count": exact_metric_count,
                "wildcard_match_count": wildcard_metric_count,
                "no_match_count": no_match_metric_count,
                "partial_coverage_count": partial_metric_count,
                "complete_quartile_coverage": complete_quartile_coverage,
                "match_quality": match_quality,
                "pme_series_coverage": pme_row.get("coverage"),
                "metrics": metric_rows,
            }
        )

    total_metric_observations = len(fund_names) * len(BENCHMARK_METRICS)
    pme_complete_count = sum(1 for row in public_market.get("fund_rows", []) if row.get("coverage") == "complete")
    risk_flags = []
    if safe_divide(no_match_count, total_metric_observations) is not None and safe_divide(no_match_count, total_metric_observations) >= 0.34:
        risk_flags.append("A third or more benchmark observations have no quartile match.")
    if partial_coverage_count:
        risk_flags.append("Some funds only have partial quartile coverage even when a benchmark match exists.")

    return {
        "meta": {
            "as_of_date": as_of,
            "benchmark_asset_class": benchmark_asset_class,
            "fund_count": len(fund_names),
        },
        "takeaway": {
            "text": "Benchmark rankings are only as credible as the match logic behind them. Exact vintage and dimensional matches should carry more weight than wildcard matches.",
            "tone": "warning" if risk_flags else "positive",
            "why_it_matters": "This separates true peer comparison from cases where the benchmark result is directionally useful but structurally weak.",
        },
        "coverage": {
            "items": [
                _coverage_item("Exact Match %", safe_divide(exact_count, total_metric_observations), "positive"),
                _coverage_item("Wildcard Match %", safe_divide(wildcard_count, total_metric_observations), "warning"),
                _coverage_item("No Match %", safe_divide(no_match_count, total_metric_observations), "risk"),
                _coverage_item("PME Complete", pme_complete_count),
            ]
        },
        "confidence": _confidence_payload(
            "high" if not risk_flags and total_metric_observations else ("medium" if total_metric_observations else "low"),
            "Confidence improves when quartile thresholds match on strategy, region, size bucket, and vintage year.",
        ),
        "summary": {
            "exact_match_pct": safe_divide(exact_count, total_metric_observations),
            "wildcard_match_pct": safe_divide(wildcard_count, total_metric_observations),
            "no_match_pct": safe_divide(no_match_count, total_metric_observations),
            "metrics_with_partial_coverage": partial_coverage_count,
            "funds_with_complete_quartile_coverage": funds_with_complete_quartile_coverage,
            "funds_with_any_quartile_coverage": funds_with_any_quartile_coverage,
            "funds_with_complete_pme_series": pme_complete_count,
        },
        "match_quality": {
            "exact_count": exact_count,
            "wildcard_count": wildcard_count,
            "no_match_count": no_match_count,
        },
        "fund_rows": _sort_lp_fund_rows(fund_rows, deals, team_id=team_id, firm_id=firm_id, fund_metadata=metadata_by_fund),
        "benchmark_gaps": benchmark_gaps,
        "risk_flags": risk_flags,
        "sections": ["summary", "fund_rows", "benchmark_gaps"],
    }


def compute_liquidity_forecast_analysis(
    deals,
    team_id=None,
    firm_id=None,
    as_of_date=None,
):
    as_of = as_of_date or resolve_analysis_as_of_date(deals)
    fund_names = _fund_names_from_deals(deals)
    quarter_rows = _fund_quarter_rows(firm_id, fund_names)
    cashflow_rows = _fund_cashflow_rows(team_id, firm_id, fund_names)
    by_fund_quarters = defaultdict(list)
    by_fund_cashflows = defaultdict(list)
    for row in quarter_rows:
        by_fund_quarters[row.fund_number].append(row)
    for row in cashflow_rows:
        by_fund_cashflows[row.fund_number].append(row)

    fund_rows = []
    capital_call_series = []
    distribution_series = []
    nav_burnoff_series = []
    reserve_flags = []
    negative_outlook_count = 0
    estimated_12m_net_cashflow = 0.0
    total_projected_dpi = []
    confidence_levels = []

    for fund_name in fund_names:
        fund_quarters = sorted(by_fund_quarters.get(fund_name, []), key=lambda row: row.quarter_end or date.min)
        latest = fund_quarters[-1] if fund_quarters else None
        fund_cashflows = by_fund_cashflows.get(fund_name, [])
        cashflow_buckets = _cashflow_quarter_buckets(fund_cashflows)
        trailing_quarters = sorted(cashflow_buckets.keys())[-4:]
        call_run_rate = safe_divide(sum(cashflow_buckets[q]["contributions"] for q in trailing_quarters), len(trailing_quarters)) if trailing_quarters else 0.0
        distribution_run_rate = safe_divide(sum(cashflow_buckets[q]["distributions"] for q in trailing_quarters), len(trailing_quarters)) if trailing_quarters else 0.0
        nav_delta_run_rate = 0.0
        if not trailing_quarters:
            cadence = _derived_cadence_from_quarters(fund_quarters[-4:])
            call_run_rate = cadence["call_run_rate"]
            distribution_run_rate = cadence["distribution_run_rate"]
            nav_delta_run_rate = cadence["nav_delta_run_rate"]

        base_nav = latest.nav if latest is not None and latest.nav is not None else sum(deal.unrealized_value or 0.0 for deal in deals if (deal.fund_number or "Unknown Fund") == fund_name)
        paid_in = latest.paid_in_capital if latest is not None and latest.paid_in_capital is not None else sum(abs(row.amount or 0.0) for row in fund_cashflows if _classify_cashflow(row.amount, row.event_type)[0] == "contribution")
        distributed = latest.distributed_capital if latest is not None and latest.distributed_capital is not None else sum(abs(row.amount or 0.0) for row in fund_cashflows if _classify_cashflow(row.amount, row.event_type)[0] == "distribution")
        unfunded = latest.unfunded_commitment if latest is not None and latest.unfunded_commitment is not None else 0.0
        forecast_start = latest.quarter_end if latest is not None and latest.quarter_end is not None else _quarter_end_from_date(as_of or date.today())

        confidence = _liquidity_forecast_confidence(len(fund_quarters), len(fund_cashflows))
        confidence_levels.append(confidence["level"])

        projected_paid_in = paid_in or 0.0
        projected_distributed = distributed or 0.0
        projected_nav = base_nav or 0.0
        remaining_unfunded = unfunded or 0.0
        projected_calls = 0.0
        projected_distributions = 0.0

        for offset, decay in enumerate((1.0, 0.85, 0.70, 0.55), start=1):
            quarter_end = _next_quarter_end(forecast_start, offset)
            projected_call = min(remaining_unfunded, (call_run_rate or 0.0) * decay)
            projected_distribution = max(0.0, (distribution_run_rate or 0.0) * (1.0 + ((offset - 1) * 0.05)))
            projected_nav = max(0.0, projected_nav + nav_delta_run_rate + projected_call - projected_distribution)
            projected_paid_in += projected_call
            projected_distributed += projected_distribution
            remaining_unfunded = max(0.0, remaining_unfunded - projected_call)
            projected_calls += projected_call
            projected_distributions += projected_distribution

            capital_call_series.append({"fund_number": fund_name, "quarter_end": quarter_end, "amount": projected_call})
            distribution_series.append({"fund_number": fund_name, "quarter_end": quarter_end, "amount": projected_distribution})
            nav_burnoff_series.append({"fund_number": fund_name, "quarter_end": quarter_end, "nav": projected_nav})

        projected_dpi = safe_divide(projected_distributed, projected_paid_in)
        total_projected_dpi.append(projected_dpi)
        net_cashflow_12m = projected_distributions - projected_calls
        estimated_12m_net_cashflow += net_cashflow_12m
        reserve_coverage_quarters = safe_divide(unfunded, call_run_rate) if call_run_rate not in (None, 0) else None
        if net_cashflow_12m < 0:
            negative_outlook_count += 1
        if reserve_coverage_quarters is not None and reserve_coverage_quarters < 2:
            reserve_flags.append(f"{fund_name}: reserve coverage is below two quarters of recent call pace.")
        if net_cashflow_12m < 0:
            reserve_flags.append(f"{fund_name}: projected 12-month net cash flow remains negative.")

        variability = {"high": 0.05, "medium": 0.10, "low": 0.20}[confidence["level"]]
        fund_rows.append(
            {
                "fund_number": fund_name,
                "confidence": confidence,
                "latest_quarter_end": getattr(latest, "quarter_end", None),
                "estimated_12m_net_cashflow": net_cashflow_12m,
                "projected_dpi": projected_dpi,
                "projected_dpi_low": projected_dpi * (1 - variability) if projected_dpi is not None else None,
                "projected_dpi_high": projected_dpi * (1 + variability) if projected_dpi is not None else None,
                "projected_call_total": projected_calls,
                "projected_distribution_total": projected_distributions,
                "projected_nav_end": projected_nav,
                "reserve_coverage_quarters": reserve_coverage_quarters,
                "negative_outlook": net_cashflow_12m < 0,
            }
        )

    dominant_confidence = "low"
    if confidence_levels:
        if confidence_levels.count("high") >= max(confidence_levels.count("medium"), confidence_levels.count("low")):
            dominant_confidence = "high"
        elif confidence_levels.count("medium") >= confidence_levels.count("low"):
            dominant_confidence = "medium"

    risk_flags = []
    if negative_outlook_count:
        risk_flags.append("At least one fund remains net cash flow negative on a 12-month view.")
    if reserve_flags:
        risk_flags.append("Reserve coverage flags are present in the current scope.")

    dpi_values = [value for value in total_projected_dpi if value is not None]
    avg_projected_dpi = safe_divide(sum(dpi_values), len(dpi_values)) if dpi_values else None
    band = {"high": 0.05, "medium": 0.10, "low": 0.20}[dominant_confidence]
    return {
        "meta": {
            "as_of_date": as_of,
            "fund_count": len(fund_rows),
            "forecast_horizon_quarters": 4,
        },
        "takeaway": {
            "text": "Liquidity forecasting should be treated as a pacing tool, not a promise. The most useful signal is whether calls, distributions, and reserve needs are moving in a supportive direction.",
            "tone": "warning" if risk_flags else "positive",
            "why_it_matters": "LPs care about near-term cash flow quality, not just current multiples and quartile snapshots.",
        },
        "coverage": {
            "items": [
                _coverage_item("Funds", len(fund_rows)),
                _coverage_item("Negative Outlook", negative_outlook_count, "warning" if negative_outlook_count else "neutral"),
                _coverage_item("Reserve Flags", len(reserve_flags), "warning" if reserve_flags else "neutral"),
                _coverage_item("Forecast Horizon", 4),
            ]
        },
        "confidence": _confidence_payload(
            dominant_confidence,
            "Forecast confidence is based on the depth of recent quarter history and dated fund cash flow events.",
        ),
        "forecast_summary": {
            "estimated_12m_net_cashflow": estimated_12m_net_cashflow,
            "projected_dpi_low": avg_projected_dpi * (1 - band) if avg_projected_dpi is not None else None,
            "projected_dpi_high": avg_projected_dpi * (1 + band) if avg_projected_dpi is not None else None,
            "negative_outlook_count": negative_outlook_count,
            "reserve_flag_count": len(reserve_flags),
        },
        "capital_call_series": capital_call_series,
        "distribution_series": distribution_series,
        "nav_burnoff_series": nav_burnoff_series,
        "reserve_flags": reserve_flags,
        "fund_rows": _sort_lp_fund_rows(fund_rows, deals, team_id=team_id, firm_id=firm_id),
        "risk_flags": risk_flags,
        "sections": ["forecast_summary", "fund_rows", "reserve_flags"],
    }


def compute_fee_drag_analysis(
    deals,
    team_id=None,
    firm_id=None,
    metrics_by_id=None,
    as_of_date=None,
):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals}
    as_of = as_of_date or resolve_analysis_as_of_date(deals)
    track_record = compute_deal_track_record(deals, metrics_by_id=metrics_by_id)

    fund_rows = []
    moic_deltas = []
    irr_deltas = []
    implied_value_drag_total = 0.0
    covered_funds = 0
    for fund in track_record.get("funds", []):
        fund_name = fund.get("fund_name") or "Unknown Fund"
        gross = fund.get("totals") or {}
        net = fund.get("net_performance") or {}
        conflicts = net.get("conflicts") or {}
        gross_moic = gross.get("gross_moic")
        gross_irr = gross.get("gross_irr")
        net_moic = None if conflicts.get("net_moic") else net.get("net_moic")
        net_irr = None if conflicts.get("net_irr") else net.get("net_irr")
        invested_equity = gross.get("invested_equity")
        moic_delta = (gross_moic - net_moic) if gross_moic is not None and net_moic is not None else None
        irr_delta = (gross_irr - net_irr) if gross_irr is not None and net_irr is not None else None
        implied_value_drag = (invested_equity * moic_delta) if invested_equity is not None and moic_delta is not None else None
        if moic_delta is not None or irr_delta is not None:
            covered_funds += 1
        if moic_delta is not None:
            moic_deltas.append(moic_delta)
        if irr_delta is not None:
            irr_deltas.append(irr_delta)
        if implied_value_drag is not None:
            implied_value_drag_total += implied_value_drag

        fund_rows.append(
            {
                "fund_number": fund_name,
                "gross_moic": gross_moic,
                "net_moic": net_moic,
                "gross_irr": gross_irr,
                "net_irr": net_irr,
                "gross_to_net_moic_delta": moic_delta,
                "gross_to_net_irr_delta": irr_delta,
                "implied_value_drag": implied_value_drag,
                "management_fee_load": None,
                "total_expense_ratio": None,
                "financing_drag": None,
                "detail_source": "fund net metrics" if moic_delta is not None or irr_delta is not None else "not available",
            }
        )

    avg_moic_delta = safe_divide(sum(moic_deltas), len(moic_deltas)) if moic_deltas else None
    avg_irr_delta = safe_divide(sum(irr_deltas), len(irr_deltas)) if irr_deltas else None
    risk_flags = []
    if avg_moic_delta is not None and avg_moic_delta >= 0.25:
        risk_flags.append("Gross-to-net MOIC compression is material across covered funds.")
    if covered_funds == 0:
        risk_flags.append("No funds in scope have consistent net metrics, so fee drag is not observable yet.")

    return {
        "meta": {
            "as_of_date": as_of,
            "fund_count": len(fund_rows),
        },
        "takeaway": {
            "text": "Fee drag matters most when gross performance looks strong but net outcomes compress materially. This page highlights where the gross-to-net gap is already visible from reported fund metrics.",
            "tone": "warning" if risk_flags else "positive",
            "why_it_matters": "LP value creation should be judged on what survives fees, expenses, and financing friction, not just gross deal outcomes.",
        },
        "coverage": {
            "items": [
                _coverage_item("Covered Funds", covered_funds, "positive" if covered_funds else "warning"),
                _coverage_item("Fund Coverage %", safe_divide(covered_funds, len(fund_rows)), "positive" if covered_funds else "warning"),
                _coverage_item("Detailed Expense Rows", 0, "neutral"),
                _coverage_item("Implied Value Drag", implied_value_drag_total, "warning" if implied_value_drag_total else "neutral"),
            ]
        },
        "confidence": _confidence_payload(
            "high" if covered_funds == len(fund_rows) and fund_rows else ("medium" if covered_funds else "low"),
            "Confidence is based on availability of consistent fund-level net metrics; detailed fee categories remain optional.",
        ),
        "summary": {
            "gross_to_net_moic_delta": avg_moic_delta,
            "gross_to_net_irr_delta": avg_irr_delta,
            "management_fee_load": None,
            "total_expense_ratio": None,
            "financing_drag": None,
            "fee_transparency_coverage_pct": safe_divide(covered_funds, len(fund_rows)),
        },
        "fee_bridge": {
            "implied_value_drag": implied_value_drag_total,
            "covered_funds": covered_funds,
        },
        "expense_breakdown": [
            {
                "category": "Implied Gross-to-Net Compression",
                "amount": implied_value_drag_total,
                "source": "Fund-level gross vs net metrics",
            }
        ] if covered_funds else [],
        "fund_rows": _sort_lp_fund_rows(fund_rows, deals, team_id=team_id, firm_id=firm_id),
        "risk_flags": risk_flags,
        "sections": ["summary", "fund_rows", "expense_breakdown"],
    }

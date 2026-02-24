"""Extended IC-oriented analytics built on top of deal-level and quarterly datasets."""

from __future__ import annotations

from collections import defaultdict
from datetime import date

from models import DealCashflowEvent, DealQuarterSnapshot, DealUnderwriteBaseline, FundQuarterSnapshot
from services.metrics.common import safe_divide
from services.metrics.deal import compute_deal_metrics


def _normalize_status(raw_status):
    status = (raw_status or "").strip().lower()
    if "partial" in status and "realized" in status:
        return "Partially Realized"
    if "fully" in status and "realized" in status:
        return "Fully Realized"
    if status == "realized" or ("realized" in status and "unrealized" not in status):
        return "Fully Realized"
    if "unrealized" in status or status == "":
        return "Unrealized"
    return "Other"


def _as_rate(value):
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if abs(out) > 1.5:
        out /= 100.0
    return out


def _pct_to_decimal(value):
    if value is None:
        return None
    try:
        return float(value) / 100.0
    except (TypeError, ValueError):
        return None


def _resolve_equity_value(snapshot):
    if snapshot is None:
        return None
    if snapshot.equity_value is not None:
        return snapshot.equity_value
    if snapshot.enterprise_value is not None and snapshot.net_debt is not None:
        return snapshot.enterprise_value - snapshot.net_debt
    return None


def _weighted_avg(rows, key, weight_key="invested_equity"):
    numerator = 0.0
    denominator = 0.0
    for row in rows:
        value = row.get(key)
        weight = row.get(weight_key) or 0.0
        if value is None or weight <= 0:
            continue
        numerator += value * weight
        denominator += weight
    return safe_divide(numerator, denominator)


def _group_summary(rows, group_key):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.get(group_key) or "Unknown"].append(row)

    out = []
    for group, rs in grouped.items():
        with_target = [r for r in rs if r.get("target_moic") is not None and r.get("actual_moic") is not None]
        hits = sum(1 for r in with_target if r.get("actual_moic") >= r.get("target_moic"))
        out.append(
            {
                "group": group,
                "deal_count": len(rs),
                "invested_equity": sum(r.get("invested_equity") or 0 for r in rs),
                "avg_delta_moic": _weighted_avg(rs, "delta_moic"),
                "avg_delta_irr": _weighted_avg(rs, "delta_irr"),
                "avg_delta_hold": _weighted_avg(rs, "delta_hold_years"),
                "hit_rate_moic": safe_divide(hits, len(with_target)) if with_target else None,
            }
        )

    return sorted(out, key=lambda r: r["invested_equity"], reverse=True)


def _latest_underwrite_by_deal(deal_ids):
    if not deal_ids:
        return {}

    rows = (
        DealUnderwriteBaseline.query.filter(DealUnderwriteBaseline.deal_id.in_(deal_ids))
        .order_by(DealUnderwriteBaseline.deal_id.asc(), DealUnderwriteBaseline.baseline_date.asc(), DealUnderwriteBaseline.id.asc())
        .all()
    )
    by_deal = {}
    for row in rows:
        by_deal[row.deal_id] = row
    return by_deal


def _quarter_end_from_date(d):
    if d is None:
        return None
    q = ((d.month - 1) // 3) + 1
    if q == 1:
        return date(d.year, 3, 31)
    if q == 2:
        return date(d.year, 6, 30)
    if q == 3:
        return date(d.year, 9, 30)
    return date(d.year, 12, 31)


def _empty_liquidity_payload():
    return {
        "has_data": False,
        "quarters": [],
        "paid_in": [],
        "distributed": [],
        "nav": [],
        "unfunded": [],
        "tvpi": [],
        "dpi": [],
        "rvpi": [],
        "pic": [],
        "gross_tvpi": [],
        "net_vs_gross": [],
        "fund_summaries": [],
        "latest": {
            "paid_in": None,
            "distributed": None,
            "nav": None,
            "unfunded": None,
            "tvpi": None,
            "dpi": None,
            "rvpi": None,
            "pic": None,
            "gross_tvpi": None,
        },
    }


def compute_fund_liquidity_analysis(deals, team_id=None):
    if not deals:
        return _empty_liquidity_payload()

    fund_set = sorted({d.fund_number for d in deals if d.fund_number})
    deal_ids = [d.id for d in deals if d.id is not None]

    fund_query = FundQuarterSnapshot.query
    if team_id is not None:
        fund_query = fund_query.filter(FundQuarterSnapshot.team_id == team_id)
    if fund_set:
        fund_query = fund_query.filter(FundQuarterSnapshot.fund_number.in_(fund_set))
    fund_rows = fund_query.order_by(FundQuarterSnapshot.quarter_end.asc(), FundQuarterSnapshot.fund_number.asc()).all()

    deal_quarter_rows = []
    if deal_ids:
        deal_quarter_rows = (
            DealQuarterSnapshot.query.filter(DealQuarterSnapshot.deal_id.in_(deal_ids))
            .order_by(DealQuarterSnapshot.quarter_end.asc(), DealQuarterSnapshot.deal_id.asc())
            .all()
        )

    quarter_totals = defaultdict(
        lambda: {
            "committed": 0.0,
            "paid_in": 0.0,
            "distributed": 0.0,
            "nav": 0.0,
            "unfunded": 0.0,
        }
    )
    fund_latest = {}

    for row in fund_rows:
        q = row.quarter_end
        agg = quarter_totals[q]
        agg["committed"] += row.committed_capital or 0.0
        agg["paid_in"] += row.paid_in_capital or 0.0
        agg["distributed"] += row.distributed_capital or 0.0
        agg["nav"] += row.nav or 0.0
        agg["unfunded"] += row.unfunded_commitment or 0.0
        fund_latest[row.fund_number] = row

    gross_equity_by_quarter = defaultdict(float)
    for row in deal_quarter_rows:
        value = _resolve_equity_value(row)
        if value is None:
            continue
        gross_equity_by_quarter[row.quarter_end] += value

    all_quarters = sorted(set(quarter_totals.keys()) | set(gross_equity_by_quarter.keys()))
    if not all_quarters:
        return _empty_liquidity_payload()

    total_invested = sum(d.equity_invested or 0.0 for d in deals)

    paid_in, distributed, nav, unfunded = [], [], [], []
    tvpi, dpi, rvpi, pic, gross_tvpi = [], [], [], [], []
    net_vs_gross = []

    for q in all_quarters:
        agg = quarter_totals.get(q) or {
            "committed": None,
            "paid_in": None,
            "distributed": None,
            "nav": None,
            "unfunded": None,
        }

        q_paid_in = agg.get("paid_in")
        q_distributed = agg.get("distributed")
        q_nav = agg.get("nav")
        q_committed = agg.get("committed")
        q_unfunded = agg.get("unfunded")

        q_tvpi = safe_divide((q_distributed or 0.0) + (q_nav or 0.0), q_paid_in)
        q_dpi = safe_divide(q_distributed, q_paid_in)
        q_rvpi = safe_divide(q_nav, q_paid_in)
        q_pic = safe_divide(q_paid_in, q_committed)
        q_gross_tvpi = safe_divide(gross_equity_by_quarter.get(q), total_invested)

        paid_in.append(q_paid_in)
        distributed.append(q_distributed)
        nav.append(q_nav)
        unfunded.append(q_unfunded)
        tvpi.append(q_tvpi)
        dpi.append(q_dpi)
        rvpi.append(q_rvpi)
        pic.append(q_pic)
        gross_tvpi.append(q_gross_tvpi)
        net_vs_gross.append({"quarter": q, "net_tvpi": q_tvpi, "gross_tvpi": q_gross_tvpi})

    fund_summaries = []
    for fund in sorted(fund_latest.keys()):
        row = fund_latest[fund]
        fund_deals = [d for d in deals if (d.fund_number or "Unknown Fund") == fund]
        invested = sum(d.equity_invested or 0.0 for d in fund_deals)
        value_total = sum((d.realized_value or 0.0) + (d.unrealized_value or 0.0) for d in fund_deals)

        fund_summaries.append(
            {
                "fund_number": fund,
                "quarter_end": row.quarter_end,
                "committed_capital": row.committed_capital,
                "paid_in_capital": row.paid_in_capital,
                "distributed_capital": row.distributed_capital,
                "nav": row.nav,
                "unfunded_commitment": row.unfunded_commitment,
                "tvpi": safe_divide((row.distributed_capital or 0.0) + (row.nav or 0.0), row.paid_in_capital),
                "dpi": safe_divide(row.distributed_capital, row.paid_in_capital),
                "rvpi": safe_divide(row.nav, row.paid_in_capital),
                "pic": safe_divide(row.paid_in_capital, row.committed_capital),
                "gross_tvpi": safe_divide(value_total, invested),
            }
        )

    latest_idx = len(all_quarters) - 1
    return {
        "has_data": True,
        "quarters": all_quarters,
        "paid_in": paid_in,
        "distributed": distributed,
        "nav": nav,
        "unfunded": unfunded,
        "tvpi": tvpi,
        "dpi": dpi,
        "rvpi": rvpi,
        "pic": pic,
        "gross_tvpi": gross_tvpi,
        "net_vs_gross": net_vs_gross,
        "fund_summaries": fund_summaries,
        "latest": {
            "paid_in": paid_in[latest_idx],
            "distributed": distributed[latest_idx],
            "nav": nav[latest_idx],
            "unfunded": unfunded[latest_idx],
            "tvpi": tvpi[latest_idx],
            "dpi": dpi[latest_idx],
            "rvpi": rvpi[latest_idx],
            "pic": pic[latest_idx],
            "gross_tvpi": gross_tvpi[latest_idx],
        },
    }


def _empty_underwrite_payload():
    return {
        "coverage": {
            "deal_count": 0,
            "invested_equity": 0.0,
            "hit_rate_moic": None,
            "avg_delta_irr": None,
            "avg_delta_moic": None,
            "avg_delta_hold_years": None,
        },
        "driver_deltas": {
            "entry_to_exit_multiple_delta": None,
            "exit_multiple_vs_underwrite": None,
            "ebitda_cagr_delta": None,
            "leverage_delta": None,
        },
        "rows": [],
        "by_partner": [],
        "by_sector": [],
        "by_entry_channel": [],
    }


def compute_underwrite_outcome_analysis(deals, metrics_by_id=None):
    if not deals:
        return _empty_underwrite_payload()

    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    underwrite_by_deal = _latest_underwrite_by_deal([d.id for d in deals if d.id is not None])

    rows = []
    for deal in deals:
        baseline = underwrite_by_deal.get(deal.id)
        if baseline is None:
            continue

        metrics = metrics_by_id[deal.id]
        target_irr = _as_rate(baseline.target_irr)
        target_rev_cagr = _as_rate(baseline.target_revenue_cagr)
        target_ebitda_cagr = _as_rate(baseline.target_ebitda_cagr)

        actual_irr = deal.irr if deal.irr is not None else metrics.get("implied_irr")
        actual_moic = metrics.get("moic")
        actual_hold = metrics.get("hold_period")
        actual_exit_multiple = metrics.get("exit_tev_ebitda")
        actual_entry_multiple = metrics.get("entry_tev_ebitda")
        actual_revenue_cagr = _pct_to_decimal(metrics.get("revenue_cagr"))
        actual_ebitda_cagr = _pct_to_decimal(metrics.get("ebitda_cagr"))

        row = {
            "deal_id": deal.id,
            "company_name": deal.company_name,
            "fund_number": deal.fund_number or "Unknown Fund",
            "status": _normalize_status(deal.status),
            "sector": deal.sector or "Unknown",
            "lead_partner": deal.lead_partner or "Unassigned",
            "entry_channel": deal.entry_channel or "Unknown",
            "invested_equity": metrics.get("equity") or 0.0,
            "actual_irr": actual_irr,
            "target_irr": target_irr,
            "delta_irr": actual_irr - target_irr if actual_irr is not None and target_irr is not None else None,
            "actual_moic": actual_moic,
            "target_moic": baseline.target_moic,
            "delta_moic": actual_moic - baseline.target_moic if actual_moic is not None and baseline.target_moic is not None else None,
            "actual_hold_years": actual_hold,
            "target_hold_years": baseline.target_hold_years,
            "delta_hold_years": actual_hold - baseline.target_hold_years if actual_hold is not None and baseline.target_hold_years is not None else None,
            "actual_entry_multiple": actual_entry_multiple,
            "actual_exit_multiple": actual_exit_multiple,
            "target_exit_multiple": baseline.target_exit_multiple,
            "entry_to_exit_multiple_delta": actual_exit_multiple - actual_entry_multiple if actual_exit_multiple is not None and actual_entry_multiple is not None else None,
            "exit_multiple_vs_underwrite": actual_exit_multiple - baseline.target_exit_multiple if actual_exit_multiple is not None and baseline.target_exit_multiple is not None else None,
            "actual_revenue_cagr": actual_revenue_cagr,
            "target_revenue_cagr": target_rev_cagr,
            "actual_ebitda_cagr": actual_ebitda_cagr,
            "target_ebitda_cagr": target_ebitda_cagr,
            "ebitda_cagr_delta": actual_ebitda_cagr - target_ebitda_cagr if actual_ebitda_cagr is not None and target_ebitda_cagr is not None else None,
            "leverage_delta": metrics.get("entry_net_debt_ebitda") - metrics.get("exit_net_debt_ebitda") if metrics.get("entry_net_debt_ebitda") is not None and metrics.get("exit_net_debt_ebitda") is not None else None,
        }
        rows.append(row)

    if not rows:
        return _empty_underwrite_payload()

    target_rows = [r for r in rows if r.get("target_moic") is not None and r.get("actual_moic") is not None]
    hits = sum(1 for r in target_rows if r["actual_moic"] >= r["target_moic"])

    return {
        "coverage": {
            "deal_count": len(rows),
            "invested_equity": sum(r.get("invested_equity") or 0.0 for r in rows),
            "hit_rate_moic": safe_divide(hits, len(target_rows)) if target_rows else None,
            "avg_delta_irr": _weighted_avg(rows, "delta_irr"),
            "avg_delta_moic": _weighted_avg(rows, "delta_moic"),
            "avg_delta_hold_years": _weighted_avg(rows, "delta_hold_years"),
        },
        "driver_deltas": {
            "entry_to_exit_multiple_delta": _weighted_avg(rows, "entry_to_exit_multiple_delta"),
            "exit_multiple_vs_underwrite": _weighted_avg(rows, "exit_multiple_vs_underwrite"),
            "ebitda_cagr_delta": _weighted_avg(rows, "ebitda_cagr_delta"),
            "leverage_delta": _weighted_avg(rows, "leverage_delta"),
        },
        "rows": sorted(rows, key=lambda r: r.get("invested_equity") or 0.0, reverse=True),
        "by_partner": _group_summary(rows, "lead_partner"),
        "by_sector": _group_summary(rows, "sector"),
        "by_entry_channel": _group_summary(rows, "entry_channel"),
    }


def _empty_valuation_quality_payload():
    return {
        "summary": {
            "unrealized_count": 0,
            "coverage_count": 0,
            "coverage_pct": None,
            "avg_staleness_days": None,
            "avg_mark_volatility": None,
            "markdown_deal_count": 0,
            "markdown_capital_pct": None,
            "avg_abs_mark_error": None,
            "mark_error_bias": None,
        },
        "staleness_buckets": [],
        "volatility_bands": [],
        "unrealized_rows": [],
        "mark_error_rows": [],
    }


def compute_valuation_quality_analysis(deals, as_of_date=None):
    if not deals:
        return _empty_valuation_quality_payload()

    as_of_date = as_of_date or date.today()
    deal_ids = [d.id for d in deals if d.id is not None]
    snapshots = []
    if deal_ids:
        snapshots = (
            DealQuarterSnapshot.query.filter(DealQuarterSnapshot.deal_id.in_(deal_ids))
            .order_by(DealQuarterSnapshot.deal_id.asc(), DealQuarterSnapshot.quarter_end.asc())
            .all()
        )

    snap_by_deal = defaultdict(list)
    for row in snapshots:
        snap_by_deal[row.deal_id].append(row)

    unrealized_deals = [d for d in deals if _normalize_status(d.status) == "Unrealized" or (d.unrealized_value or 0.0) > 0.0]
    unrealized_rows = []
    staleness_vals = []
    volatility_vals = []
    markdown_count = 0
    markdown_equity = 0.0
    total_unrealized_equity = sum(d.equity_invested or 0.0 for d in unrealized_deals)

    for deal in unrealized_deals:
        rows = snap_by_deal.get(deal.id, [])
        latest = rows[-1] if rows else None
        latest_val = _resolve_equity_value(latest)

        staleness_days = (as_of_date - latest.quarter_end).days if latest is not None else None
        if staleness_days is not None:
            staleness_vals.append(staleness_days)

        values = [_resolve_equity_value(r) for r in rows]
        values = [v for v in values if v is not None]
        qoq_changes = []
        for i in range(1, len(values)):
            prev = values[i - 1]
            curr = values[i]
            if prev is None or prev == 0:
                continue
            qoq_changes.append(abs((curr - prev) / prev))
        volatility = (sum(qoq_changes) / len(qoq_changes)) if qoq_changes else None
        if volatility is not None:
            volatility_vals.append(volatility)

        markdown = False
        if latest_val is not None and values:
            peak = max(values)
            markdown = latest_val < peak
        if markdown:
            markdown_count += 1
            markdown_equity += deal.equity_invested or 0.0

        unrealized_rows.append(
            {
                "deal_id": deal.id,
                "company_name": deal.company_name,
                "fund_number": deal.fund_number or "Unknown Fund",
                "staleness_days": staleness_days,
                "latest_mark": latest_val,
                "volatility": volatility,
                "markdown": markdown,
                "invested_equity": deal.equity_invested,
                "unrealized_value": deal.unrealized_value,
            }
        )

    staleness_buckets = {
        "<=90d": 0,
        "91-180d": 0,
        "181-365d": 0,
        ">365d": 0,
        "No Mark": 0,
    }
    for row in unrealized_rows:
        days = row["staleness_days"]
        if days is None:
            staleness_buckets["No Mark"] += 1
        elif days <= 90:
            staleness_buckets["<=90d"] += 1
        elif days <= 180:
            staleness_buckets["91-180d"] += 1
        elif days <= 365:
            staleness_buckets["181-365d"] += 1
        else:
            staleness_buckets[">365d"] += 1

    volatility_bands = {
        "Low (<10%)": 0,
        "Moderate (10-25%)": 0,
        "High (>25%)": 0,
        "No Series": 0,
    }
    for row in unrealized_rows:
        vol = row["volatility"]
        if vol is None:
            volatility_bands["No Series"] += 1
        elif vol < 0.10:
            volatility_bands["Low (<10%)"] += 1
        elif vol <= 0.25:
            volatility_bands["Moderate (10-25%)"] += 1
        else:
            volatility_bands["High (>25%)"] += 1

    mark_error_rows = []
    for deal in deals:
        if (deal.realized_value or 0.0) <= 0.0:
            continue
        rows = snap_by_deal.get(deal.id, [])
        if not rows:
            continue

        if deal.exit_date is not None:
            rows = [r for r in rows if r.quarter_end <= deal.exit_date]
        if not rows:
            continue

        pre_exit = rows[-1]
        pre_exit_val = _resolve_equity_value(pre_exit)
        if pre_exit_val is None or pre_exit_val == 0:
            continue

        mark_error = (deal.realized_value - pre_exit_val) / pre_exit_val
        mark_error_rows.append(
            {
                "deal_id": deal.id,
                "company_name": deal.company_name,
                "fund_number": deal.fund_number or "Unknown Fund",
                "exit_date": deal.exit_date,
                "pre_exit_quarter": pre_exit.quarter_end,
                "pre_exit_mark": pre_exit_val,
                "realized_value": deal.realized_value,
                "mark_error": mark_error,
                "abs_mark_error": abs(mark_error),
            }
        )

    coverage_count = sum(1 for row in unrealized_rows if row.get("latest_mark") is not None)
    avg_staleness = (sum(staleness_vals) / len(staleness_vals)) if staleness_vals else None
    avg_volatility = (sum(volatility_vals) / len(volatility_vals)) if volatility_vals else None

    mark_errors = [row["mark_error"] for row in mark_error_rows]
    avg_abs_mark_error = (sum(abs(v) for v in mark_errors) / len(mark_errors)) if mark_errors else None
    mark_error_bias = (sum(mark_errors) / len(mark_errors)) if mark_errors else None

    return {
        "summary": {
            "unrealized_count": len(unrealized_rows),
            "coverage_count": coverage_count,
            "coverage_pct": safe_divide(coverage_count, len(unrealized_rows)) if unrealized_rows else None,
            "avg_staleness_days": avg_staleness,
            "avg_mark_volatility": avg_volatility,
            "markdown_deal_count": markdown_count,
            "markdown_capital_pct": safe_divide(markdown_equity, total_unrealized_equity),
            "avg_abs_mark_error": avg_abs_mark_error,
            "mark_error_bias": mark_error_bias,
        },
        "staleness_buckets": [{"label": k, "count": v} for k, v in staleness_buckets.items()],
        "volatility_bands": [{"label": k, "count": v} for k, v in volatility_bands.items()],
        "unrealized_rows": sorted(unrealized_rows, key=lambda r: (r.get("staleness_days") or -1), reverse=True),
        "mark_error_rows": sorted(mark_error_rows, key=lambda r: r.get("abs_mark_error") or 0.0, reverse=True),
    }


def _empty_exit_readiness_payload():
    return {
        "summary": {
            "deal_count": 0,
            "avg_hold_years": None,
            "avg_thesis_score": None,
            "ready_count": 0,
            "avg_time_above_target_years": None,
        },
        "aging_buckets": [],
        "aging_by_fund": [],
        "aging_by_sector": [],
        "rows": [],
    }


def _aging_bucket(hold_years):
    if hold_years is None:
        return "Unknown"
    if hold_years < 3:
        return "<3y"
    if hold_years < 5:
        return "3-5y"
    if hold_years < 7:
        return "5-7y"
    return ">=7y"


def _score_ratio(actual, target):
    if actual is None or target is None or target <= 0:
        return None
    return max(0.0, min(1.0, actual / target))


def compute_exit_readiness_analysis(deals, metrics_by_id=None):
    if not deals:
        return _empty_exit_readiness_payload()

    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    underwrite_by_deal = _latest_underwrite_by_deal([d.id for d in deals if d.id is not None])

    deal_ids = [d.id for d in deals if d.id is not None]
    snapshots = []
    if deal_ids:
        snapshots = (
            DealQuarterSnapshot.query.filter(DealQuarterSnapshot.deal_id.in_(deal_ids))
            .order_by(DealQuarterSnapshot.deal_id.asc(), DealQuarterSnapshot.quarter_end.asc())
            .all()
        )
    snap_by_deal = defaultdict(list)
    for row in snapshots:
        snap_by_deal[row.deal_id].append(row)

    rows = []
    aging = defaultdict(lambda: {"deal_count": 0, "invested_equity": 0.0, "unrealized_value": 0.0})
    by_fund = defaultdict(lambda: defaultdict(float))
    by_sector = defaultdict(lambda: defaultdict(float))

    for deal in deals:
        if not (_normalize_status(deal.status) == "Unrealized" or (deal.unrealized_value or 0.0) > 0.0):
            continue

        metrics = metrics_by_id[deal.id]
        baseline = underwrite_by_deal.get(deal.id)

        current_moic = metrics.get("moic")
        hold_years = metrics.get("hold_period")
        target_moic = baseline.target_moic if baseline else None
        target_hold = baseline.target_hold_years if baseline else None
        target_ebitda_cagr = _as_rate(baseline.target_ebitda_cagr) if baseline else None
        actual_ebitda_cagr = _pct_to_decimal(metrics.get("ebitda_cagr"))

        perf_score = _score_ratio(current_moic, target_moic)
        time_score = _score_ratio(hold_years, target_hold)
        operating_score = _score_ratio(actual_ebitda_cagr, target_ebitda_cagr)

        components = [v for v in (perf_score, time_score, operating_score) if v is not None]
        thesis_score = (sum(components) / len(components) * 100.0) if components else None

        time_above_target_years = None
        if target_moic is not None and (deal.equity_invested or 0.0) > 0:
            q_count = 0
            for snap in snap_by_deal.get(deal.id, []):
                eq = _resolve_equity_value(snap)
                q_moic = safe_divide(eq, deal.equity_invested)
                if q_moic is not None and q_moic >= target_moic:
                    q_count += 1
            if q_count > 0:
                time_above_target_years = q_count / 4.0
            elif current_moic is not None and current_moic >= target_moic and hold_years is not None:
                time_above_target_years = hold_years

        ready = bool(thesis_score is not None and thesis_score >= 75 and (target_hold is None or (hold_years or 0) >= (target_hold * 0.8)))
        bucket = _aging_bucket(hold_years)
        fund = deal.fund_number or "Unknown Fund"
        sector = deal.sector or "Unknown"

        rows.append(
            {
                "deal_id": deal.id,
                "company_name": deal.company_name,
                "fund_number": fund,
                "sector": sector,
                "status": _normalize_status(deal.status),
                "invested_equity": metrics.get("equity") or 0.0,
                "unrealized_value": deal.unrealized_value or 0.0,
                "hold_years": hold_years,
                "current_moic": current_moic,
                "target_moic": target_moic,
                "target_hold_years": target_hold,
                "thesis_score": thesis_score,
                "time_above_target_years": time_above_target_years,
                "ready": ready,
                "aging_bucket": bucket,
            }
        )

        agg = aging[bucket]
        agg["deal_count"] += 1
        agg["invested_equity"] += metrics.get("equity") or 0.0
        agg["unrealized_value"] += deal.unrealized_value or 0.0
        by_fund[fund][bucket] += metrics.get("equity") or 0.0
        by_sector[sector][bucket] += metrics.get("equity") or 0.0

    if not rows:
        return _empty_exit_readiness_payload()

    holds = [r["hold_years"] for r in rows if r.get("hold_years") is not None]
    thesis_scores = [r["thesis_score"] for r in rows if r.get("thesis_score") is not None]
    above_target = [r["time_above_target_years"] for r in rows if r.get("time_above_target_years") is not None]

    bucket_order = ["<3y", "3-5y", "5-7y", ">=7y", "Unknown"]
    aging_buckets = [{"bucket": b, **aging[b]} for b in bucket_order if b in aging]

    def _matrix_from_map(source):
        out = []
        for label, bucket_map in sorted(source.items(), key=lambda kv: kv[0]):
            out.append({"label": label, "buckets": {b: bucket_map.get(b, 0.0) for b in bucket_order}})
        return out

    return {
        "summary": {
            "deal_count": len(rows),
            "avg_hold_years": (sum(holds) / len(holds)) if holds else None,
            "avg_thesis_score": (sum(thesis_scores) / len(thesis_scores)) if thesis_scores else None,
            "ready_count": sum(1 for r in rows if r.get("ready")),
            "avg_time_above_target_years": (sum(above_target) / len(above_target)) if above_target else None,
        },
        "aging_buckets": aging_buckets,
        "aging_by_fund": _matrix_from_map(by_fund),
        "aging_by_sector": _matrix_from_map(by_sector),
        "rows": sorted(rows, key=lambda r: (r.get("thesis_score") if r.get("thesis_score") is not None else -1), reverse=True),
    }


def _empty_stress_payload(default_scenario=None):
    default_scenario = default_scenario or {
        "default_multiple_shock": 0.0,
        "default_ebitda_shock": 0.0,
    }
    return {
        "scenario": default_scenario,
        "summary": {
            "deal_count": 0,
            "invested_equity": 0.0,
            "current_value": 0.0,
            "stressed_value": 0.0,
            "current_total_moic": None,
            "stressed_total_moic": None,
            "base_total_value": 0.0,
            "stressed_total_value": 0.0,
            "base_tvpi": None,
            "stressed_tvpi": None,
            "delta_value": 0.0,
            "delta_tvpi": None,
        },
        "deal_rows": [],
        "fund_subtotals": [],
        "fund_subtotals_map": {},
        "top_contributors": [],
    }


def _implied_irr_from_moic(moic, hold_years):
    if moic is None or hold_years is None or hold_years <= 0 or moic <= 0:
        return None
    try:
        return (moic ** (1.0 / hold_years)) - 1.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def compute_stress_lab_analysis(deals, scenario=None, deal_overrides=None, metrics_by_id=None):
    scenario = scenario or {}
    default_multiple_shock = float(scenario.get("default_multiple_shock", scenario.get("multiple_shock", 0.0)))
    default_ebitda_shock = float(scenario.get("default_ebitda_shock", scenario.get("ebitda_shock", 0.0)))
    scenario_clean = {
        "default_multiple_shock": default_multiple_shock,
        "default_ebitda_shock": default_ebitda_shock,
    }
    deal_overrides = deal_overrides or {}

    if not deals:
        return _empty_stress_payload(scenario_clean)

    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    deal_ids = [d.id for d in deals if d.id is not None]

    latest_snapshot = {}
    if deal_ids:
        snapshots = (
            DealQuarterSnapshot.query.filter(DealQuarterSnapshot.deal_id.in_(deal_ids))
            .order_by(DealQuarterSnapshot.deal_id.asc(), DealQuarterSnapshot.quarter_end.asc())
            .all()
        )
        for row in snapshots:
            latest_snapshot[row.deal_id] = row

    rows = []
    invested_total = 0.0
    base_total_value = 0.0
    stressed_total_value = 0.0
    fund_rollup = defaultdict(
        lambda: {
            "deal_count": 0,
            "invested_equity": 0.0,
            "current_total_value": 0.0,
            "stressed_total_value": 0.0,
            "delta_value": 0.0,
            "_current_hold_num": 0.0,
            "_current_hold_den": 0.0,
            "_expected_hold_num": 0.0,
            "_expected_hold_den": 0.0,
        }
    )

    status_rank = {"Fully Realized": 0, "Partially Realized": 1, "Unrealized": 2, "Other": 3}
    sorted_deals = sorted(
        deals,
        key=lambda d: (
            d.fund_number or "Unknown Fund",
            d.investment_date or date.max,
            status_rank.get(_normalize_status(d.status), 99),
        ),
    )

    for deal in sorted_deals:
        metrics = metrics_by_id[deal.id]
        status_norm = _normalize_status(deal.status)
        overrides = deal_overrides.get(deal.id, {})
        multiple_shock = float(overrides.get("multiple_shock", default_multiple_shock))
        ebitda_shock = float(overrides.get("ebitda_shock", default_ebitda_shock))

        invested = metrics.get("equity") or 0.0
        base_value = metrics.get("value_total") or 0.0
        realized_value = deal.realized_value or 0.0
        unrealized_value = deal.unrealized_value or 0.0

        snap = latest_snapshot.get(deal.id)
        base_ebitda = snap.ebitda if snap is not None and snap.ebitda is not None else deal.exit_ebitda
        base_net_debt = snap.net_debt if snap is not None and snap.net_debt is not None else deal.exit_net_debt
        if base_net_debt is None:
            base_net_debt = 0.0

        stressed_unrealized = unrealized_value
        is_unrealized_exposure = (status_norm == "Unrealized") or unrealized_value > 0.0

        if is_unrealized_exposure and base_ebitda is not None:
            current_ev = unrealized_value + base_net_debt
        elif snap is not None and snap.enterprise_value is not None:
            current_ev = snap.enterprise_value
        else:
            current_ev = deal.exit_enterprise_value

        base_multiple = safe_divide(current_ev, base_ebitda)
        stressed_ebitda = max(0.0, base_ebitda * (1.0 + ebitda_shock)) if base_ebitda is not None else None
        stressed_multiple = max(0.10, base_multiple + multiple_shock) if base_multiple is not None else None

        if is_unrealized_exposure:
            if stressed_ebitda is not None and stressed_multiple is not None:
                stressed_ev = stressed_multiple * stressed_ebitda
                stressed_equity = max(0.0, stressed_ev - (base_net_debt or 0.0))
                stressed_unrealized = stressed_equity
            else:
                stressed_unrealized = max(0.0, unrealized_value * (1.0 + ebitda_shock))

            # Guardrail: if both shocks are negative/zero, stressed value should not exceed current unrealized.
            if multiple_shock <= 0 and ebitda_shock <= 0:
                stressed_unrealized = min(stressed_unrealized, unrealized_value)

        stressed_total = realized_value + stressed_unrealized
        delta = stressed_total - base_value
        stressed_moic = safe_divide(stressed_total, invested)
        base_hold = metrics.get("hold_period")
        expected_hold = overrides.get("expected_hold_years", base_hold)
        if expected_hold is not None and expected_hold <= 0:
            expected_hold = None
        stressed_implied_irr = _implied_irr_from_moic(stressed_moic, expected_hold)

        rows.append(
            {
                "deal_id": deal.id,
                "company_name": deal.company_name,
                "fund_number": deal.fund_number or "Unknown Fund",
                "investment_date": deal.investment_date,
                "status": status_norm,
                "multiple_shock": multiple_shock,
                "ebitda_shock": ebitda_shock,
                "current_ebitda": base_ebitda,
                "current_multiple": base_multiple,
                "stressed_ebitda": stressed_ebitda,
                "stressed_multiple": stressed_multiple,
                "invested_equity": invested,
                "base_total_value": base_value,
                "current_total_value": base_value,
                "stressed_total_value": stressed_total,
                "delta_value": delta,
                "base_moic": safe_divide(base_value, invested),
                "current_moic": safe_divide(base_value, invested),
                "current_implied_irr": metrics.get("implied_irr"),
                "current_hold_period": base_hold,
                "expected_hold_period": expected_hold,
                "stressed_moic": stressed_moic,
                "stressed_implied_irr": stressed_implied_irr,
            }
        )

        fund_key = deal.fund_number or "Unknown Fund"
        agg = fund_rollup[fund_key]
        agg["deal_count"] += 1
        agg["invested_equity"] += invested
        agg["current_total_value"] += base_value
        agg["stressed_total_value"] += stressed_total
        agg["delta_value"] += delta
        if base_hold is not None and invested > 0:
            agg["_current_hold_num"] += base_hold * invested
            agg["_current_hold_den"] += invested
        if expected_hold is not None and invested > 0:
            agg["_expected_hold_num"] += expected_hold * invested
            agg["_expected_hold_den"] += invested

        invested_total += invested
        base_total_value += base_value
        stressed_total_value += stressed_total

    base_tvpi = safe_divide(base_total_value, invested_total)
    stressed_tvpi = safe_divide(stressed_total_value, invested_total)

    rows_sorted = rows
    top_contributors = sorted(rows, key=lambda r: r.get("delta_value") or 0.0)[:10]
    fund_order = []
    for deal in sorted_deals:
        fund_name = deal.fund_number or "Unknown Fund"
        if fund_name not in fund_order:
            fund_order.append(fund_name)

    fund_subtotals = []
    for fund_name in fund_order:
        agg = fund_rollup[fund_name]
        current_hold = safe_divide(agg["_current_hold_num"], agg["_current_hold_den"])
        expected_hold = safe_divide(agg["_expected_hold_num"], agg["_expected_hold_den"])
        current_moic = safe_divide(agg["current_total_value"], agg["invested_equity"])
        stressed_moic = safe_divide(agg["stressed_total_value"], agg["invested_equity"])
        current_irr = _implied_irr_from_moic(current_moic, current_hold)
        stressed_irr = _implied_irr_from_moic(stressed_moic, expected_hold)
        fund_subtotals.append(
            {
                "fund_number": fund_name,
                "deal_count": agg["deal_count"],
                "invested_equity": agg["invested_equity"],
                "current_total_value": agg["current_total_value"],
                "stressed_total_value": agg["stressed_total_value"],
                "delta_value": agg["delta_value"],
                "current_hold_period": current_hold,
                "expected_hold_period": expected_hold,
                "current_moic": current_moic,
                "stressed_moic": stressed_moic,
                "current_implied_irr": current_irr,
                "stressed_implied_irr": stressed_irr,
                "delta_moic": (stressed_moic - current_moic) if stressed_moic is not None and current_moic is not None else None,
            }
        )

    fund_subtotals_map = {row["fund_number"]: row for row in fund_subtotals}
    return {
        "scenario": scenario_clean,
        "summary": {
            "deal_count": len(rows),
            "invested_equity": invested_total,
            "current_value": base_total_value,
            "stressed_value": stressed_total_value,
            "current_total_moic": base_tvpi,
            "stressed_total_moic": stressed_tvpi,
            "base_total_value": base_total_value,
            "stressed_total_value": stressed_total_value,
            "base_tvpi": base_tvpi,
            "stressed_tvpi": stressed_tvpi,
            "delta_value": stressed_total_value - base_total_value,
            "delta_tvpi": (stressed_tvpi - base_tvpi) if stressed_tvpi is not None and base_tvpi is not None else None,
        },
        "deal_rows": rows_sorted,
        "fund_subtotals": fund_subtotals,
        "fund_subtotals_map": fund_subtotals_map,
        "top_contributors": top_contributors,
    }


def _empty_trajectory_payload(deal_options=None):
    return {
        "has_data": False,
        "selected_deal_id": None,
        "deal_options": deal_options or [],
        "summary": {
            "company_name": None,
            "fund_number": None,
            "hold_years": None,
            "current_moic": None,
            "current_equity_value": None,
        },
        "trajectory": [],
        "cashflow_curve": [],
    }


def compute_deal_trajectory_analysis(deals, deal_id=None, metrics_by_id=None):
    options = [{"deal_id": d.id, "company_name": d.company_name} for d in sorted(deals, key=lambda x: x.company_name or "")]
    if not deals:
        return _empty_trajectory_payload(options)

    by_id = {d.id: d for d in deals}
    selected = None
    if deal_id is not None:
        try:
            selected = by_id.get(int(deal_id))
        except (TypeError, ValueError):
            selected = None
    if selected is None:
        selected = sorted(deals, key=lambda d: d.company_name or "")[0]

    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    metrics = metrics_by_id[selected.id]

    snapshots = (
        DealQuarterSnapshot.query.filter(DealQuarterSnapshot.deal_id == selected.id)
        .order_by(DealQuarterSnapshot.quarter_end.asc())
        .all()
    )
    cashflows = (
        DealCashflowEvent.query.filter(DealCashflowEvent.deal_id == selected.id)
        .order_by(DealCashflowEvent.event_date.asc(), DealCashflowEvent.id.asc())
        .all()
    )

    trajectory = []
    for row in snapshots:
        implied_multiple = safe_divide(row.enterprise_value, row.ebitda)
        trajectory.append(
            {
                "quarter_end": row.quarter_end,
                "revenue": row.revenue,
                "ebitda": row.ebitda,
                "enterprise_value": row.enterprise_value,
                "net_debt": row.net_debt,
                "equity_value": _resolve_equity_value(row),
                "implied_multiple": implied_multiple,
                "valuation_basis": row.valuation_basis,
                "source": row.source,
            }
        )

    calls_by_quarter = defaultdict(float)
    distributions_by_quarter = defaultdict(float)
    for cf in cashflows:
        q = _quarter_end_from_date(cf.event_date)
        if q is None:
            continue
        event = (cf.event_type or "").strip().lower()
        amount = cf.amount or 0.0
        if "call" in event or "contribution" in event or "invest" in event:
            calls_by_quarter[q] += abs(amount)
        elif "distribution" in event or "proceed" in event or "return" in event:
            distributions_by_quarter[q] += abs(amount)
        elif amount < 0:
            calls_by_quarter[q] += abs(amount)
        else:
            distributions_by_quarter[q] += abs(amount)

    all_quarters = sorted(set(calls_by_quarter.keys()) | set(distributions_by_quarter.keys()) | {r["quarter_end"] for r in trajectory})
    cumulative_calls = 0.0
    cumulative_distributions = 0.0
    cashflow_curve = []
    for q in all_quarters:
        cumulative_calls += calls_by_quarter.get(q, 0.0)
        cumulative_distributions += distributions_by_quarter.get(q, 0.0)
        cashflow_curve.append(
            {
                "quarter_end": q,
                "calls": calls_by_quarter.get(q, 0.0),
                "distributions": distributions_by_quarter.get(q, 0.0),
                "cum_calls": cumulative_calls,
                "cum_distributions": cumulative_distributions,
            }
        )

    current_equity = None
    if trajectory:
        current_equity = trajectory[-1].get("equity_value")
    if current_equity is None:
        current_equity = selected.unrealized_value

    return {
        "has_data": bool(trajectory or cashflow_curve),
        "selected_deal_id": selected.id,
        "deal_options": options,
        "summary": {
            "company_name": selected.company_name,
            "fund_number": selected.fund_number or "Unknown Fund",
            "hold_years": metrics.get("hold_period"),
            "current_moic": metrics.get("moic"),
            "current_equity_value": current_equity,
        },
        "trajectory": trajectory,
        "cashflow_curve": cashflow_curve,
    }

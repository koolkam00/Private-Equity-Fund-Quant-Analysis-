"""Portfolio aggregation analytics for deal-only dashboards."""

from __future__ import annotations

from collections import defaultdict
import math
import re

from services.metrics.bridge import DRIVERS
from services.metrics.deal import compute_bridge_view, compute_deal_metrics
from services.metrics.common import EPS, safe_divide

TRACK_RECORD_STATUS_ORDER = ("Fully Realized", "Partially Realized", "Unrealized", "Other")


def _avg(values):
    return sum(values) / len(values) if values else None


def _wavg(pairs):
    numer = sum(v * w for v, w in pairs)
    denom = sum(w for _, w in pairs)
    if denom <= 0:
        return None
    return numer / denom


def _metric_aggregate(metrics, metric_key):
    vals = [m[metric_key] for m in metrics if m.get(metric_key) is not None]
    weighted = [(m[metric_key], m["equity"]) for m in metrics if m.get(metric_key) is not None and (m.get("equity") or 0) > 0]
    return {"avg": _avg(vals), "wavg": _wavg(weighted) if weighted else _avg(vals)}


def _normalize_track_status(raw_status):
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


def _empty_track_totals():
    return {
        "deal_count": 0,
        "invested_equity": 0.0,
        "realized_value": 0.0,
        "unrealized_value": 0.0,
        "total_value": 0.0,
        "_gross_irr_weight_num": 0.0,
        "_gross_irr_weight_den": 0.0,
        "_hold_period_weight_num": 0.0,
        "_hold_period_weight_den": 0.0,
        "_ownership_weight_num": 0.0,
        "_ownership_weight_den": 0.0,
    }


def _update_track_totals(totals, row):
    equity = row.get("invested_equity") or 0.0
    realized = row.get("realized_value") or 0.0
    unrealized = row.get("unrealized_value") or 0.0
    total_value = row.get("total_value") or 0.0
    gross_irr = row.get("gross_irr")
    hold_period = row.get("hold_period")
    ownership_pct = row.get("ownership_pct")

    totals["deal_count"] += 1
    totals["invested_equity"] += equity
    totals["realized_value"] += realized
    totals["unrealized_value"] += unrealized
    totals["total_value"] += total_value

    if gross_irr is not None and equity > 0:
        totals["_gross_irr_weight_num"] += gross_irr * equity
        totals["_gross_irr_weight_den"] += equity
    if hold_period is not None and equity > 0:
        totals["_hold_period_weight_num"] += hold_period * equity
        totals["_hold_period_weight_den"] += equity
    if ownership_pct is not None and equity > 0:
        totals["_ownership_weight_num"] += ownership_pct * equity
        totals["_ownership_weight_den"] += equity


def _merge_track_totals(lhs, rhs):
    merged = _empty_track_totals()
    for key in merged:
        if key == "deal_count":
            merged[key] = int((lhs.get(key) or 0) + (rhs.get(key) or 0))
        else:
            merged[key] = (lhs.get(key) or 0.0) + (rhs.get(key) or 0.0)
    return merged


def _finalize_track_totals(raw_totals, invested_total=None, fund_size=None):
    invested_equity = raw_totals["invested_equity"]
    total_value = raw_totals["total_value"]
    realized_value = raw_totals["realized_value"]
    unrealized_value = raw_totals["unrealized_value"]

    return {
        "deal_count": raw_totals["deal_count"],
        "invested_equity": invested_equity,
        "realized_value": realized_value,
        "unrealized_value": unrealized_value,
        "total_value": total_value,
        "hold_period": safe_divide(raw_totals["_hold_period_weight_num"], raw_totals["_hold_period_weight_den"]),
        "ownership_pct": safe_divide(raw_totals["_ownership_weight_num"], raw_totals["_ownership_weight_den"]),
        "pct_total_invested": safe_divide(invested_equity, invested_total),
        "pct_fund_size": safe_divide(invested_equity, fund_size),
        "gross_irr": safe_divide(raw_totals["_gross_irr_weight_num"], raw_totals["_gross_irr_weight_den"]),
        "gross_moic": safe_divide(total_value, invested_equity),
        "realized_gross_moic": safe_divide(realized_value, invested_equity),
        "unrealized_gross_moic": safe_divide(unrealized_value, invested_equity),
        # Backward compatibility for pre-existing callers/tests.
        "moic": safe_divide(total_value, invested_equity),
        "irr": safe_divide(raw_totals["_gross_irr_weight_num"], raw_totals["_gross_irr_weight_den"]),
    }


def _resolve_scalar(values, tolerance=1e-9):
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return {"value": None, "conflict": False}
    base = clean[0]
    if any(abs(v - base) > tolerance for v in clean[1:]):
        return {"value": None, "conflict": True}
    return {"value": base, "conflict": False}


def _fund_net_performance(rows):
    irr = _resolve_scalar([r.get("net_irr") for r in rows])
    moic = _resolve_scalar([r.get("net_moic") for r in rows])
    dpi = _resolve_scalar([r.get("net_dpi") for r in rows])
    return {
        "net_irr": irr["value"],
        "net_moic": moic["value"],
        "net_dpi": dpi["value"],
        "conflicts": {
            "net_irr": irr["conflict"],
            "net_moic": moic["conflict"],
            "net_dpi": dpi["conflict"],
        },
    }


def compute_portfolio_analytics(deals, metrics_by_id=None):
    metrics = list(metrics_by_id.values()) if metrics_by_id is not None else [compute_deal_metrics(d) for d in deals]

    total_equity = sum(m.get("equity") or 0 for m in metrics)
    total_value = sum(m.get("value_total") or 0 for m in metrics)
    total_created = sum(m.get("value_created") or 0 for m in metrics)

    gross_moic = safe_divide(total_value, total_equity)

    returns = {
        "gross_moic": {"avg": gross_moic, "wavg": gross_moic},
        "implied_irr": _metric_aggregate(metrics, "implied_irr"),
        "hold_period": _metric_aggregate(metrics, "hold_period"),
    }

    entry = {
        "revenue": _metric_aggregate(metrics, "entry_revenue"),
        "ebitda": _metric_aggregate(metrics, "entry_ebitda"),
        "tev": _metric_aggregate(metrics, "entry_enterprise_value"),
        "net_debt": _metric_aggregate(metrics, "entry_net_debt"),
        "tev_ebitda": _metric_aggregate(metrics, "entry_tev_ebitda"),
        "tev_revenue": _metric_aggregate(metrics, "entry_tev_revenue"),
        "net_debt_ebitda": _metric_aggregate(metrics, "entry_net_debt_ebitda"),
        "net_debt_tev": _metric_aggregate(metrics, "entry_net_debt_tev"),
        "ebitda_margin": _metric_aggregate(metrics, "entry_ebitda_margin"),
    }

    exit_ = {
        "revenue": _metric_aggregate(metrics, "exit_revenue"),
        "ebitda": _metric_aggregate(metrics, "exit_ebitda"),
        "tev": _metric_aggregate(metrics, "exit_enterprise_value"),
        "net_debt": _metric_aggregate(metrics, "exit_net_debt"),
        "tev_ebitda": _metric_aggregate(metrics, "exit_tev_ebitda"),
        "tev_revenue": _metric_aggregate(metrics, "exit_tev_revenue"),
        "net_debt_ebitda": _metric_aggregate(metrics, "exit_net_debt_ebitda"),
        "net_debt_tev": _metric_aggregate(metrics, "exit_net_debt_tev"),
        "ebitda_margin": _metric_aggregate(metrics, "exit_ebitda_margin"),
    }

    growth = {
        "revenue_growth": _metric_aggregate(metrics, "revenue_growth"),
        "ebitda_growth": _metric_aggregate(metrics, "ebitda_growth"),
        "revenue_cagr": _metric_aggregate(metrics, "revenue_cagr"),
        "ebitda_cagr": _metric_aggregate(metrics, "ebitda_cagr"),
    }

    return {
        "total_equity": total_equity,
        "total_value": total_value,
        "total_value_created": total_created,
        "returns": returns,
        "entry": entry,
        "exit": exit_,
        "growth": growth,
    }


def compute_bridge_aggregate(deals, basis="fund"):
    sums = {k: 0.0 for k in DRIVERS}
    ready_count = 0
    total_equity = 0.0
    total_value_created = 0.0

    for d in deals:
        warnings = []
        bridge = compute_bridge_view(d, model="additive", basis=basis, unit="dollar", warnings=warnings)
        eq = d.equity_invested or 0
        total_equity += eq

        if basis == "fund":
            total_value_created += (d.realized_value or 0) + (d.unrealized_value or 0) - eq
        else:
            # For company basis aggregate, use bridge-provided company value when available.
            if bridge.get("company_value_created") is not None:
                total_value_created += bridge.get("company_value_created")

        if not bridge.get("ready"):
            continue

        ready_count += 1
        for k in DRIVERS:
            v = bridge["drivers_dollar"].get(k)
            if v is not None:
                sums[k] += v

    moic = {k: (safe_divide(v, total_equity) if total_equity > 0 else None) for k, v in sums.items()}
    pct = {
        k: (safe_divide(v, total_value_created) if abs(total_value_created) > EPS else None)
        for k, v in sums.items()
    }

    start_end = {
        "dollar": {
            "start": total_equity,
            "end": total_equity + total_value_created,
        },
        "moic": {
            "start": 1.0 if total_equity > 0 else None,
            "end": safe_divide(total_equity + total_value_created, total_equity) if total_equity > 0 else None,
        },
        "pct": {
            "start": 0.0,
            "end": 1.0,
        },
    }

    return {
        "model": "additive",
        "basis": basis,
        "ready_count": ready_count,
        "drivers": {
            "dollar": sums,
            "moic": moic,
            "pct": pct,
        },
        "total_value_created": total_value_created,
        "total_equity": total_equity,
        "start_end": start_end,
    }


def compute_vintage_series(deals, metrics_by_id=None):
    by_year = defaultdict(list)
    for d in deals:
        yr = d.year_invested or (d.investment_date.year if d.investment_date else None)
        if yr is not None:
            by_year[int(yr)].append(d)

    out = []
    for yr in sorted(by_year):
        ds = by_year[yr]
        metrics = [metrics_by_id[d.id] for d in ds] if metrics_by_id is not None else [compute_deal_metrics(d) for d in ds]
        total_equity = sum(m.get("equity") or 0 for m in metrics)
        total_value = sum(m.get("value_total") or 0 for m in metrics)
        total_created = sum(m.get("value_created") or 0 for m in metrics)
        moic = safe_divide(total_value, total_equity)

        out.append(
            {
                "year": yr,
                "deal_count": len(ds),
                "total_equity": total_equity,
                "total_value_created": total_created,
                "avg_moic": moic,
            }
        )

    return out


def compute_moic_hold_scatter(deals, metrics_by_id=None):
    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    max_equity = max((metrics_by_id[d.id].get("equity") or 0) for d in deals) if deals else 0
    points = []

    for d in deals:
        m = metrics_by_id[d.id]
        moic = m.get("moic")
        hold = m.get("hold_period")
        equity = m.get("equity") or 0
        if moic is None or hold is None:
            continue

        radius = 6
        if max_equity > 0 and equity > 0:
            radius = 5 + (9 * math.sqrt(equity / max_equity))

        points.append(
            {
                "x": hold,
                "y": moic,
                "r": round(radius, 2),
                "company": d.company_name,
                "status": d.status or "Unrealized",
                "sector": d.sector or "Unknown",
                "equity": equity,
            }
        )

    return points


def compute_value_creation_mix(deals, metrics_by_id=None, group_by="fund"):
    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    groups = {}

    def _group_key(deal):
        if group_by == "fund":
            return deal.fund_number or "Unknown Fund"
        if group_by == "sector":
            return deal.sector or "Unknown Sector"
        if group_by == "exit_type":
            return deal.exit_type or "Not Specified"
        raise ValueError(f"Unsupported group_by: {group_by}")

    for d in deals:
        group = _group_key(d)
        m = metrics_by_id[d.id]
        bridge = m.get("bridge_additive_fund", {})
        if not bridge.get("ready"):
            continue

        agg = groups.setdefault(
            group,
            {"drivers_dollar": {k: 0.0 for k in DRIVERS}, "total_value_created": 0.0},
        )
        drivers = bridge.get("drivers_dollar", {})
        for k in DRIVERS:
            v = drivers.get(k)
            if v is not None:
                agg["drivers_dollar"][k] += v
        agg["total_value_created"] += bridge.get("value_created") or 0.0

    ordered = sorted(groups.items(), key=lambda kv: abs(kv[1]["total_value_created"]), reverse=True)
    labels = [k for k, _ in ordered]
    totals = [v["total_value_created"] for _, v in ordered]
    drivers_pct = {k: [] for k in DRIVERS}

    for _, agg in ordered:
        total = agg["total_value_created"]
        for k in DRIVERS:
            val = agg["drivers_dollar"].get(k) or 0.0
            drivers_pct[k].append(safe_divide(val, total) if abs(total) > EPS else None)

    return {
        "labels": labels,
        "drivers": drivers_pct,
        "totals_dollar": totals,
    }


def compute_realized_unrealized_exposure(deals):
    by_fund = defaultdict(lambda: {"realized": 0.0, "unrealized": 0.0})
    for d in deals:
        key = d.fund_number or "Unknown Fund"
        by_fund[key]["realized"] += d.realized_value or 0.0
        by_fund[key]["unrealized"] += d.unrealized_value or 0.0

    ordered = sorted(by_fund.items(), key=lambda kv: kv[0])
    return {
        "labels": [k for k, _ in ordered],
        "realized": [v["realized"] for _, v in ordered],
        "unrealized": [v["unrealized"] for _, v in ordered],
    }


def compute_loss_concentration_heatmap(deals, metrics_by_id=None):
    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    matrix = defaultdict(lambda: defaultdict(float))

    for d in deals:
        m = metrics_by_id[d.id]
        moic = m.get("moic")
        equity = m.get("equity") or 0.0
        if moic is None or moic >= 1.0 or equity <= 0:
            continue
        sector = d.sector or "Unknown"
        geography = d.geography or "Unknown"
        matrix[sector][geography] += equity

    sectors = sorted(matrix.keys())
    geographies = sorted({g for s in sectors for g in matrix[s].keys()})
    values = [[matrix[s].get(g, 0.0) for g in geographies] for s in sectors]
    max_value = max((v for row in values for v in row), default=0.0)

    return {
        "sectors": sectors,
        "geographies": geographies,
        "values": values,
        "max_value": max_value,
    }


def compute_exit_type_performance(deals, metrics_by_id=None):
    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    by_exit = defaultdict(
        lambda: {
            "deal_count": 0,
            "total_equity": 0.0,
            "total_value": 0.0,
            "realized_value": 0.0,
        }
    )

    for d in deals:
        key = d.exit_type or "Not Specified"
        m = metrics_by_id[d.id]
        equity = m.get("equity") or 0.0
        value_total = m.get("value_total") or 0.0
        by_exit[key]["deal_count"] += 1
        by_exit[key]["total_equity"] += equity
        by_exit[key]["total_value"] += value_total
        by_exit[key]["realized_value"] += d.realized_value or 0.0

    ordered = sorted(by_exit.items(), key=lambda kv: kv[1]["total_equity"], reverse=True)
    return {
        "labels": [k for k, _ in ordered],
        "calculated_moic": [safe_divide(v["total_value"], v["total_equity"]) for _, v in ordered],
        "deal_count": [v["deal_count"] for _, v in ordered],
        "realized_value": [v["realized_value"] for _, v in ordered],
    }


def compute_deal_track_record(deals, metrics_by_id=None):
    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}

    grouped = defaultdict(list)
    status_index = {status: idx for idx, status in enumerate(TRACK_RECORD_STATUS_ORDER)}

    for d in deals:
        metric = metrics_by_id[d.id]
        status = _normalize_track_status(d.status)
        fund = d.fund_number or "Unknown Fund"
        ownership_pct = d.ownership_pct
        if ownership_pct is None:
            ownership_pct = (metric.get("bridge_additive_fund") or {}).get("ownership_pct")
        gross_irr = d.irr if d.irr is not None else metric.get("implied_irr")

        row = {
            "deal_id": d.id,
            "company_name": d.company_name or "Unknown Company",
            "status": status,
            "investment_date": d.investment_date,
            "exit_date": d.exit_date,
            "hold_period": metric.get("hold_period"),
            "ownership_pct": ownership_pct,
            "invested_equity": metric.get("equity"),
            "realized_value": metric.get("realized"),
            "unrealized_value": metric.get("unrealized"),
            "total_value": metric.get("value_total"),
            "gross_irr": gross_irr,
            "gross_moic": metric.get("moic"),
            "realized_gross_moic": metric.get("realized_moic"),
            "unrealized_gross_moic": metric.get("unrealized_moic"),
            "fund_size": d.fund_size,
            "net_irr": d.net_irr,
            "net_moic": d.net_moic,
            "net_dpi": d.net_dpi,
            # Backward compatibility aliases used by pre-existing tests.
            "moic": metric.get("moic"),
            "irr": gross_irr,
        }
        grouped[fund].append(row)

    fund_groups = []
    overall_status_totals = {status: _empty_track_totals() for status in TRACK_RECORD_STATUS_ORDER}
    overall_totals = _empty_track_totals()
    overall_realized_totals = _empty_track_totals()
    overall_unrealized_totals = _empty_track_totals()

    for fund in sorted(grouped.keys(), key=lambda v: (v == "Unknown Fund", v)):
        fund_rows = sorted(grouped[fund], key=lambda r: (status_index.get(r["status"], 99), r["company_name"], r["deal_id"]))
        fund_totals = _empty_track_totals()
        fund_status_totals = {status: _empty_track_totals() for status in TRACK_RECORD_STATUS_ORDER}

        for row in fund_rows:
            _update_track_totals(fund_totals, row)
            _update_track_totals(fund_status_totals[row["status"]], row)
            _update_track_totals(overall_totals, row)
            _update_track_totals(overall_status_totals[row["status"]], row)
            if row["status"] in {"Fully Realized", "Partially Realized"}:
                _update_track_totals(overall_realized_totals, row)
            if row["status"] == "Unrealized":
                _update_track_totals(overall_unrealized_totals, row)

        fund_size_meta = _resolve_scalar([r.get("fund_size") for r in fund_rows])
        fund_size = fund_size_meta["value"]
        fund_invested_total = fund_totals["invested_equity"]

        for idx, row in enumerate(fund_rows, start=1):
            row["row_num"] = idx
            row["pct_total_invested"] = safe_divide(row.get("invested_equity"), fund_invested_total)
            row["pct_fund_size"] = safe_divide(row.get("invested_equity"), fund_size)

        status_rollups = []
        for status in TRACK_RECORD_STATUS_ORDER:
            raw = fund_status_totals[status]
            if raw["deal_count"] == 0:
                continue
            status_rollups.append(
                {
                    "status": status,
                    "label": f"All {raw['deal_count']} {fund} {status} Investments",
                    "totals": _finalize_track_totals(raw, invested_total=fund_invested_total, fund_size=fund_size),
                }
            )

        realized_raw = _merge_track_totals(fund_status_totals["Fully Realized"], fund_status_totals["Partially Realized"])
        unrealized_raw = fund_status_totals["Unrealized"]
        fund_summary_rollups = []
        if realized_raw["deal_count"] > 0:
            fund_summary_rollups.append(
                {
                    "label": f"All {realized_raw['deal_count']} {fund} Fully and Partially Realized Investments",
                    "totals": _finalize_track_totals(realized_raw, invested_total=fund_invested_total, fund_size=fund_size),
                }
            )
        if unrealized_raw["deal_count"] > 0:
            fund_summary_rollups.append(
                {
                    "label": f"All {unrealized_raw['deal_count']} {fund} Unrealized Investments",
                    "totals": _finalize_track_totals(unrealized_raw, invested_total=fund_invested_total, fund_size=fund_size),
                }
            )
        fund_summary_rollups.append(
            {
                "label": f"All {fund_totals['deal_count']} {fund} Investments",
                "totals": _finalize_track_totals(fund_totals, invested_total=fund_invested_total, fund_size=fund_size),
            }
        )

        fund_groups.append(
            {
                "fund_name": fund,
                "fund_size": fund_size,
                "fund_size_conflict": fund_size_meta["conflict"],
                "rows": fund_rows,
                "status_rollups": status_rollups,
                "summary_rollups": fund_summary_rollups,
                "net_performance": _fund_net_performance(fund_rows),
                "totals": _finalize_track_totals(fund_totals, invested_total=fund_invested_total, fund_size=fund_size),
            }
        )

    overall_invested_total = overall_totals["invested_equity"]
    overall_status_rollups = []
    for status in TRACK_RECORD_STATUS_ORDER:
        totals = _finalize_track_totals(overall_status_totals[status], invested_total=overall_invested_total)
        if totals["deal_count"] == 0:
            continue
        overall_status_rollups.append(
            {"status": status, "label": f"All {totals['deal_count']} {status} Investments", "totals": totals}
        )

    overall_summary_rollups = []
    if overall_realized_totals["deal_count"] > 0:
        overall_summary_rollups.append(
            {
                "label": f"All {overall_realized_totals['deal_count']} Fully and Partially Realized Investments",
                "totals": _finalize_track_totals(overall_realized_totals, invested_total=overall_invested_total),
            }
        )
    if overall_unrealized_totals["deal_count"] > 0:
        overall_summary_rollups.append(
            {
                "label": f"All {overall_unrealized_totals['deal_count']} Unrealized Investments",
                "totals": _finalize_track_totals(overall_unrealized_totals, invested_total=overall_invested_total),
            }
        )
    overall_summary_rollups.append(
        {
            "label": f"All {overall_totals['deal_count']} Investments",
            "totals": _finalize_track_totals(overall_totals, invested_total=overall_invested_total),
        }
    )

    return {
        "funds": fund_groups,
        "overall": {
            "status_rollups": overall_status_rollups,
            "summary_rollups": overall_summary_rollups,
            # Backward compatibility key name used by old tests.
            "status_groups": overall_status_rollups,
            "totals": _finalize_track_totals(overall_totals, invested_total=overall_invested_total),
        },
    }


def _slug_token(value, default="na"):
    token = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return token or default


def _summary_bucket_from_label(label):
    text = (label or "").strip().lower()
    if "fully and partially realized" in text:
        return "realized"
    if "unrealized investments" in text:
        return "unrealized"
    return "all"


def _ensure_unique_row_key(base_key, used_keys):
    if base_key not in used_keys:
        used_keys.add(base_key)
        return base_key
    suffix = 2
    while f"{base_key}__{suffix}" in used_keys:
        suffix += 1
    key = f"{base_key}__{suffix}"
    used_keys.add(key)
    return key


def _build_rollup_detail_entry(row_key, label, scope, subset, metrics_by_id, fund_name=None, status=None):
    subset_metrics = {d.id: metrics_by_id[d.id] for d in subset if d.id in metrics_by_id}
    portfolio = compute_portfolio_analytics(subset, metrics_by_id=subset_metrics)
    bridge = compute_bridge_aggregate(subset, basis="fund")
    return {
        "row_key": row_key,
        "label": label,
        "scope": scope,
        "fund_name": fund_name,
        "status": status,
        "deal_count": len(subset),
        "entry_exit": {
            "entry": portfolio.get("entry", {}),
            "exit": portfolio.get("exit", {}),
            "growth": portfolio.get("growth", {}),
            "returns": portfolio.get("returns", {}),
        },
        "bridge": bridge,
    }


def compute_deals_rollup_details(deals, track_record, metrics_by_id=None):
    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    deals_by_id = {d.id: d for d in deals if d.id is not None}
    details = {}
    used_keys = set()
    status_order = set(TRACK_RECORD_STATUS_ORDER)

    for fund in track_record.get("funds", []):
        fund_name = fund.get("fund_name") or "Unknown Fund"
        fund_rows = fund.get("rows", [])
        fund_deals = [deals_by_id[r.get("deal_id")] for r in fund_rows if r.get("deal_id") in deals_by_id]

        by_status = defaultdict(list)
        for row in fund_rows:
            deal = deals_by_id.get(row.get("deal_id"))
            if not deal:
                continue
            by_status[row.get("status")].append(deal)

        for rollup in fund.get("status_rollups", []):
            status = rollup.get("status")
            subset = by_status.get(status, [])
            base_key = f"fund_{_slug_token(fund_name)}__status_{_slug_token(status)}"
            row_key = _ensure_unique_row_key(base_key, used_keys)
            rollup["row_key"] = row_key
            details[row_key] = _build_rollup_detail_entry(
                row_key=row_key,
                label=rollup.get("label"),
                scope="fund_status",
                subset=subset,
                metrics_by_id=metrics_by_id,
                fund_name=fund_name,
                status=status,
            )

        for rollup in fund.get("summary_rollups", []):
            bucket = _summary_bucket_from_label(rollup.get("label"))
            if bucket == "realized":
                subset = [d for d in fund_deals if _normalize_track_status(d.status) in {"Fully Realized", "Partially Realized"}]
            elif bucket == "unrealized":
                subset = [d for d in fund_deals if _normalize_track_status(d.status) == "Unrealized"]
            else:
                subset = list(fund_deals)
            base_key = f"fund_{_slug_token(fund_name)}__summary_{_slug_token(bucket)}"
            row_key = _ensure_unique_row_key(base_key, used_keys)
            rollup["row_key"] = row_key
            details[row_key] = _build_rollup_detail_entry(
                row_key=row_key,
                label=rollup.get("label"),
                scope="fund_summary",
                subset=subset,
                metrics_by_id=metrics_by_id,
                fund_name=fund_name,
            )

    overall = track_record.get("overall", {})
    for rollup in overall.get("status_rollups", []):
        status = rollup.get("status")
        if status not in status_order:
            continue
        subset = [d for d in deals if _normalize_track_status(d.status) == status]
        base_key = f"overall__status_{_slug_token(status)}"
        row_key = _ensure_unique_row_key(base_key, used_keys)
        rollup["row_key"] = row_key
        details[row_key] = _build_rollup_detail_entry(
            row_key=row_key,
            label=rollup.get("label"),
            scope="overall_status",
            subset=subset,
            metrics_by_id=metrics_by_id,
            status=status,
        )

    for rollup in overall.get("summary_rollups", []):
        bucket = _summary_bucket_from_label(rollup.get("label"))
        if bucket == "realized":
            subset = [d for d in deals if _normalize_track_status(d.status) in {"Fully Realized", "Partially Realized"}]
        elif bucket == "unrealized":
            subset = [d for d in deals if _normalize_track_status(d.status) == "Unrealized"]
        else:
            subset = list(deals)
        base_key = f"overall__summary_{_slug_token(bucket)}"
        row_key = _ensure_unique_row_key(base_key, used_keys)
        rollup["row_key"] = row_key
        details[row_key] = _build_rollup_detail_entry(
            row_key=row_key,
            label=rollup.get("label"),
            scope="overall_summary",
            subset=subset,
            metrics_by_id=metrics_by_id,
        )

    return details


def compute_lead_partner_scorecard(deals, metrics_by_id=None):
    metrics_by_id = metrics_by_id or {d.id: compute_deal_metrics(d) for d in deals}
    by_partner = defaultdict(
        lambda: {
            "deal_count": 0,
            "capital_deployed": 0.0,
            "total_value": 0.0,
            "moics": [],
            "hit_count": 0,
            "loss_count": 0,
        }
    )

    for d in deals:
        partner = d.lead_partner or "Unassigned"
        m = metrics_by_id[d.id]
        moic = m.get("moic")
        equity = m.get("equity") or 0.0
        value_total = m.get("value_total") or 0.0

        row = by_partner[partner]
        row["deal_count"] += 1
        row["capital_deployed"] += equity
        row["total_value"] += value_total

        if moic is not None:
            row["moics"].append(moic)
            if moic >= 2.0:
                row["hit_count"] += 1
            if moic < 1.0:
                row["loss_count"] += 1

    scorecard = []
    for partner, row in by_partner.items():
        moic_count = len(row["moics"])
        scorecard.append(
            {
                "lead_partner": partner,
                "deal_count": row["deal_count"],
                "capital_deployed": row["capital_deployed"],
                "weighted_moic": safe_divide(row["total_value"], row["capital_deployed"]),
                "hit_rate": safe_divide(row["hit_count"], moic_count) if moic_count else None,
                "loss_ratio": safe_divide(row["loss_count"], moic_count) if moic_count else None,
            }
        )

    return sorted(scorecard, key=lambda x: x["capital_deployed"] or 0.0, reverse=True)

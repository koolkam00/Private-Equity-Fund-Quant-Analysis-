"""Portfolio aggregation analytics for deal-only dashboards."""

from __future__ import annotations

from collections import defaultdict

from services.metrics.bridge import DRIVERS
from services.metrics.deal import compute_bridge_view, compute_deal_metrics
from services.metrics.common import EPS, safe_divide


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
        "tev_ebitda": _metric_aggregate(metrics, "entry_tev_ebitda"),
        "tev_revenue": _metric_aggregate(metrics, "entry_tev_revenue"),
        "net_debt_ebitda": _metric_aggregate(metrics, "entry_net_debt_ebitda"),
        "net_debt_tev": _metric_aggregate(metrics, "entry_net_debt_tev"),
        "ebitda_margin": _metric_aggregate(metrics, "entry_ebitda_margin"),
    }

    exit_ = {
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


def compute_bridge_aggregate(deals, model="additive", basis="fund"):
    sums = {k: 0.0 for k in DRIVERS}
    ready_count = 0
    low_confidence_count = 0
    total_equity = 0.0
    total_value_created = 0.0

    for d in deals:
        warnings = []
        bridge = compute_bridge_view(d, model=model, basis=basis, unit="dollar", warnings=warnings)
        eq = d.equity_invested or 0
        total_equity += eq

        if basis == "fund":
            total_value_created += (d.realized_value or 0) + (d.unrealized_value or 0) - eq
        else:
            # For company basis aggregate, use bridge-provided company value when available.
            if bridge.get("company_value_created") is not None:
                total_value_created += bridge.get("company_value_created")

        if bridge.get("low_confidence_bridge"):
            low_confidence_count += 1

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

    return {
        "model": model,
        "basis": basis,
        "ready_count": ready_count,
        "low_confidence_count": low_confidence_count,
        "drivers": {
            "dollar": sums,
            "moic": moic,
            "pct": pct,
        },
        "total_value_created": total_value_created,
        "total_equity": total_equity,
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

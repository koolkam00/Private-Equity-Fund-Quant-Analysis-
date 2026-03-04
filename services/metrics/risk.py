"""Loss and distribution analytics for deal-only data."""

from __future__ import annotations

from services.metrics.common import safe_divide


def compute_loss_and_distribution(deals, metrics_by_id=None):
    positive_equity_deals = [d for d in deals if d.equity_invested and d.equity_invested > 0]
    deals_with_moic = []

    for d in positive_equity_deals:
        if metrics_by_id is not None:
            moic = metrics_by_id[d.id].get("moic")
        else:
            moic = safe_divide((d.realized_value or 0) + (d.unrealized_value or 0), d.equity_invested)
        if moic is not None:
            deals_with_moic.append((d, moic))

    total_positive_equity = sum(d.equity_invested for d in positive_equity_deals)
    loss_ratio_capital = safe_divide(
        sum(
            max((d.equity_invested or 0) - ((d.realized_value or 0) + (d.unrealized_value or 0)), 0)
            for d, m in deals_with_moic
            if m < 1.0
        ),
        total_positive_equity,
    )
    if loss_ratio_capital is not None:
        loss_ratio_capital *= 100

    if deals_with_moic:
        losses = [(d, m) for d, m in deals_with_moic if m < 1.0]
        loss_ratio_count = len(losses) / len(deals_with_moic) * 100
    else:
        losses = []
        loss_ratio_count = None

    buckets = [
        ("<0.5x", 0, 0.5),
        ("0.5-1.0x", 0.5, 1.0),
        ("1.0-1.5x", 1.0, 1.5),
        ("1.5-2.0x", 1.5, 2.0),
        ("2.0-3.0x", 2.0, 3.0),
        ("3.0x+", 3.0, float("inf")),
    ]

    distribution = []
    for label, lo, hi in buckets:
        count = sum(1 for _, m in deals_with_moic if lo <= m < hi)
        pct = count / len(deals_with_moic) * 100 if deals_with_moic else 0
        distribution.append({"label": label, "count": count, "pct": pct})

    return {
        "loss_ratios": {
            "count_pct": loss_ratio_count,
            "capital_pct": loss_ratio_capital,
            "loss_count": len(losses),
            "total_count": len(deals_with_moic),
        },
        "moic_distribution": distribution,
    }

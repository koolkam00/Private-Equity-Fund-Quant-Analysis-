"""Data quality helpers for deal-only dashboards."""

from __future__ import annotations


BRIDGE_FIELDS = [
    "entry_revenue",
    "entry_ebitda",
    "entry_enterprise_value",
    "entry_net_debt",
    "exit_revenue",
    "exit_ebitda",
    "exit_enterprise_value",
    "exit_net_debt",
]


def compute_data_quality(deals, metrics_by_id):
    warnings = []
    for d in deals:
        m = metrics_by_id.get(d.id)
        if m:
            warnings.extend(m.get("_warnings", []))

    return {
        "total_deals": len(deals),
        "complete_deals": sum(1 for d in deals if d.equity_invested is not None and d.equity_invested > 0),
        "bridge_ready": sum(1 for d in deals if all(getattr(d, f) is not None for f in BRIDGE_FIELDS)),
        "warnings": warnings,
    }

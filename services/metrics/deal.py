"""Deal-level return, operating, and bridge analytics (deal-only)."""

from __future__ import annotations

from services.metrics.bridge import (
    compute_additive_bridge,
    compute_bridge_diagnostics,
)
from services.metrics.common import deal_hold_years, safe_divide, safe_power


def _implied_irr(moic, hold_years):
    if moic is None or hold_years is None or hold_years <= 0 or moic <= 0:
        return None
    root = safe_power(moic, 1.0 / hold_years)
    if root is None:
        return None
    return root - 1


def _growth_pct(exit_value, entry_value):
    if entry_value is None or exit_value is None or entry_value == 0:
        return None
    out = safe_divide(exit_value - entry_value, entry_value)
    return out * 100 if out is not None else None


def _cagr_pct(exit_value, entry_value, hold_years):
    if (
        entry_value is None
        or exit_value is None
        or hold_years is None
        or hold_years <= 0
        or entry_value <= 0
        or exit_value <= 0
    ):
        return None
    root = safe_power(exit_value / entry_value, 1.0 / hold_years)
    if root is None:
        return None
    return (root - 1) * 100


def compute_bridge_view(deal, model="additive", basis="fund", unit="dollar", warnings=None):
    warnings = warnings if warnings is not None else []
    if model not in {"additive", None}:
        raise ValueError("Only additive bridge model is supported.")
    return compute_additive_bridge(deal, warnings=warnings, basis=basis, unit=unit)


def compute_deal_metrics(deal, as_of_date=None):
    m = {"_warnings": []}

    equity = deal.equity_invested or 0
    realized = deal.realized_value or 0
    unrealized = deal.unrealized_value or 0

    m["equity"] = equity
    m["realized"] = realized
    m["unrealized"] = unrealized
    m["value_total"] = realized + unrealized
    m["value_created"] = m["value_total"] - equity

    m["moic"] = safe_divide(m["value_total"], equity)
    m["realized_moic"] = safe_divide(realized, equity)
    m["unrealized_moic"] = safe_divide(unrealized, equity)

    m["hold_period"] = deal_hold_years(deal, as_of_date=as_of_date)
    m["implied_irr"] = _implied_irr(m["moic"], m["hold_period"])

    if deal.equity_invested is not None and deal.equity_invested < 0:
        m["_warnings"].append("Negative equity invested")

    # Growth
    m["revenue_growth"] = _growth_pct(deal.exit_revenue, deal.entry_revenue)
    m["ebitda_growth"] = _growth_pct(deal.exit_ebitda, deal.entry_ebitda)
    m["revenue_cagr"] = _cagr_pct(deal.exit_revenue, deal.entry_revenue, m["hold_period"])
    m["ebitda_cagr"] = _cagr_pct(deal.exit_ebitda, deal.entry_ebitda, m["hold_period"])

    # Raw entry/exit operating values
    m["entry_revenue"] = deal.entry_revenue
    m["entry_ebitda"] = deal.entry_ebitda
    m["entry_enterprise_value"] = deal.entry_enterprise_value
    m["entry_net_debt"] = deal.entry_net_debt
    m["exit_revenue"] = deal.exit_revenue
    m["exit_ebitda"] = deal.exit_ebitda
    m["exit_enterprise_value"] = deal.exit_enterprise_value
    m["exit_net_debt"] = deal.exit_net_debt

    # Entry ratios
    m["entry_tev_ebitda"] = safe_divide(deal.entry_enterprise_value, deal.entry_ebitda)
    m["entry_tev_revenue"] = safe_divide(deal.entry_enterprise_value, deal.entry_revenue)
    m["entry_net_debt_ebitda"] = safe_divide(deal.entry_net_debt, deal.entry_ebitda)
    m["entry_net_debt_tev"] = safe_divide(deal.entry_net_debt, deal.entry_enterprise_value)

    # Exit ratios
    m["exit_tev_ebitda"] = safe_divide(deal.exit_enterprise_value, deal.exit_ebitda)
    m["exit_tev_revenue"] = safe_divide(deal.exit_enterprise_value, deal.exit_revenue)
    m["exit_net_debt_ebitda"] = safe_divide(deal.exit_net_debt, deal.exit_ebitda)
    m["exit_net_debt_tev"] = safe_divide(deal.exit_net_debt, deal.exit_enterprise_value)

    # Margin levels (entry/exit)
    m["entry_ebitda_margin"] = safe_divide(deal.entry_ebitda, deal.entry_revenue)
    m["exit_ebitda_margin"] = safe_divide(deal.exit_ebitda, deal.exit_revenue)
    if m["entry_ebitda_margin"] is not None:
        m["entry_ebitda_margin"] *= 100
    if m["exit_ebitda_margin"] is not None:
        m["exit_ebitda_margin"] *= 100

    if m["moic"] is not None and m["moic"] > 100:
        m["_warnings"].append(f"MOIC {m['moic']:.1f}x appears implausible")

    additive_fund = compute_additive_bridge(deal, warnings=m["_warnings"], basis="fund", unit="dollar")

    m["bridge_ready"] = bool(additive_fund.get("ready"))
    m["bridge_additive_fund"] = additive_fund
    m["bridge_diagnostics"] = compute_bridge_diagnostics(additive_fund)

    return m

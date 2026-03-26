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
    # Guard against extremely short hold periods that produce meaningless IRRs.
    # 30 days ≈ 0.082 years is the minimum for a credible annualized return.
    if hold_years < 30 / 365.25:
        return None
    root = safe_power(moic, 1.0 / hold_years)
    if root is None:
        return None
    irr = root - 1
    # Cap implied IRR at 10,000% (100x) to flag data issues rather than
    # propagating implausible values through analytics.
    if irr is not None and abs(irr) > 100:
        return None
    return irr


def _growth_pct(exit_value, entry_value):
    if entry_value is None or exit_value is None or entry_value == 0:
        return None
    # For negative starting values, use absolute entry base so improvements
    # (moving toward or above zero) have intuitive positive sign.
    denom = abs(entry_value) if entry_value < 0 else entry_value
    out = safe_divide(exit_value - entry_value, denom)
    return out * 100 if out is not None else None


def _cagr_pct(exit_value, entry_value, hold_years):
    """Annualized compound growth rate between entry and exit values.

    Methodology for negative base values (e.g. negative EBITDA):
    - Negative→negative: CAGR is computed on the absolute magnitude ratio
      inverted (|entry|/|exit|), so loss reduction yields positive CAGR.
      Example: EBITDA from -50 to -10 over 3y → CAGR = (50/10)^(1/3)-1 ≈ 71%.
    - Sign-flip (positive→negative or vice versa): returns None because
      compounding is undefined across sign changes.

    This convention is standard in PE for tracking operational turnarounds
    where EBITDA remains negative but is improving toward breakeven.
    """
    if (
        entry_value is None
        or exit_value is None
        or hold_years is None
        or hold_years <= 0
        or entry_value == 0
        or exit_value == 0
    ):
        return None

    if entry_value > 0 and exit_value > 0:
        ratio = exit_value / entry_value
    elif entry_value < 0 and exit_value < 0:
        ratio = abs(entry_value) / abs(exit_value)
    else:
        # Sign-flip trajectories are not compounding-comparable.
        return None

    root = safe_power(ratio, 1.0 / hold_years)
    if root is None:
        return None
    return (root - 1) * 100


def _non_negative_multiple(value, warnings, label):
    if value is None:
        return None
    if value < 0:
        warnings.append(f"{label} is negative; treated as unavailable.")
        return None
    return value


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
    if m["hold_period"] is not None and m["hold_period"] > 20:
        m["_warnings"].append(f"Hold period of {m['hold_period']:.1f} years appears unusually long")
    m["implied_irr"] = _implied_irr(m["moic"], m["hold_period"])
    # Primary IRR for analytics/reporting is uploaded gross IRR from the deal sheet.
    m["gross_irr"] = deal.irr

    if deal.equity_invested is not None and deal.equity_invested < 0:
        m["_warnings"].append("Negative equity invested")
    if deal.realized_value is not None and deal.realized_value < 0:
        m["_warnings"].append("Negative realized value")
    if deal.unrealized_value is not None and deal.unrealized_value < 0:
        m["_warnings"].append("Negative unrealized value")

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

    # Acquired / bolt-on metrics and organic derivations
    m["acquired_revenue"] = getattr(deal, "acquired_revenue", None)
    m["acquired_ebitda"] = getattr(deal, "acquired_ebitda", None)
    m["acquired_tev"] = getattr(deal, "acquired_tev", None)

    # Classify deal's acquisition data status for UI completeness indicator
    has_any_acquired = any(
        v is not None and v != 0
        for v in (m["acquired_revenue"], m["acquired_ebitda"], m["acquired_tev"])
    )
    m["acquired_data_status"] = "acquired_data_provided" if has_any_acquired else "data_not_provided"

    # Revenue: organic vs acquired decomposition
    if deal.exit_revenue is not None and deal.entry_revenue is not None:
        acq_rev = m["acquired_revenue"] or 0.0
        m["total_revenue_growth"] = deal.exit_revenue - deal.entry_revenue
        m["acquired_revenue_contribution"] = acq_rev
        m["organic_revenue_growth"] = m["total_revenue_growth"] - acq_rev
        m["organic_revenue_pct"] = safe_divide(m["organic_revenue_growth"], m["total_revenue_growth"])
        m["acquired_revenue_pct"] = safe_divide(acq_rev, m["total_revenue_growth"])
    else:
        m["total_revenue_growth"] = None
        m["acquired_revenue_contribution"] = None
        m["organic_revenue_growth"] = None
        m["organic_revenue_pct"] = None
        m["acquired_revenue_pct"] = None

    # EBITDA: organic vs acquired decomposition
    if deal.exit_ebitda is not None and deal.entry_ebitda is not None:
        acq_ebitda = m["acquired_ebitda"] or 0.0
        m["total_ebitda_growth"] = deal.exit_ebitda - deal.entry_ebitda
        m["acquired_ebitda_contribution"] = acq_ebitda
        m["organic_ebitda_growth"] = m["total_ebitda_growth"] - acq_ebitda
        m["organic_ebitda_pct"] = safe_divide(m["organic_ebitda_growth"], m["total_ebitda_growth"])
        m["acquired_ebitda_pct"] = safe_divide(acq_ebitda, m["total_ebitda_growth"])
    else:
        m["total_ebitda_growth"] = None
        m["acquired_ebitda_contribution"] = None
        m["organic_ebitda_growth"] = None
        m["organic_ebitda_pct"] = None
        m["acquired_ebitda_pct"] = None

    # Organic CAGR: compound growth of (exit - acquired) from entry base
    # Note: assumes acquired revenue existed for full hold period. This is a
    # simplification for cumulative bolt-on data; see methodology notes in UI.
    hold = m["hold_period"]
    if deal.exit_revenue is not None and deal.entry_revenue is not None and hold:
        organic_exit_rev = deal.exit_revenue - (m["acquired_revenue"] or 0.0)
        m["organic_revenue_cagr"] = _cagr_pct(organic_exit_rev, deal.entry_revenue, hold)
    else:
        m["organic_revenue_cagr"] = None

    if deal.exit_ebitda is not None and deal.entry_ebitda is not None and hold:
        organic_exit_ebitda = deal.exit_ebitda - (m["acquired_ebitda"] or 0.0)
        m["organic_ebitda_cagr"] = _cagr_pct(organic_exit_ebitda, deal.entry_ebitda, hold)
    else:
        m["organic_ebitda_cagr"] = None

    # Entry ratios
    m["entry_tev_ebitda"] = _non_negative_multiple(
        safe_divide(deal.entry_enterprise_value, deal.entry_ebitda),
        m["_warnings"],
        "Entry TEV/EBITDA",
    )
    m["entry_tev_revenue"] = safe_divide(deal.entry_enterprise_value, deal.entry_revenue)
    m["entry_net_debt_ebitda"] = safe_divide(deal.entry_net_debt, deal.entry_ebitda)
    m["entry_net_debt_tev"] = safe_divide(deal.entry_net_debt, deal.entry_enterprise_value)

    # Exit ratios
    m["exit_tev_ebitda"] = _non_negative_multiple(
        safe_divide(deal.exit_enterprise_value, deal.exit_ebitda),
        m["_warnings"],
        "Exit TEV/EBITDA",
    )
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

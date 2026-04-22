"""Value Creation Analysis with add-on attribution payload builder."""

from __future__ import annotations

from collections import defaultdict
from statistics import median

from services.metrics.common import resolve_analysis_as_of_date, safe_divide, safe_power
from services.metrics.deal import compute_deal_metrics
from services.metrics.vca_ebitda import (
    DISPLAY_PERCENT_UNITS,
    STATUS_INDEX,
    _date_sort_value,
    _dominant_status_rank,
    _fund_net_performance,
    _fund_sort_key,
    _mean,
    _normalize_status,
    _overall_subtotal_deal_sets,
    _reconciled_display_percent_units,
    _subtotal_deal_sets,
    _weighted_average,
)

COLUMN_KEYS = (
    "row_num",
    "platform",
    "sector",
    "geography",
    "exit_type",
    "close_date",
    "final_exit_date",
    "hold_period",
    "status",
    "fund_initial_cost",
    "fund_total_cost",
    "realized_proceeds",
    "unrealized_value",
    "total_value",
    "gross_profit",
    "gross_profit_pct_of_total",
    "gross_irr",
    "realized_moic",
    "gross_moic",
    "organic_ebitda_cagr",
    "organic_ebitda_cumulative_growth",
    "vc_organic_ebitda_growth_pct",
    "vc_add_on_ebitda_pct",
    "vc_multiple_pct",
    "vc_debt_pct",
    "vc_total_pct",
    "vc_organic_ebitda_growth_dollar",
    "vc_add_on_ebitda_dollar",
    "vc_multiple_dollar",
    "vc_debt_dollar",
    "vc_total_dollar",
    "entry_ltm_ebitda",
    "entry_ebitda_margin",
    "entry_ev_ebitda",
    "entry_net_debt",
    "entry_net_debt_ebitda",
    "entry_net_debt_ev",
    "acquired_ebitda",
    "acquired_ev_ebitda",
    "blended_ev_ebitda_with_addons",
    "exit_ltm_ebitda",
    "exit_ebitda_margin",
    "exit_ev_ebitda",
    "exit_net_debt",
    "exit_net_debt_ebitda",
    "exit_net_debt_ev",
    "diff_ebitda",
    "diff_ebitda_margin",
    "diff_ev_ebitda",
    "diff_net_debt",
    "diff_net_debt_ebitda",
    "diff_net_debt_ev",
)

NUMERIC_SUMMARY_KEYS = tuple(key for key in COLUMN_KEYS if key not in {"row_num", "platform", "sector", "geography", "exit_type", "close_date", "final_exit_date", "status"})

VCA_COLUMNS = [
    {"key": "row_num", "label": "#", "numeric": True},
    {"key": "platform", "label": "Platform", "numeric": False},
    {"key": "sector", "label": "Sector", "numeric": False},
    {"key": "geography", "label": "Geography", "numeric": False},
    {"key": "exit_type", "label": "Exit Type", "numeric": False},
    {"key": "close_date", "label": "Close Date", "numeric": False},
    {"key": "final_exit_date", "label": "Final Sale Date", "numeric": False},
    {"key": "hold_period", "label": "Hold Period*", "numeric": True},
    {"key": "status", "label": "Status", "numeric": False},
    {"key": "fund_initial_cost", "label": "Fund Initial Cost", "numeric": True},
    {"key": "fund_total_cost", "label": "Fund Total Cost", "numeric": True},
    {"key": "realized_proceeds", "label": "Realized Proceeds", "numeric": True},
    {"key": "unrealized_value", "label": "Unrealized Value", "numeric": True},
    {"key": "total_value", "label": "Total Value", "numeric": True},
    {"key": "gross_profit", "label": "Gross Profit", "numeric": True},
    {"key": "gross_profit_pct_of_total", "label": "Gross Profit % of Total", "numeric": True},
    {"key": "gross_irr", "label": "Gross IRR", "numeric": True},
    {"key": "realized_moic", "label": "Realized MOIC", "numeric": True},
    {"key": "gross_moic", "label": "Gross MOIC", "numeric": True},
    {"key": "organic_ebitda_cagr", "label": "Organic EBITDA CAGR*", "numeric": True},
    {"key": "organic_ebitda_cumulative_growth", "label": "Organic EBITDA Cumulative Growth*", "numeric": True},
    {"key": "vc_organic_ebitda_growth_pct", "label": "Organic EBITDA Growth", "numeric": True},
    {"key": "vc_add_on_ebitda_pct", "label": "Add-On EBITDA", "numeric": True},
    {"key": "vc_multiple_pct", "label": "Multiple Expansion", "numeric": True},
    {"key": "vc_debt_pct", "label": "Debt Reduction/(Increase)", "numeric": True},
    {"key": "vc_total_pct", "label": "Total", "numeric": True},
    {"key": "vc_organic_ebitda_growth_dollar", "label": "Organic EBITDA Growth", "numeric": True},
    {"key": "vc_add_on_ebitda_dollar", "label": "Add-On EBITDA", "numeric": True},
    {"key": "vc_multiple_dollar", "label": "Multiple Expansion", "numeric": True},
    {"key": "vc_debt_dollar", "label": "Debt Reduction/(Increase)", "numeric": True},
    {"key": "vc_total_dollar", "label": "Total", "numeric": True},
    {"key": "entry_ltm_ebitda", "label": "Entry LTM EBITDA", "numeric": True},
    {"key": "entry_ebitda_margin", "label": "Entry EBITDA Margin", "numeric": True},
    {"key": "entry_ev_ebitda", "label": "Entry EV/EBITDA*", "numeric": True},
    {"key": "entry_net_debt", "label": "Entry Net Debt", "numeric": True},
    {"key": "entry_net_debt_ebitda", "label": "Entry Net Debt/EBITDA*", "numeric": True},
    {"key": "entry_net_debt_ev", "label": "Entry Net Debt/EV", "numeric": True},
    {"key": "acquired_ebitda", "label": "Acquired EBITDA", "numeric": True},
    {"key": "acquired_ev_ebitda", "label": "Acquired EV/EBITDA*", "numeric": True},
    {"key": "blended_ev_ebitda_with_addons", "label": "Blended EV/EBITDA With Add-Ons", "numeric": True},
    {"key": "exit_ltm_ebitda", "label": "Exit/Current LTM EBITDA", "numeric": True},
    {"key": "exit_ebitda_margin", "label": "Exit/Current EBITDA Margin", "numeric": True},
    {"key": "exit_ev_ebitda", "label": "Exit/Current EV/EBITDA*", "numeric": True},
    {"key": "exit_net_debt", "label": "Exit/Current Net Debt", "numeric": True},
    {"key": "exit_net_debt_ebitda", "label": "Exit/Current Net Debt/EBITDA*", "numeric": True},
    {"key": "exit_net_debt_ev", "label": "Exit/Current Net Debt/EV", "numeric": True},
    {"key": "diff_ebitda", "label": "Difference EBITDA", "numeric": True},
    {"key": "diff_ebitda_margin", "label": "Difference EBITDA Margin", "numeric": True},
    {"key": "diff_ev_ebitda", "label": "Difference EV/EBITDA*", "numeric": True},
    {"key": "diff_net_debt", "label": "Difference Net Debt", "numeric": True},
    {"key": "diff_net_debt_ebitda", "label": "Difference Net Debt/EBITDA*", "numeric": True},
    {"key": "diff_net_debt_ev", "label": "Difference Net Debt/EV", "numeric": True},
]

VCA_HEADER_GROUPS = [
    {"label": "Deal Profile", "span": 9},
    {"label": "Fund Performance", "span": 10},
    {"label": "Organic EBITDA Growth During Hold Period", "span": 2},
    {"label": "Value Creation (%)", "span": 5},
    {"label": "Value Creation ($)", "span": 5},
    {"label": "Company Op. Metrics At Entry", "span": 6},
    {"label": "Add-Ons", "span": 2},
    {"label": "Blended EV/EBITDA With Add-Ons", "span": 1},
    {"label": "Company Op. Metrics At Exit / Current", "span": 6},
    {"label": "Difference Exit/Current vs Entry", "span": 6},
]

FORMULA_LEGEND = {
    "hold_period": "(Exit - Close) / 365.25",
    "total_value": "C = A + B",
    "gross_profit": "D = C - Cost",
    "gross_profit_pct_of_total": "E = D / Total GP",
    "realized_moic": "F = Realized / Cost",
    "gross_moic": "G = Total / Cost",
    "organic_ebitda_cagr": "H = ((Organic Exit EBITDA/Entry EBITDA)^(1/Hold))-1",
    "organic_ebitda_cumulative_growth": "I = Organic Exit EBITDA/Entry EBITDA - 1",
    "vc_organic_ebitda_growth_pct": "J = Organic EBITDA $ / D",
    "vc_add_on_ebitda_pct": "K = Add-On EBITDA $ / D",
    "vc_multiple_pct": "L = Multiple $ / D",
    "vc_debt_pct": "M = Debt $ / D",
    "vc_total_pct": "N = J + K + L + M",
    "vc_total_dollar": "S = O + P + Q + R",
    "acquired_ev_ebitda": "Acq TEV / Acq EBITDA",
    "blended_ev_ebitda_with_addons": "(Entry TEV + Acq TEV) / (Entry EBITDA + Acq EBITDA)",
    "diff_ebitda": "Exit - Entry",
    "diff_ebitda_margin": "Exit - Entry",
    "diff_ev_ebitda": "Exit - Entry",
    "diff_net_debt": "Exit - Entry",
    "diff_net_debt_ebitda": "Exit - Entry",
    "diff_net_debt_ev": "Exit - Entry",
}


def _blank_vca_row(row_kind="detail", platform=None):
    row = {key: None for key in COLUMN_KEYS}
    row["platform"] = platform
    row["row_kind"] = row_kind
    return row


def _ordered_deals_for_fund(deals):
    return sorted(
        deals,
        key=lambda d: (
            STATUS_INDEX.get(_normalize_status(d.status), 99),
            d.investment_date is None,
            _date_sort_value(d.investment_date),
            (d.company_name or "").lower(),
            d.id,
        ),
    )


def _sum_present(values):
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present)


def _difference(exit_value, entry_value):
    if exit_value is None or entry_value is None:
        return None
    return exit_value - entry_value


def _growth_pct(exit_value, entry_value):
    if entry_value is None or exit_value is None or entry_value == 0:
        return None
    denom = abs(entry_value) if entry_value < 0 else entry_value
    out = safe_divide(exit_value - entry_value, denom)
    return out * 100 if out is not None else None


def _cagr_pct(exit_value, entry_value, hold_years):
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
        return None
    root = safe_power(ratio, 1.0 / hold_years)
    return (root - 1) * 100 if root is not None else None


def _organic_exit_value(exit_value, acquired_value):
    if exit_value is None:
        return None
    return exit_value - (acquired_value or 0.0)


def _ownership_for_deal(deal, metric):
    bridge = metric.get("bridge_additive_fund") or {}
    ownership = bridge.get("ownership_pct")
    if ownership is not None:
        return ownership
    if getattr(deal, "ownership_pct", None) is not None and deal.ownership_pct >= 0:
        return deal.ownership_pct
    if (
        getattr(deal, "entry_enterprise_value", None) is not None
        and getattr(deal, "entry_net_debt", None) is not None
    ):
        entry_equity = deal.entry_enterprise_value - deal.entry_net_debt
        if entry_equity <= 0:
            return 1.0
        return safe_divide(getattr(deal, "equity_invested", None), entry_equity, default=1.0)
    return 1.0


def _normalize_displayed_vca_percentages(row):
    gross_profit = row.get("gross_profit")
    component_keys = (
        ("vc_organic_ebitda_growth_dollar", "vc_organic_ebitda_growth_pct"),
        ("vc_add_on_ebitda_dollar", "vc_add_on_ebitda_pct"),
        ("vc_multiple_dollar", "vc_multiple_pct"),
        ("vc_debt_dollar", "vc_debt_pct"),
    )
    if gross_profit in (None, 0):
        for _, pct_key in component_keys:
            row[pct_key] = None
        row["vc_total_pct"] = None
        return row

    component_dollars = [row.get(dollar_key) for dollar_key, _ in component_keys]
    if all(value is None for value in component_dollars):
        for _, pct_key in component_keys:
            row[pct_key] = None
        row["vc_total_pct"] = None
        return row

    raw_units = []
    for value in component_dollars:
        ratio = safe_divide(0.0 if value is None else value, gross_profit)
        raw_units.append((ratio or 0.0) * DISPLAY_PERCENT_UNITS)

    reconciled_units = _reconciled_display_percent_units(raw_units)
    for (_, pct_key), units in zip(component_keys, reconciled_units):
        row[pct_key] = units / DISPLAY_PERCENT_UNITS
    row["vc_total_pct"] = 1.0
    return row


def _clear_summary_value_creation_fields(row):
    for key in (
        "vc_organic_ebitda_growth_pct",
        "vc_add_on_ebitda_pct",
        "vc_multiple_pct",
        "vc_debt_pct",
        "vc_total_pct",
        "vc_organic_ebitda_growth_dollar",
        "vc_add_on_ebitda_dollar",
        "vc_multiple_dollar",
        "vc_debt_dollar",
        "vc_total_dollar",
    ):
        row[key] = None
    return row


def _add_on_bridge_values(deal, metric, gross_profit):
    bridge = metric.get("bridge_additive_fund") or {}
    drivers = bridge.get("drivers_dollar") or {}
    ownership = _ownership_for_deal(deal, metric)

    entry_multiple = metric.get("entry_tev_ebitda")
    organic_exit_ebitda = _organic_exit_value(metric.get("exit_ebitda"), metric.get("acquired_ebitda"))
    organic_growth = _difference(organic_exit_ebitda, metric.get("entry_ebitda"))
    acquired_ebitda = metric.get("acquired_ebitda") or 0.0

    organic_dollar = None
    add_on_dollar = None
    if entry_multiple is not None and entry_multiple >= 0 and ownership is not None:
        if organic_growth is not None:
            organic_dollar = organic_growth * entry_multiple * ownership
        add_on_dollar = acquired_ebitda * entry_multiple * ownership

    multiple_dollar = drivers.get("multiple")
    debt_dollar = drivers.get("leverage")
    if multiple_dollar is None and entry_multiple is not None and metric.get("exit_tev_ebitda") is not None:
        exit_ebitda = metric.get("exit_ebitda")
        multiple_dollar = (
            (metric["exit_tev_ebitda"] - entry_multiple) * exit_ebitda * ownership
            if exit_ebitda is not None and ownership is not None
            else None
        )
    if debt_dollar is None and deal.entry_net_debt is not None and deal.exit_net_debt is not None and ownership is not None:
        debt_dollar = (deal.entry_net_debt - deal.exit_net_debt) * ownership

    total_dollar = _sum_present([organic_dollar, add_on_dollar, multiple_dollar, debt_dollar])
    return {
        "vc_organic_ebitda_growth_dollar": organic_dollar,
        "vc_add_on_ebitda_dollar": add_on_dollar,
        "vc_multiple_dollar": multiple_dollar,
        "vc_debt_dollar": debt_dollar,
        "vc_total_dollar": total_dollar,
        "vc_organic_ebitda_growth_pct": safe_divide(organic_dollar, gross_profit),
        "vc_add_on_ebitda_pct": safe_divide(add_on_dollar, gross_profit),
        "vc_multiple_pct": safe_divide(multiple_dollar, gross_profit),
        "vc_debt_pct": safe_divide(debt_dollar, gross_profit),
        "vc_total_pct": safe_divide(total_dollar, gross_profit),
    }


def _row_operating_fields(row, metric):
    entry_revenue = metric.get("entry_revenue")
    entry_ebitda = metric.get("entry_ebitda")
    entry_tev = metric.get("entry_enterprise_value")
    entry_net_debt = metric.get("entry_net_debt")

    exit_revenue = metric.get("exit_revenue")
    exit_ebitda = metric.get("exit_ebitda")
    exit_tev = metric.get("exit_enterprise_value")
    exit_net_debt = metric.get("exit_net_debt")

    acquired_revenue = metric.get("acquired_revenue")
    acquired_ebitda = metric.get("acquired_ebitda")
    acquired_tev = metric.get("acquired_tev")

    row.update(
        {
            "entry_ltm_revenue": entry_revenue,
            "entry_ltm_ebitda": entry_ebitda,
            "entry_ebitda_margin": safe_divide(entry_ebitda, entry_revenue),
            "entry_tev": entry_tev,
            "entry_ev_revenue": safe_divide(entry_tev, entry_revenue),
            "entry_ev_ebitda": safe_divide(entry_tev, entry_ebitda),
            "entry_net_debt": entry_net_debt,
            "entry_net_debt_ebitda": safe_divide(entry_net_debt, entry_ebitda),
            "entry_net_debt_ev": safe_divide(entry_net_debt, entry_tev),
            "acquired_revenue": acquired_revenue,
            "acquired_ebitda": acquired_ebitda,
            "acquired_tev": acquired_tev,
            "acquired_ev_revenue": safe_divide(acquired_tev, acquired_revenue),
            "acquired_ev_ebitda": safe_divide(acquired_tev, acquired_ebitda),
            "blended_ev_ebitda_with_addons": safe_divide(
                _sum_present([entry_tev, acquired_tev]),
                _sum_present([entry_ebitda, acquired_ebitda]),
            ),
            "add_on_tev_pct_of_exit": safe_divide(acquired_tev, exit_tev),
            "exit_ltm_revenue": exit_revenue,
            "exit_ltm_ebitda": exit_ebitda,
            "exit_ebitda_margin": safe_divide(exit_ebitda, exit_revenue),
            "exit_tev": exit_tev,
            "exit_ev_revenue": safe_divide(exit_tev, exit_revenue),
            "exit_ev_ebitda": safe_divide(exit_tev, exit_ebitda),
            "exit_net_debt": exit_net_debt,
            "exit_net_debt_ebitda": safe_divide(exit_net_debt, exit_ebitda),
            "exit_net_debt_ev": safe_divide(exit_net_debt, exit_tev),
        }
    )

    row["diff_revenue"] = _difference(row["exit_ltm_revenue"], row["entry_ltm_revenue"])
    row["diff_ebitda"] = _difference(row["exit_ltm_ebitda"], row["entry_ltm_ebitda"])
    row["diff_ebitda_margin"] = _difference(row["exit_ebitda_margin"], row["entry_ebitda_margin"])
    row["diff_tev"] = _difference(row["exit_tev"], row["entry_tev"])
    row["diff_ev_revenue"] = _difference(row["exit_ev_revenue"], row["entry_ev_revenue"])
    row["diff_ev_ebitda"] = _difference(row["exit_ev_ebitda"], row["entry_ev_ebitda"])
    row["diff_net_debt"] = _difference(row["exit_net_debt"], row["entry_net_debt"])
    row["diff_net_debt_ebitda"] = _difference(row["exit_net_debt_ebitda"], row["entry_net_debt_ebitda"])
    row["diff_net_debt_ev"] = _difference(row["exit_net_debt_ev"], row["entry_net_debt_ev"])
    return row


def _aggregate_operating_fields(deals_subset):
    metrics = [
        {
            "entry_revenue": deal.entry_revenue,
            "entry_ebitda": deal.entry_ebitda,
            "entry_enterprise_value": deal.entry_enterprise_value,
            "entry_net_debt": deal.entry_net_debt,
            "exit_revenue": deal.exit_revenue,
            "exit_ebitda": deal.exit_ebitda,
            "exit_enterprise_value": deal.exit_enterprise_value,
            "exit_net_debt": deal.exit_net_debt,
            "acquired_revenue": getattr(deal, "acquired_revenue", None),
            "acquired_ebitda": getattr(deal, "acquired_ebitda", None),
            "acquired_tev": getattr(deal, "acquired_tev", None),
        }
        for deal in deals_subset
    ]
    row = {}
    for key in (
        "entry_revenue",
        "entry_ebitda",
        "entry_enterprise_value",
        "entry_net_debt",
        "exit_revenue",
        "exit_ebitda",
        "exit_enterprise_value",
        "exit_net_debt",
        "acquired_revenue",
        "acquired_ebitda",
        "acquired_tev",
    ):
        row[key] = _sum_present([metric.get(key) for metric in metrics])

    proxy_metric = {
        "entry_revenue": row["entry_revenue"],
        "entry_ebitda": row["entry_ebitda"],
        "entry_enterprise_value": row["entry_enterprise_value"],
        "entry_net_debt": row["entry_net_debt"],
        "exit_revenue": row["exit_revenue"],
        "exit_ebitda": row["exit_ebitda"],
        "exit_enterprise_value": row["exit_enterprise_value"],
        "exit_net_debt": row["exit_net_debt"],
        "acquired_revenue": row["acquired_revenue"],
        "acquired_ebitda": row["acquired_ebitda"],
        "acquired_tev": row["acquired_tev"],
    }
    output = {}
    _row_operating_fields(output, proxy_metric)
    return output


def build_vca_addon_row(deal, metric, row_num, gross_profit_denominator):
    row = _blank_vca_row(row_kind="deal", platform=deal.company_name or "Unknown")
    row["row_num"] = row_num
    row["sector"] = deal.sector
    row["geography"] = deal.geography
    row["exit_type"] = getattr(deal, "exit_type", None)
    row["close_date"] = deal.investment_date
    row["final_exit_date"] = deal.exit_date
    row["hold_period"] = metric.get("hold_period")
    row["status"] = _normalize_status(deal.status)

    row["fund_initial_cost"] = metric.get("equity")
    row["fund_total_cost"] = metric.get("equity")
    row["realized_proceeds"] = metric.get("realized")
    row["unrealized_value"] = metric.get("unrealized")
    row["total_value"] = metric.get("value_total")
    row["gross_profit"] = metric.get("value_created")
    row["gross_profit_pct_of_total"] = safe_divide(row["gross_profit"], gross_profit_denominator)

    row["gross_irr"] = deal.irr
    row["realized_moic"] = metric.get("realized_moic")
    row["gross_moic"] = metric.get("moic")

    organic_exit_ebitda = _organic_exit_value(metric.get("exit_ebitda"), metric.get("acquired_ebitda"))
    row["organic_ebitda_cagr"] = _cagr_pct(organic_exit_ebitda, metric.get("entry_ebitda"), metric.get("hold_period"))
    row["organic_ebitda_cumulative_growth"] = _growth_pct(organic_exit_ebitda, metric.get("entry_ebitda"))

    row.update(_add_on_bridge_values(deal, metric, row["gross_profit"]))
    _row_operating_fields(row, metric)
    return _normalize_displayed_vca_percentages(row)


def build_vca_addon_subtotal(label, deals_subset, metrics_by_id=None, gross_profit_denominator=None):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals_subset}
    if not deals_subset:
        return None

    row = _blank_vca_row(row_kind="subtotal", platform=label)
    component_rows = [
        build_vca_addon_row(deal, metrics_by_id[deal.id], idx, gross_profit_denominator)
        for idx, deal in enumerate(deals_subset, start=1)
    ]

    row["status"] = ""
    row["fund_initial_cost"] = _sum_present(r.get("fund_initial_cost") for r in component_rows)
    row["fund_total_cost"] = _sum_present(r.get("fund_total_cost") for r in component_rows)
    row["realized_proceeds"] = _sum_present(r.get("realized_proceeds") for r in component_rows)
    row["unrealized_value"] = _sum_present(r.get("unrealized_value") for r in component_rows)
    row["total_value"] = _sum_present(r.get("total_value") for r in component_rows)
    row["gross_profit"] = _sum_present(r.get("gross_profit") for r in component_rows)
    row["gross_profit_pct_of_total"] = safe_divide(row["gross_profit"], gross_profit_denominator)
    row["gross_irr"] = _weighted_average((r.get("gross_irr"), r.get("fund_total_cost")) for r in component_rows)
    row["realized_moic"] = safe_divide(row["realized_proceeds"], row["fund_total_cost"])
    row["gross_moic"] = safe_divide(row["total_value"], row["fund_total_cost"])
    row["hold_period"] = _weighted_average((r.get("hold_period"), r.get("fund_total_cost")) for r in component_rows)
    row["organic_ebitda_cagr"] = _weighted_average((r.get("organic_ebitda_cagr"), r.get("fund_total_cost")) for r in component_rows)
    row["organic_ebitda_cumulative_growth"] = _weighted_average((r.get("organic_ebitda_cumulative_growth"), r.get("fund_total_cost")) for r in component_rows)

    for key in (
        "vc_organic_ebitda_growth_dollar",
        "vc_add_on_ebitda_dollar",
        "vc_multiple_dollar",
        "vc_debt_dollar",
    ):
        row[key] = _sum_present(r.get(key) for r in component_rows)
    row["vc_total_dollar"] = _sum_present(
        row.get(key)
        for key in (
            "vc_organic_ebitda_growth_dollar",
            "vc_add_on_ebitda_dollar",
            "vc_multiple_dollar",
            "vc_debt_dollar",
        )
    )

    row.update(_aggregate_operating_fields(deals_subset))
    return _normalize_displayed_vca_percentages(row)


def build_vca_addon_summary_rows(deal_rows):
    output = []
    summary_modes = ("Average", "Median", "Weighted Average")

    for label in summary_modes:
        row = _blank_vca_row(row_kind="summary", platform=label)
        row["status"] = ""

        for key in NUMERIC_SUMMARY_KEYS:
            values = [r.get(key) for r in deal_rows if r.get(key) is not None]
            if label == "Average":
                row[key] = _mean(values)
                continue
            if label == "Median":
                row[key] = median(values) if values else None
                continue
            weighted_pairs = [(r.get(key), r.get("fund_total_cost")) for r in deal_rows if r.get(key) is not None]
            row[key] = _weighted_average(weighted_pairs)

        output.append(_clear_summary_value_creation_fields(row))

    return output


def compute_vca_addons_analysis(deals, metrics_by_id=None):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals}
    by_fund = defaultdict(list)

    for deal in deals:
        fund_name = deal.fund_number or "Unknown Fund"
        by_fund[fund_name].append(deal)

    grand_gross_profit = sum((metrics_by_id[deal.id].get("value_created") or 0.0) for deal in deals)
    fund_blocks = []
    all_deal_rows = []

    for fund_name in sorted(by_fund.keys(), key=_fund_sort_key):
        ordered_deals = _ordered_deals_for_fund(by_fund[fund_name])
        fund_gross_profit = sum((metrics_by_id[deal.id].get("value_created") or 0.0) for deal in ordered_deals)

        fund_size_values = [deal.fund_size for deal in ordered_deals if deal.fund_size is not None]
        fund_size = fund_size_values[0] if fund_size_values else None
        fund_size_conflict = any(abs(value - fund_size) > 1e-9 for value in fund_size_values[1:]) if fund_size is not None else False

        deal_rows = []
        for idx, deal in enumerate(ordered_deals, start=1):
            row = build_vca_addon_row(
                deal,
                metrics_by_id[deal.id],
                row_num=idx,
                gross_profit_denominator=fund_gross_profit,
            )
            row["deal_id"] = deal.id
            deal_rows.append(row)

        subtotal_rows = []
        for label, subset in _subtotal_deal_sets(fund_name, ordered_deals):
            subset_metrics = {deal.id: metrics_by_id[deal.id] for deal in subset}
            subtotal = build_vca_addon_subtotal(
                label,
                subset,
                metrics_by_id=subset_metrics,
                gross_profit_denominator=fund_gross_profit,
            )
            if subtotal is not None:
                subtotal_rows.append(subtotal)

        summary_rows = build_vca_addon_summary_rows(deal_rows)
        all_subtotal = subtotal_rows[-1] if subtotal_rows else None

        fund_blocks.append(
            {
                "fund_name": fund_name,
                "fund_size": fund_size,
                "fund_size_conflict": fund_size_conflict,
                "net_performance": _fund_net_performance(ordered_deals),
                "print_sort_metrics": {
                    "gross_profit": all_subtotal.get("gross_profit") if all_subtotal else None,
                    "gross_moic": all_subtotal.get("gross_moic") if all_subtotal else None,
                    "gross_irr": all_subtotal.get("gross_irr") if all_subtotal else None,
                    "status_rank": _dominant_status_rank(deal_rows),
                    "fund_name_norm": (fund_name or "").lower(),
                },
                "deal_rows": deal_rows,
                "subtotal_rows": subtotal_rows,
                "summary_rows": summary_rows,
            }
        )
        all_deal_rows.extend(deal_rows)

    ordered_all_deals = []
    for fund_name in sorted(by_fund.keys(), key=_fund_sort_key):
        ordered_all_deals.extend(_ordered_deals_for_fund(by_fund[fund_name]))

    overall_subtotals = []
    for label, subset in _overall_subtotal_deal_sets(ordered_all_deals):
        subset_metrics = {deal.id: metrics_by_id[deal.id] for deal in subset}
        subtotal = build_vca_addon_subtotal(
            label,
            subset,
            metrics_by_id=subset_metrics,
            gross_profit_denominator=grand_gross_profit,
        )
        if subtotal is not None:
            overall_subtotals.append(subtotal)

    overall_summary = build_vca_addon_summary_rows(all_deal_rows)
    grand_total_row = overall_subtotals[-1] if overall_subtotals else None
    formula_row = [FORMULA_LEGEND.get(column["key"], "") for column in VCA_COLUMNS]

    return {
        "meta": {
            "title": "Value Creation Analysis - with Add-Ons",
            "as_of_date": resolve_analysis_as_of_date(deals),
            "currency_unit_label": "USD $M",
            "footnotes": [
                "* Hold period and CAGR calculations use year fractions with a 365.25-day basis.",
                "Organic EBITDA excludes uploaded Acquired EBITDA from Exit/Current EBITDA before calculating organic growth.",
                "Add-On EBITDA value creation uses uploaded Acquired EBITDA at the entry EV/EBITDA multiple and fund ownership.",
                "Acquired EBITDA and Acquired EV/EBITDA use uploaded Acquired EBITDA and Acquired TEV.",
                "Fund Initial Cost mirrors Fund Total Cost until a separate add-on equity cost upload field is available.",
            ],
            "formula_legend": FORMULA_LEGEND,
        },
        "header": {
            "groups": VCA_HEADER_GROUPS,
            "columns": VCA_COLUMNS,
            "formula_row": formula_row,
        },
        "fund_blocks": fund_blocks,
        "overall_block": {
            "subtotal_rows": overall_subtotals,
            "summary_rows": overall_summary,
            "summary_metrics": {
                "gross_profit": grand_total_row.get("gross_profit") if grand_total_row else None,
                "gross_moic": grand_total_row.get("gross_moic") if grand_total_row else None,
                "gross_irr": grand_total_row.get("gross_irr") if grand_total_row else None,
                "deal_count": len(ordered_all_deals),
                "fund_count": len(fund_blocks),
            },
        },
    }


REVENUE_COLUMN_KEYS = (
    "row_num",
    "platform",
    "sector",
    "geography",
    "exit_type",
    "close_date",
    "final_exit_date",
    "hold_period",
    "status",
    "fund_initial_cost",
    "fund_total_cost",
    "realized_proceeds",
    "unrealized_value",
    "total_value",
    "gross_profit",
    "gross_profit_pct_of_total",
    "gross_irr",
    "realized_moic",
    "gross_moic",
    "organic_revenue_cagr",
    "organic_revenue_cumulative_growth",
    "vc_organic_revenue_growth_pct",
    "vc_add_on_revenue_pct",
    "vc_multiple_pct",
    "vc_debt_pct",
    "vc_total_pct",
    "vc_organic_revenue_growth_dollar",
    "vc_add_on_revenue_dollar",
    "vc_multiple_dollar",
    "vc_debt_dollar",
    "vc_total_dollar",
    "entry_ltm_revenue",
    "entry_ev_revenue",
    "entry_net_debt",
    "entry_net_debt_revenue",
    "entry_net_debt_ev",
    "acquired_revenue",
    "acquired_ev_revenue",
    "blended_ev_revenue_with_addons",
    "exit_ltm_revenue",
    "exit_ev_revenue",
    "exit_net_debt",
    "exit_net_debt_revenue",
    "exit_net_debt_ev",
    "diff_revenue",
    "diff_ev_revenue",
    "diff_net_debt",
    "diff_net_debt_revenue",
    "diff_net_debt_ev",
)

REVENUE_NUMERIC_SUMMARY_KEYS = tuple(
    key
    for key in REVENUE_COLUMN_KEYS
    if key not in {"row_num", "platform", "sector", "geography", "exit_type", "close_date", "final_exit_date", "status"}
)

REVENUE_VCA_COLUMNS = [
    {"key": "row_num", "label": "#", "numeric": True},
    {"key": "platform", "label": "Platform", "numeric": False},
    {"key": "sector", "label": "Sector", "numeric": False},
    {"key": "geography", "label": "Geography", "numeric": False},
    {"key": "exit_type", "label": "Exit Type", "numeric": False},
    {"key": "close_date", "label": "Close Date", "numeric": False},
    {"key": "final_exit_date", "label": "Final Sale Date", "numeric": False},
    {"key": "hold_period", "label": "Hold Period*", "numeric": True},
    {"key": "status", "label": "Status", "numeric": False},
    {"key": "fund_initial_cost", "label": "Fund Initial Cost", "numeric": True},
    {"key": "fund_total_cost", "label": "Fund Total Cost", "numeric": True},
    {"key": "realized_proceeds", "label": "Realized Proceeds", "numeric": True},
    {"key": "unrealized_value", "label": "Unrealized Value", "numeric": True},
    {"key": "total_value", "label": "Total Value", "numeric": True},
    {"key": "gross_profit", "label": "Gross Profit", "numeric": True},
    {"key": "gross_profit_pct_of_total", "label": "Gross Profit % of Total", "numeric": True},
    {"key": "gross_irr", "label": "Gross IRR", "numeric": True},
    {"key": "realized_moic", "label": "Realized MOIC", "numeric": True},
    {"key": "gross_moic", "label": "Gross MOIC", "numeric": True},
    {"key": "organic_revenue_cagr", "label": "Organic Revenue CAGR*", "numeric": True},
    {"key": "organic_revenue_cumulative_growth", "label": "Organic Revenue Cumulative Growth*", "numeric": True},
    {"key": "vc_organic_revenue_growth_pct", "label": "Organic Revenue Growth", "numeric": True},
    {"key": "vc_add_on_revenue_pct", "label": "Revenue Growth Through Add-Ons", "numeric": True},
    {"key": "vc_multiple_pct", "label": "Multiple Expansion", "numeric": True},
    {"key": "vc_debt_pct", "label": "Debt Reduction/(Increase)", "numeric": True},
    {"key": "vc_total_pct", "label": "Total", "numeric": True},
    {"key": "vc_organic_revenue_growth_dollar", "label": "Organic Revenue Growth", "numeric": True},
    {"key": "vc_add_on_revenue_dollar", "label": "Revenue Growth Through Add-Ons", "numeric": True},
    {"key": "vc_multiple_dollar", "label": "Multiple Expansion", "numeric": True},
    {"key": "vc_debt_dollar", "label": "Debt Reduction/(Increase)", "numeric": True},
    {"key": "vc_total_dollar", "label": "Total", "numeric": True},
    {"key": "entry_ltm_revenue", "label": "Entry LTM Revenue", "numeric": True},
    {"key": "entry_ev_revenue", "label": "Entry EV/Revenue*", "numeric": True},
    {"key": "entry_net_debt", "label": "Entry Net Debt", "numeric": True},
    {"key": "entry_net_debt_revenue", "label": "Entry Net Debt/Revenue*", "numeric": True},
    {"key": "entry_net_debt_ev", "label": "Entry Net Debt/EV", "numeric": True},
    {"key": "acquired_revenue", "label": "Acquired Revenue", "numeric": True},
    {"key": "acquired_ev_revenue", "label": "Acquired EV/Revenue*", "numeric": True},
    {"key": "blended_ev_revenue_with_addons", "label": "Blended EV/Revenue With Add-Ons", "numeric": True},
    {"key": "exit_ltm_revenue", "label": "Exit/Current LTM Revenue", "numeric": True},
    {"key": "exit_ev_revenue", "label": "Exit/Current EV/Revenue*", "numeric": True},
    {"key": "exit_net_debt", "label": "Exit/Current Net Debt", "numeric": True},
    {"key": "exit_net_debt_revenue", "label": "Exit/Current Net Debt/Revenue*", "numeric": True},
    {"key": "exit_net_debt_ev", "label": "Exit/Current Net Debt/EV", "numeric": True},
    {"key": "diff_revenue", "label": "Difference Revenue", "numeric": True},
    {"key": "diff_ev_revenue", "label": "Difference EV/Revenue*", "numeric": True},
    {"key": "diff_net_debt", "label": "Difference Net Debt", "numeric": True},
    {"key": "diff_net_debt_revenue", "label": "Difference Net Debt/Revenue*", "numeric": True},
    {"key": "diff_net_debt_ev", "label": "Difference Net Debt/EV", "numeric": True},
]

REVENUE_VCA_HEADER_GROUPS = [
    {"label": "Deal Profile", "span": 9},
    {"label": "Fund Performance", "span": 10},
    {"label": "Organic Revenue Growth During Hold Period", "span": 2},
    {"label": "Value Creation (%)", "span": 5},
    {"label": "Value Creation ($)", "span": 5},
    {"label": "Company Op. Metrics At Entry", "span": 5},
    {"label": "Add-Ons", "span": 2},
    {"label": "Blended EV/Revenue With Add-Ons", "span": 1},
    {"label": "Company Op. Metrics At Exit / Current", "span": 5},
    {"label": "Difference Exit/Current vs Entry", "span": 5},
]

REVENUE_FORMULA_LEGEND = {
    "hold_period": "(Exit - Close) / 365.25",
    "total_value": "C = A + B",
    "gross_profit": "D = C - Cost",
    "gross_profit_pct_of_total": "E = D / Total GP",
    "realized_moic": "F = Realized / Cost",
    "gross_moic": "G = Total / Cost",
    "organic_revenue_cagr": "H = ((Organic Exit Revenue/Entry Revenue)^(1/Hold))-1",
    "organic_revenue_cumulative_growth": "I = Organic Exit Revenue/Entry Revenue - 1",
    "vc_organic_revenue_growth_pct": "J = Organic Revenue $ / D",
    "vc_add_on_revenue_pct": "K = Add-On Revenue $ / D",
    "vc_multiple_pct": "L = Multiple $ / D",
    "vc_debt_pct": "M = Debt $ / D",
    "vc_total_pct": "N = J + K + L + M",
    "vc_total_dollar": "S = O + P + Q + R",
    "acquired_ev_revenue": "Acq TEV / Acq Revenue",
    "blended_ev_revenue_with_addons": "(Entry TEV + Acq TEV) / (Entry Revenue + Acq Revenue)",
    "diff_revenue": "Exit - Entry",
    "diff_ev_revenue": "Exit - Entry",
    "diff_net_debt": "Exit - Entry",
    "diff_net_debt_revenue": "Exit - Entry",
    "diff_net_debt_ev": "Exit - Entry",
}


def _blank_revenue_row(row_kind="detail", platform=None):
    row = {key: None for key in REVENUE_COLUMN_KEYS}
    row["platform"] = platform
    row["row_kind"] = row_kind
    return row


def _normalize_displayed_revenue_vca_percentages(row):
    gross_profit = row.get("gross_profit")
    component_keys = (
        ("vc_organic_revenue_growth_dollar", "vc_organic_revenue_growth_pct"),
        ("vc_add_on_revenue_dollar", "vc_add_on_revenue_pct"),
        ("vc_multiple_dollar", "vc_multiple_pct"),
        ("vc_debt_dollar", "vc_debt_pct"),
    )
    if gross_profit in (None, 0):
        for _, pct_key in component_keys:
            row[pct_key] = None
        row["vc_total_pct"] = None
        return row

    component_dollars = [row.get(dollar_key) for dollar_key, _ in component_keys]
    if all(value is None for value in component_dollars):
        for _, pct_key in component_keys:
            row[pct_key] = None
        row["vc_total_pct"] = None
        return row

    raw_units = []
    for value in component_dollars:
        ratio = safe_divide(0.0 if value is None else value, gross_profit)
        raw_units.append((ratio or 0.0) * DISPLAY_PERCENT_UNITS)

    reconciled_units = _reconciled_display_percent_units(raw_units)
    for (_, pct_key), units in zip(component_keys, reconciled_units):
        row[pct_key] = units / DISPLAY_PERCENT_UNITS
    row["vc_total_pct"] = 1.0
    return row


def _clear_revenue_summary_value_creation_fields(row):
    for key in (
        "vc_organic_revenue_growth_pct",
        "vc_add_on_revenue_pct",
        "vc_multiple_pct",
        "vc_debt_pct",
        "vc_total_pct",
        "vc_organic_revenue_growth_dollar",
        "vc_add_on_revenue_dollar",
        "vc_multiple_dollar",
        "vc_debt_dollar",
        "vc_total_dollar",
    ):
        row[key] = None
    return row


def _revenue_add_on_bridge_values(deal, metric, gross_profit):
    bridge = metric.get("bridge_additive_fund") or {}
    drivers = bridge.get("drivers_dollar") or {}
    ownership = _ownership_for_deal(deal, metric)

    entry_multiple = metric.get("entry_tev_revenue")
    organic_exit_revenue = _organic_exit_value(metric.get("exit_revenue"), metric.get("acquired_revenue"))
    organic_growth = _difference(organic_exit_revenue, metric.get("entry_revenue"))
    acquired_revenue = metric.get("acquired_revenue") or 0.0

    organic_dollar = None
    add_on_dollar = None
    if entry_multiple is not None and ownership is not None:
        if organic_growth is not None:
            organic_dollar = organic_growth * entry_multiple * ownership
        add_on_dollar = acquired_revenue * entry_multiple * ownership

    debt_dollar = drivers.get("leverage")
    if debt_dollar is None and deal.entry_net_debt is not None and deal.exit_net_debt is not None and ownership is not None:
        debt_dollar = (deal.entry_net_debt - deal.exit_net_debt) * ownership

    total_dollar = gross_profit
    multiple_dollar = (
        total_dollar - organic_dollar - add_on_dollar - debt_dollar
        if total_dollar is not None and organic_dollar is not None and add_on_dollar is not None and debt_dollar is not None
        else drivers.get("multiple")
    )
    if total_dollar is None:
        total_dollar = _sum_present([organic_dollar, add_on_dollar, multiple_dollar, debt_dollar])

    return {
        "vc_organic_revenue_growth_dollar": organic_dollar,
        "vc_add_on_revenue_dollar": add_on_dollar,
        "vc_multiple_dollar": multiple_dollar,
        "vc_debt_dollar": debt_dollar,
        "vc_total_dollar": total_dollar,
        "vc_organic_revenue_growth_pct": safe_divide(organic_dollar, gross_profit),
        "vc_add_on_revenue_pct": safe_divide(add_on_dollar, gross_profit),
        "vc_multiple_pct": safe_divide(multiple_dollar, gross_profit),
        "vc_debt_pct": safe_divide(debt_dollar, gross_profit),
        "vc_total_pct": safe_divide(total_dollar, gross_profit),
    }


def _row_revenue_operating_fields(row, metric):
    entry_revenue = metric.get("entry_revenue")
    entry_tev = metric.get("entry_enterprise_value")
    entry_net_debt = metric.get("entry_net_debt")
    exit_revenue = metric.get("exit_revenue")
    exit_tev = metric.get("exit_enterprise_value")
    exit_net_debt = metric.get("exit_net_debt")
    acquired_revenue = metric.get("acquired_revenue")
    acquired_tev = metric.get("acquired_tev")

    blended_tev = _sum_present([entry_tev, acquired_tev])
    blended_revenue = _sum_present([entry_revenue, acquired_revenue])

    row.update(
        {
            "entry_ltm_revenue": entry_revenue,
            "entry_tev": entry_tev,
            "entry_ev_revenue": safe_divide(entry_tev, entry_revenue),
            "entry_net_debt": entry_net_debt,
            "entry_net_debt_revenue": safe_divide(entry_net_debt, entry_revenue),
            "entry_net_debt_ev": safe_divide(entry_net_debt, entry_tev),
            "acquired_revenue": acquired_revenue,
            "acquired_ev_revenue": safe_divide(acquired_tev, acquired_revenue),
            "blended_ev_revenue_with_addons": safe_divide(blended_tev, blended_revenue),
            "exit_ltm_revenue": exit_revenue,
            "exit_tev": exit_tev,
            "exit_ev_revenue": safe_divide(exit_tev, exit_revenue),
            "exit_net_debt": exit_net_debt,
            "exit_net_debt_revenue": safe_divide(exit_net_debt, exit_revenue),
            "exit_net_debt_ev": safe_divide(exit_net_debt, exit_tev),
        }
    )
    row["diff_revenue"] = _difference(row["exit_ltm_revenue"], row["entry_ltm_revenue"])
    row["diff_tev"] = _difference(row["exit_tev"], row["entry_tev"])
    row["diff_ev_revenue"] = _difference(row["exit_ev_revenue"], row["entry_ev_revenue"])
    row["diff_net_debt"] = _difference(row["exit_net_debt"], row["entry_net_debt"])
    row["diff_net_debt_revenue"] = _difference(row["exit_net_debt_revenue"], row["entry_net_debt_revenue"])
    row["diff_net_debt_ev"] = _difference(row["exit_net_debt_ev"], row["entry_net_debt_ev"])
    return row


def _aggregate_revenue_operating_fields(deals_subset):
    row = {}
    for key, attr in (
        ("entry_revenue", "entry_revenue"),
        ("entry_enterprise_value", "entry_enterprise_value"),
        ("entry_net_debt", "entry_net_debt"),
        ("exit_revenue", "exit_revenue"),
        ("exit_enterprise_value", "exit_enterprise_value"),
        ("exit_net_debt", "exit_net_debt"),
        ("acquired_revenue", "acquired_revenue"),
        ("acquired_tev", "acquired_tev"),
    ):
        row[key] = _sum_present(getattr(deal, attr, None) for deal in deals_subset)

    output = {}
    _row_revenue_operating_fields(output, row)
    return output


def build_vca_addon_revenue_row(deal, metric, row_num, gross_profit_denominator):
    row = _blank_revenue_row(row_kind="deal", platform=deal.company_name or "Unknown")
    row["row_num"] = row_num
    row["sector"] = deal.sector
    row["geography"] = deal.geography
    row["exit_type"] = getattr(deal, "exit_type", None)
    row["close_date"] = deal.investment_date
    row["final_exit_date"] = deal.exit_date
    row["hold_period"] = metric.get("hold_period")
    row["status"] = _normalize_status(deal.status)

    row["fund_initial_cost"] = metric.get("equity")
    row["fund_total_cost"] = metric.get("equity")
    row["realized_proceeds"] = metric.get("realized")
    row["unrealized_value"] = metric.get("unrealized")
    row["total_value"] = metric.get("value_total")
    row["gross_profit"] = metric.get("value_created")
    row["gross_profit_pct_of_total"] = safe_divide(row["gross_profit"], gross_profit_denominator)

    row["gross_irr"] = deal.irr
    row["realized_moic"] = metric.get("realized_moic")
    row["gross_moic"] = metric.get("moic")

    organic_exit_revenue = _organic_exit_value(metric.get("exit_revenue"), metric.get("acquired_revenue"))
    row["organic_revenue_cagr"] = _cagr_pct(organic_exit_revenue, metric.get("entry_revenue"), metric.get("hold_period"))
    row["organic_revenue_cumulative_growth"] = _growth_pct(organic_exit_revenue, metric.get("entry_revenue"))

    row.update(_revenue_add_on_bridge_values(deal, metric, row["gross_profit"]))
    _row_revenue_operating_fields(row, metric)
    return _normalize_displayed_revenue_vca_percentages(row)


def build_vca_addon_revenue_subtotal(label, deals_subset, metrics_by_id=None, gross_profit_denominator=None):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals_subset}
    if not deals_subset:
        return None

    row = _blank_revenue_row(row_kind="subtotal", platform=label)
    component_rows = [
        build_vca_addon_revenue_row(deal, metrics_by_id[deal.id], idx, gross_profit_denominator)
        for idx, deal in enumerate(deals_subset, start=1)
    ]

    row["status"] = ""
    for key in (
        "fund_initial_cost",
        "fund_total_cost",
        "realized_proceeds",
        "unrealized_value",
        "total_value",
        "gross_profit",
    ):
        row[key] = _sum_present(r.get(key) for r in component_rows)
    row["gross_profit_pct_of_total"] = safe_divide(row["gross_profit"], gross_profit_denominator)
    row["gross_irr"] = _weighted_average((r.get("gross_irr"), r.get("fund_total_cost")) for r in component_rows)
    row["realized_moic"] = safe_divide(row["realized_proceeds"], row["fund_total_cost"])
    row["gross_moic"] = safe_divide(row["total_value"], row["fund_total_cost"])
    row["hold_period"] = _weighted_average((r.get("hold_period"), r.get("fund_total_cost")) for r in component_rows)
    row["organic_revenue_cagr"] = _weighted_average((r.get("organic_revenue_cagr"), r.get("fund_total_cost")) for r in component_rows)
    row["organic_revenue_cumulative_growth"] = _weighted_average((r.get("organic_revenue_cumulative_growth"), r.get("fund_total_cost")) for r in component_rows)

    for key in (
        "vc_organic_revenue_growth_dollar",
        "vc_add_on_revenue_dollar",
        "vc_multiple_dollar",
        "vc_debt_dollar",
    ):
        row[key] = _sum_present(r.get(key) for r in component_rows)
    row["vc_total_dollar"] = _sum_present(
        row.get(key)
        for key in (
            "vc_organic_revenue_growth_dollar",
            "vc_add_on_revenue_dollar",
            "vc_multiple_dollar",
            "vc_debt_dollar",
        )
    )

    row.update(_aggregate_revenue_operating_fields(deals_subset))
    return _normalize_displayed_revenue_vca_percentages(row)


def build_vca_addon_revenue_summary_rows(deal_rows):
    output = []
    summary_modes = ("Average", "Median", "Weighted Average")

    for label in summary_modes:
        row = _blank_revenue_row(row_kind="summary", platform=label)
        row["status"] = ""
        for key in REVENUE_NUMERIC_SUMMARY_KEYS:
            values = [r.get(key) for r in deal_rows if r.get(key) is not None]
            if label == "Average":
                row[key] = _mean(values)
                continue
            if label == "Median":
                row[key] = median(values) if values else None
                continue
            weighted_pairs = [(r.get(key), r.get("fund_total_cost")) for r in deal_rows if r.get(key) is not None]
            row[key] = _weighted_average(weighted_pairs)
        output.append(_clear_revenue_summary_value_creation_fields(row))

    return output


def compute_vca_addons_revenue_analysis(deals, metrics_by_id=None):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals}
    by_fund = defaultdict(list)

    for deal in deals:
        fund_name = deal.fund_number or "Unknown Fund"
        by_fund[fund_name].append(deal)

    grand_gross_profit = sum((metrics_by_id[deal.id].get("value_created") or 0.0) for deal in deals)
    fund_blocks = []
    all_deal_rows = []

    for fund_name in sorted(by_fund.keys(), key=_fund_sort_key):
        ordered_deals = _ordered_deals_for_fund(by_fund[fund_name])
        fund_gross_profit = sum((metrics_by_id[deal.id].get("value_created") or 0.0) for deal in ordered_deals)
        fund_size_values = [deal.fund_size for deal in ordered_deals if deal.fund_size is not None]
        fund_size = fund_size_values[0] if fund_size_values else None
        fund_size_conflict = any(abs(value - fund_size) > 1e-9 for value in fund_size_values[1:]) if fund_size is not None else False

        deal_rows = []
        for idx, deal in enumerate(ordered_deals, start=1):
            row = build_vca_addon_revenue_row(
                deal,
                metrics_by_id[deal.id],
                row_num=idx,
                gross_profit_denominator=fund_gross_profit,
            )
            row["deal_id"] = deal.id
            deal_rows.append(row)

        subtotal_rows = []
        for label, subset in _subtotal_deal_sets(fund_name, ordered_deals):
            subset_metrics = {deal.id: metrics_by_id[deal.id] for deal in subset}
            subtotal = build_vca_addon_revenue_subtotal(
                label,
                subset,
                metrics_by_id=subset_metrics,
                gross_profit_denominator=fund_gross_profit,
            )
            if subtotal is not None:
                subtotal_rows.append(subtotal)

        summary_rows = build_vca_addon_revenue_summary_rows(deal_rows)
        all_subtotal = subtotal_rows[-1] if subtotal_rows else None
        fund_blocks.append(
            {
                "fund_name": fund_name,
                "fund_size": fund_size,
                "fund_size_conflict": fund_size_conflict,
                "net_performance": _fund_net_performance(ordered_deals),
                "print_sort_metrics": {
                    "gross_profit": all_subtotal.get("gross_profit") if all_subtotal else None,
                    "gross_moic": all_subtotal.get("gross_moic") if all_subtotal else None,
                    "gross_irr": all_subtotal.get("gross_irr") if all_subtotal else None,
                    "status_rank": _dominant_status_rank(deal_rows),
                    "fund_name_norm": (fund_name or "").lower(),
                },
                "deal_rows": deal_rows,
                "subtotal_rows": subtotal_rows,
                "summary_rows": summary_rows,
            }
        )
        all_deal_rows.extend(deal_rows)

    ordered_all_deals = []
    for fund_name in sorted(by_fund.keys(), key=_fund_sort_key):
        ordered_all_deals.extend(_ordered_deals_for_fund(by_fund[fund_name]))

    overall_subtotals = []
    for label, subset in _overall_subtotal_deal_sets(ordered_all_deals):
        subset_metrics = {deal.id: metrics_by_id[deal.id] for deal in subset}
        subtotal = build_vca_addon_revenue_subtotal(
            label,
            subset,
            metrics_by_id=subset_metrics,
            gross_profit_denominator=grand_gross_profit,
        )
        if subtotal is not None:
            overall_subtotals.append(subtotal)

    overall_summary = build_vca_addon_revenue_summary_rows(all_deal_rows)
    grand_total_row = overall_subtotals[-1] if overall_subtotals else None
    formula_row = [REVENUE_FORMULA_LEGEND.get(column["key"], "") for column in REVENUE_VCA_COLUMNS]

    return {
        "meta": {
            "title": "Value Creation Analysis - with Add-Ons by Revenue",
            "as_of_date": resolve_analysis_as_of_date(deals),
            "currency_unit_label": "USD $M",
            "footnotes": [
                "* Hold period and CAGR calculations use year fractions with a 365.25-day basis.",
                "Organic Revenue excludes uploaded Acquired Revenue from Exit/Current Revenue before calculating organic growth.",
                "Revenue Growth Through Add-Ons uses uploaded Acquired Revenue at the entry EV/Revenue multiple and fund ownership.",
                "Acquired EV/Revenue and blended EV/Revenue use uploaded Acquired TEV.",
                "Fund Initial Cost mirrors Fund Total Cost until a separate add-on equity cost upload field is available.",
            ],
            "formula_legend": REVENUE_FORMULA_LEGEND,
        },
        "header": {
            "groups": REVENUE_VCA_HEADER_GROUPS,
            "columns": REVENUE_VCA_COLUMNS,
            "formula_row": formula_row,
        },
        "fund_blocks": fund_blocks,
        "overall_block": {
            "subtotal_rows": overall_subtotals,
            "summary_rows": overall_summary,
            "summary_metrics": {
                "gross_profit": grand_total_row.get("gross_profit") if grand_total_row else None,
                "gross_moic": grand_total_row.get("gross_moic") if grand_total_row else None,
                "gross_irr": grand_total_row.get("gross_irr") if grand_total_row else None,
                "deal_count": len(ordered_all_deals),
                "fund_count": len(fund_blocks),
            },
        },
    }

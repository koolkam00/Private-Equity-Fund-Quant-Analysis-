"""Value Creation Analysis (by Revenue) payload builder."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from statistics import median

from services.metrics.common import safe_divide
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import compute_bridge_aggregate, compute_portfolio_analytics

STATUS_ORDER = ("Fully Realized", "Partially Realized", "Unrealized", "Other")
STATUS_INDEX = {status: idx for idx, status in enumerate(STATUS_ORDER)}

COLUMN_KEYS = (
    "row_num",
    "platform",
    "close_date",
    "final_exit_date",
    "hold_period",
    "status",
    "fund_total_cost",
    "realized_proceeds",
    "unrealized_value",
    "total_value",
    "gross_profit",
    "gross_profit_pct_of_total",
    "gross_irr",
    "realized_moic",
    "gross_moic",
    "revenue_cagr",
    "revenue_cumulative_growth",
    "vc_revenue_growth_pct",
    "vc_multiple_pct",
    "vc_debt_pct",
    "vc_total_pct",
    "vc_revenue_growth_dollar",
    "vc_multiple_dollar",
    "vc_debt_dollar",
    "vc_total_dollar",
    "entry_ltm_revenue",
    "entry_tev",
    "entry_ev_revenue",
    "entry_net_debt_revenue",
    "entry_net_debt_ev",
    "exit_ltm_revenue",
    "exit_tev",
    "exit_ev_revenue",
    "exit_net_debt_revenue",
    "exit_net_debt_ev",
    "diff_revenue",
    "diff_tev",
    "diff_ev_revenue",
    "diff_net_debt_revenue",
    "diff_net_debt_ev",
)

NUMERIC_SUMMARY_KEYS = (
    "hold_period",
    "fund_total_cost",
    "realized_proceeds",
    "unrealized_value",
    "total_value",
    "gross_profit",
    "gross_profit_pct_of_total",
    "gross_irr",
    "realized_moic",
    "gross_moic",
    "revenue_cagr",
    "revenue_cumulative_growth",
    "vc_revenue_growth_pct",
    "vc_multiple_pct",
    "vc_debt_pct",
    "vc_total_pct",
    "vc_revenue_growth_dollar",
    "vc_multiple_dollar",
    "vc_debt_dollar",
    "vc_total_dollar",
    "entry_ltm_revenue",
    "entry_tev",
    "entry_ev_revenue",
    "entry_net_debt_revenue",
    "entry_net_debt_ev",
    "exit_ltm_revenue",
    "exit_tev",
    "exit_ev_revenue",
    "exit_net_debt_revenue",
    "exit_net_debt_ev",
    "diff_revenue",
    "diff_tev",
    "diff_ev_revenue",
    "diff_net_debt_revenue",
    "diff_net_debt_ev",
)

VCA_COLUMNS = [
    {"key": "row_num", "label": "#", "numeric": True},
    {"key": "platform", "label": "Platform", "numeric": False},
    {"key": "close_date", "label": "Close Date", "numeric": False},
    {"key": "final_exit_date", "label": "Final Exit Date", "numeric": False},
    {"key": "hold_period", "label": "Hold Period*", "numeric": True},
    {"key": "status", "label": "Status", "numeric": False},
    {"key": "fund_total_cost", "label": "Fund Total Cost", "numeric": True},
    {"key": "realized_proceeds", "label": "Realized Proceeds", "numeric": True},
    {"key": "unrealized_value", "label": "Unrealized Value", "numeric": True},
    {"key": "total_value", "label": "Total Value", "numeric": True},
    {"key": "gross_profit", "label": "Gross Profit", "numeric": True},
    {"key": "gross_profit_pct_of_total", "label": "Gross Profit % of Total", "numeric": True},
    {"key": "gross_irr", "label": "Gross IRR", "numeric": True},
    {"key": "realized_moic", "label": "Realized MOIC", "numeric": True},
    {"key": "gross_moic", "label": "Gross MOIC", "numeric": True},
    {"key": "revenue_cagr", "label": "Revenue CAGR*", "numeric": True},
    {"key": "revenue_cumulative_growth", "label": "Revenue Cumulative Growth*", "numeric": True},
    {"key": "vc_revenue_growth_pct", "label": "Revenue Growth", "numeric": True},
    {"key": "vc_multiple_pct", "label": "Multiple Expansion", "numeric": True},
    {"key": "vc_debt_pct", "label": "Debt Reduction/(Increase)", "numeric": True},
    {"key": "vc_total_pct", "label": "Total", "numeric": True},
    {"key": "vc_revenue_growth_dollar", "label": "Revenue Growth", "numeric": True},
    {"key": "vc_multiple_dollar", "label": "Multiple Expansion", "numeric": True},
    {"key": "vc_debt_dollar", "label": "Debt Reduction/(Increase)", "numeric": True},
    {"key": "vc_total_dollar", "label": "Total", "numeric": True},
    {"key": "entry_ltm_revenue", "label": "Entry LTM Revenue", "numeric": True},
    {"key": "entry_tev", "label": "Entry TEV", "numeric": True},
    {"key": "entry_ev_revenue", "label": "Entry EV/Revenue*", "numeric": True},
    {"key": "entry_net_debt_revenue", "label": "Entry Net Debt/Revenue*", "numeric": True},
    {"key": "entry_net_debt_ev", "label": "Entry Net Debt/EV", "numeric": True},
    {"key": "exit_ltm_revenue", "label": "Exit/Current LTM Revenue", "numeric": True},
    {"key": "exit_tev", "label": "Exit/Current TEV", "numeric": True},
    {"key": "exit_ev_revenue", "label": "Exit/Current EV/Revenue*", "numeric": True},
    {"key": "exit_net_debt_revenue", "label": "Exit/Current Net Debt/Revenue*", "numeric": True},
    {"key": "exit_net_debt_ev", "label": "Exit/Current Net Debt/EV", "numeric": True},
    {"key": "diff_revenue", "label": "Difference Revenue", "numeric": True},
    {"key": "diff_tev", "label": "Difference TEV", "numeric": True},
    {"key": "diff_ev_revenue", "label": "Difference EV/Revenue*", "numeric": True},
    {"key": "diff_net_debt_revenue", "label": "Difference Net Debt/Revenue*", "numeric": True},
    {"key": "diff_net_debt_ev", "label": "Difference Net Debt/EV", "numeric": True},
]

VCA_HEADER_GROUPS = [
    {"label": "Deal Profile", "span": 6},
    {"label": "Fund Performance", "span": 9},
    {"label": "Revenue Growth During Hold Period", "span": 2},
    {"label": "Value Creation (%)", "span": 4},
    {"label": "Value Creation ($)", "span": 4},
    {"label": "Company Op. Metrics At Entry", "span": 5},
    {"label": "Company Op. Metrics At Exit / Current", "span": 5},
    {"label": "Difference Exit/Current vs Entry", "span": 5},
]

FORMULA_LEGEND = {
    "hold_period": "(Exit - Close) / 365.25",
    "total_value": "C = A + B",
    "gross_profit": "D = C - Cost",
    "gross_profit_pct_of_total": "E = D / Total GP",
    "realized_moic": "F = Realized / Cost",
    "gross_moic": "G = Total / Cost",
    "revenue_cagr": "H = ((Exit/Entry)^(1/Hold))-1",
    "revenue_cumulative_growth": "I = Exit/Entry - 1",
    "vc_revenue_growth_pct": "J = O / D",
    "vc_multiple_pct": "K = P / D",
    "vc_debt_pct": "L = Q / D",
    "vc_total_pct": "M = J + K + L",
    "vc_total_dollar": "R = D",
    "diff_revenue": "S = Exit - Entry",
    "diff_tev": "T = Exit - Entry",
    "diff_ev_revenue": "U = Exit - Entry",
    "diff_net_debt_revenue": "V = Exit - Entry",
    "diff_net_debt_ev": "W = Exit - Entry",
}


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


def _date_sort_value(value):
    if value is None:
        return 3652059
    return value.toordinal()


def _mean(values):
    return (sum(values) / len(values)) if values else None


def _weighted_average(pairs):
    numer = 0.0
    denom = 0.0
    for value, weight in pairs:
        if value is None:
            continue
        if weight is None or weight <= 0:
            continue
        numer += value * weight
        denom += weight
    if denom <= 0:
        return None
    return numer / denom


def _resolve_scalar(values, tolerance=1e-9):
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return {"value": None, "conflict": False}
    base = clean[0]
    if any(abs(v - base) > tolerance for v in clean[1:]):
        return {"value": None, "conflict": True}
    return {"value": base, "conflict": False}


def _fund_net_performance(deals):
    irr = _resolve_scalar([deal.net_irr for deal in deals])
    moic = _resolve_scalar([deal.net_moic for deal in deals])
    dpi = _resolve_scalar([deal.net_dpi for deal in deals])
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


def _dominant_status_rank(deal_rows):
    status_counts = defaultdict(int)
    for row in deal_rows:
        status_counts[_normalize_status(row.get("status"))] += 1
    if not status_counts:
        return 99
    dominant = sorted(status_counts.items(), key=lambda item: (-item[1], STATUS_INDEX.get(item[0], 99)))[0][0]
    return STATUS_INDEX.get(dominant, 99)


def _pick_wavg(metric_block):
    if not isinstance(metric_block, dict):
        return None
    if metric_block.get("wavg") is not None:
        return metric_block.get("wavg")
    return metric_block.get("avg")


def _coalesce_sum(*values):
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present)


def _blank_vca_row(row_kind="detail", platform=None):
    row = {key: None for key in COLUMN_KEYS}
    row["platform"] = platform
    row["row_kind"] = row_kind
    return row


def _bridge_values_from_metric(metric, gross_profit):
    bridge = metric.get("bridge_additive_fund") or {}
    if not bridge.get("ready"):
        return {
            "vc_revenue_growth_dollar": None,
            "vc_multiple_dollar": None,
            "vc_debt_dollar": None,
            "vc_total_dollar": None,
            "vc_revenue_growth_pct": None,
            "vc_multiple_pct": None,
            "vc_debt_pct": None,
            "vc_total_pct": None,
        }

    drivers = bridge.get("drivers_dollar") or {}
    revenue_growth_dollar = drivers.get("revenue")
    debt_dollar = drivers.get("leverage")
    total_dollar = gross_profit
    multiple_dollar = (
        total_dollar - revenue_growth_dollar - debt_dollar
        if total_dollar is not None and revenue_growth_dollar is not None and debt_dollar is not None
        else None
    )

    return {
        "vc_revenue_growth_dollar": revenue_growth_dollar,
        "vc_multiple_dollar": multiple_dollar,
        "vc_debt_dollar": debt_dollar,
        "vc_total_dollar": total_dollar,
        "vc_revenue_growth_pct": safe_divide(revenue_growth_dollar, gross_profit),
        "vc_multiple_pct": safe_divide(multiple_dollar, gross_profit),
        "vc_debt_pct": safe_divide(debt_dollar, gross_profit),
        "vc_total_pct": safe_divide(total_dollar, gross_profit),
    }


def _bridge_values_from_subset(deals_subset, gross_profit):
    aggregate = compute_bridge_aggregate(deals_subset, basis="fund")
    if (aggregate.get("ready_count") or 0) <= 0:
        return {
            "vc_revenue_growth_dollar": None,
            "vc_multiple_dollar": None,
            "vc_debt_dollar": None,
            "vc_total_dollar": None,
            "vc_revenue_growth_pct": None,
            "vc_multiple_pct": None,
            "vc_debt_pct": None,
            "vc_total_pct": None,
            "bridge_ready_count": 0,
        }

    drivers = (aggregate.get("drivers") or {}).get("dollar") or {}
    revenue_growth_dollar = drivers.get("revenue")
    debt_dollar = drivers.get("leverage")
    total_dollar = gross_profit
    multiple_dollar = (
        total_dollar - revenue_growth_dollar - debt_dollar
        if total_dollar is not None and revenue_growth_dollar is not None and debt_dollar is not None
        else None
    )

    return {
        "vc_revenue_growth_dollar": revenue_growth_dollar,
        "vc_multiple_dollar": multiple_dollar,
        "vc_debt_dollar": debt_dollar,
        "vc_total_dollar": total_dollar,
        "vc_revenue_growth_pct": safe_divide(revenue_growth_dollar, gross_profit),
        "vc_multiple_pct": safe_divide(multiple_dollar, gross_profit),
        "vc_debt_pct": safe_divide(debt_dollar, gross_profit),
        "vc_total_pct": safe_divide(total_dollar, gross_profit),
        "bridge_ready_count": aggregate.get("ready_count") or 0,
    }


def _operating_fields_from_portfolio(portfolio):
    entry = portfolio.get("entry") or {}
    exit_ = portfolio.get("exit") or {}

    entry_ltm_revenue = _pick_wavg(entry.get("revenue"))
    exit_ltm_revenue = _pick_wavg(exit_.get("revenue"))

    entry_tev = _pick_wavg(entry.get("tev"))
    exit_tev = _pick_wavg(exit_.get("tev"))

    entry_ev_revenue = _pick_wavg(entry.get("tev_revenue"))
    exit_ev_revenue = _pick_wavg(exit_.get("tev_revenue"))

    entry_net_debt = _pick_wavg(entry.get("net_debt"))
    exit_net_debt = _pick_wavg(exit_.get("net_debt"))
    entry_nd_revenue = safe_divide(entry_net_debt, entry_ltm_revenue)
    exit_nd_revenue = safe_divide(exit_net_debt, exit_ltm_revenue)

    entry_nd_ev = _pick_wavg(entry.get("net_debt_tev"))
    exit_nd_ev = _pick_wavg(exit_.get("net_debt_tev"))

    return {
        "entry_ltm_revenue": entry_ltm_revenue,
        "entry_tev": entry_tev,
        "entry_ev_revenue": entry_ev_revenue,
        "entry_net_debt_revenue": entry_nd_revenue,
        "entry_net_debt_ev": entry_nd_ev,
        "exit_ltm_revenue": exit_ltm_revenue,
        "exit_tev": exit_tev,
        "exit_ev_revenue": exit_ev_revenue,
        "exit_net_debt_revenue": exit_nd_revenue,
        "exit_net_debt_ev": exit_nd_ev,
        "diff_revenue": (exit_ltm_revenue - entry_ltm_revenue)
        if exit_ltm_revenue is not None and entry_ltm_revenue is not None
        else None,
        "diff_tev": (exit_tev - entry_tev)
        if exit_tev is not None and entry_tev is not None
        else None,
        "diff_ev_revenue": (exit_ev_revenue - entry_ev_revenue)
        if exit_ev_revenue is not None and entry_ev_revenue is not None
        else None,
        "diff_net_debt_revenue": (exit_nd_revenue - entry_nd_revenue)
        if exit_nd_revenue is not None and entry_nd_revenue is not None
        else None,
        "diff_net_debt_ev": (exit_nd_ev - entry_nd_ev)
        if exit_nd_ev is not None and entry_nd_ev is not None
        else None,
    }


def build_vca_row(deal, metric, row_num, gross_profit_denominator):
    row = _blank_vca_row(row_kind="deal", platform=deal.company_name or "Unknown")
    row["row_num"] = row_num
    row["close_date"] = deal.investment_date
    row["final_exit_date"] = deal.exit_date
    row["hold_period"] = metric.get("hold_period")
    row["status"] = _normalize_status(deal.status)

    row["fund_total_cost"] = metric.get("equity")
    row["realized_proceeds"] = metric.get("realized")
    row["unrealized_value"] = metric.get("unrealized")
    row["total_value"] = metric.get("value_total")
    row["gross_profit"] = metric.get("value_created")
    row["gross_profit_pct_of_total"] = safe_divide(row["gross_profit"], gross_profit_denominator)

    row["gross_irr"] = deal.irr
    row["realized_moic"] = metric.get("realized_moic")
    row["gross_moic"] = metric.get("moic")
    row["revenue_cagr"] = metric.get("revenue_cagr")
    row["revenue_cumulative_growth"] = metric.get("revenue_growth")

    row.update(_bridge_values_from_metric(metric, row["gross_profit"]))

    row["entry_ltm_revenue"] = metric.get("entry_revenue")
    row["entry_tev"] = metric.get("entry_enterprise_value")
    row["entry_ev_revenue"] = metric.get("entry_tev_revenue")
    row["entry_net_debt_revenue"] = safe_divide(metric.get("entry_net_debt"), row["entry_ltm_revenue"])
    row["entry_net_debt_ev"] = metric.get("entry_net_debt_tev")

    row["exit_ltm_revenue"] = metric.get("exit_revenue")
    row["exit_tev"] = metric.get("exit_enterprise_value")
    row["exit_ev_revenue"] = metric.get("exit_tev_revenue")
    row["exit_net_debt_revenue"] = safe_divide(metric.get("exit_net_debt"), row["exit_ltm_revenue"])
    row["exit_net_debt_ev"] = metric.get("exit_net_debt_tev")

    row["diff_revenue"] = (
        row["exit_ltm_revenue"] - row["entry_ltm_revenue"]
        if row["exit_ltm_revenue"] is not None and row["entry_ltm_revenue"] is not None
        else None
    )
    row["diff_tev"] = (
        row["exit_tev"] - row["entry_tev"]
        if row["exit_tev"] is not None and row["entry_tev"] is not None
        else None
    )
    row["diff_ev_revenue"] = (
        row["exit_ev_revenue"] - row["entry_ev_revenue"]
        if row["exit_ev_revenue"] is not None and row["entry_ev_revenue"] is not None
        else None
    )
    row["diff_net_debt_revenue"] = (
        row["exit_net_debt_revenue"] - row["entry_net_debt_revenue"]
        if row["exit_net_debt_revenue"] is not None and row["entry_net_debt_revenue"] is not None
        else None
    )
    row["diff_net_debt_ev"] = (
        row["exit_net_debt_ev"] - row["entry_net_debt_ev"]
        if row["exit_net_debt_ev"] is not None and row["entry_net_debt_ev"] is not None
        else None
    )

    return row


def build_vca_subtotal(label, deals_subset, metrics_by_id=None, gross_profit_denominator=None):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals_subset}
    if not deals_subset:
        return None

    row = _blank_vca_row(row_kind="subtotal", platform=label)

    portfolio = compute_portfolio_analytics(deals_subset, metrics_by_id=metrics_by_id)
    realized_total = sum((metrics_by_id[deal.id].get("realized") or 0.0) for deal in deals_subset)
    unrealized_total = sum((metrics_by_id[deal.id].get("unrealized") or 0.0) for deal in deals_subset)

    row["status"] = ""
    row["fund_total_cost"] = portfolio.get("total_equity")
    row["realized_proceeds"] = realized_total
    row["unrealized_value"] = unrealized_total
    row["total_value"] = portfolio.get("total_value")
    row["gross_profit"] = portfolio.get("total_value_created")
    row["gross_profit_pct_of_total"] = safe_divide(row["gross_profit"], gross_profit_denominator)

    returns = portfolio.get("returns") or {}
    growth = portfolio.get("growth") or {}

    row["hold_period"] = _pick_wavg(returns.get("hold_period"))
    row["gross_irr"] = _pick_wavg(returns.get("gross_irr"))
    row["realized_moic"] = safe_divide(realized_total, row["fund_total_cost"])
    row["gross_moic"] = safe_divide(row["total_value"], row["fund_total_cost"])

    row["revenue_cagr"] = _pick_wavg(growth.get("revenue_cagr"))
    row["revenue_cumulative_growth"] = _pick_wavg(growth.get("revenue_growth"))

    row.update(_bridge_values_from_subset(deals_subset, row["gross_profit"]))
    row.update(_operating_fields_from_portfolio(portfolio))

    return row


def build_vca_summary_rows(deal_rows):
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

            weighted_pairs = [
                (r.get(key), r.get("fund_total_cost"))
                for r in deal_rows
                if r.get(key) is not None
            ]
            row[key] = _weighted_average(weighted_pairs)

        output.append(row)

    return output


def _fund_sort_key(fund_name):
    return (fund_name == "Unknown Fund", (fund_name or "").lower())


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


def _subtotal_deal_sets(fund_name, ordered_deals):
    by_status = defaultdict(list)
    for deal in ordered_deals:
        by_status[_normalize_status(deal.status)].append(deal)

    rows = []
    fully = by_status.get("Fully Realized", [])
    partial = by_status.get("Partially Realized", [])
    unrealized = by_status.get("Unrealized", [])

    if fully:
        rows.append((f"{fund_name} - Fully & Majority Realized", fully))
    if partial:
        rows.append((f"{fund_name} - Partially Realized", partial))
    if fully and partial:
        rows.append((f"{fund_name} - Fully and Partially Realized", fully + partial))
    if unrealized:
        rows.append((f"{fund_name} - Unrealized", unrealized))

    rows.append((f"{fund_name} - All", ordered_deals))
    return rows


def _overall_subtotal_deal_sets(ordered_deals):
    by_status = defaultdict(list)
    for deal in ordered_deals:
        by_status[_normalize_status(deal.status)].append(deal)

    rows = []
    fully = by_status.get("Fully Realized", [])
    partial = by_status.get("Partially Realized", [])
    unrealized = by_status.get("Unrealized", [])

    if fully:
        rows.append(("Total Fully & Majority Realized Only", fully))
    if partial:
        rows.append(("Total Partially Realized Only", partial))
    if fully or partial:
        rows.append(("Total Fully and Partially Realized", fully + partial))
    if unrealized:
        rows.append(("Total Unrealized Only", unrealized))

    rows.append(("Grand Total", ordered_deals))
    return rows


def _as_of_date(deals):
    exit_dates = [deal.exit_date for deal in deals if deal.exit_date is not None]
    if exit_dates:
        return max(exit_dates)

    close_dates = [deal.investment_date for deal in deals if deal.investment_date is not None]
    if close_dates:
        return max(close_dates)

    return date.today()


def compute_vca_revenue_analysis(deals, metrics_by_id=None):
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
            row = build_vca_row(
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
            subtotal = build_vca_subtotal(
                label,
                subset,
                metrics_by_id=subset_metrics,
                gross_profit_denominator=fund_gross_profit,
            )
            if subtotal is not None:
                subtotal_rows.append(subtotal)

        summary_rows = build_vca_summary_rows(deal_rows)
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
        subtotal = build_vca_subtotal(
            label,
            subset,
            metrics_by_id=subset_metrics,
            gross_profit_denominator=grand_gross_profit,
        )
        if subtotal is not None:
            overall_subtotals.append(subtotal)

    overall_summary = build_vca_summary_rows(all_deal_rows)
    grand_total_row = overall_subtotals[-1] if overall_subtotals else None

    formula_row = [FORMULA_LEGEND.get(column["key"], "") for column in VCA_COLUMNS]

    return {
        "meta": {
            "title": "Value Creation Analysis - by Revenue",
            "as_of_date": _as_of_date(deals),
            "currency_unit_label": "USD $M",
            "footnotes": [
                "* Hold period and CAGR calculations use year fractions with a 365.25-day basis.",
                "Value Creation Revenue Growth ($) maps to the bridge revenue driver only.",
                "Value Creation Multiple ($) is residualized (including margin/other effects) to fully reconcile TEV/Revenue attribution.",
                "Value Creation % fields are calculated as lever $ divided by Gross Profit.",
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

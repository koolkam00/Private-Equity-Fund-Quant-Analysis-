"""Cross-firm fund performance comparison analytics."""

from __future__ import annotations

from collections import defaultdict

from services.metrics.common import safe_divide
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import compute_deal_track_record


def compute_fund_performance_comparison(
    firms_data,
    vintage_filter=None,
    metric_filter="",
):
    """Build a cross-firm fund performance comparison payload.

    Parameters
    ----------
    firms_data : list[dict]
        Each dict has keys: firm_id, firm_name, deals, fund_vintage_lookup.
    vintage_filter : int | None
        If set, only include funds matching this vintage year.
    metric_filter : str
        One of "net_irr", "net_moic", "net_dpi", or "" for all.
    """
    comparison_rows = []
    all_vintages = set()

    for firm in firms_data:
        firm_id = firm["firm_id"]
        firm_name = firm["firm_name"]
        deals = firm["deals"]
        fund_vintage_lookup = firm["fund_vintage_lookup"]

        if not deals:
            continue

        metrics_by_id = {d.id: compute_deal_metrics(d) for d in deals}
        track_record = compute_deal_track_record(
            deals,
            metrics_by_id=metrics_by_id,
            fund_vintage_lookup=fund_vintage_lookup,
        )

        for fund in track_record.get("funds", []):
            fund_name = fund.get("fund_name") or "Unknown Fund"
            fund_net = fund.get("net_performance") or {}
            conflicts = fund_net.get("conflicts") or {}
            vintage_year = fund_vintage_lookup.get(fund_name)

            if vintage_filter is not None and vintage_year != vintage_filter:
                continue

            if vintage_year is not None:
                all_vintages.add(vintage_year)

            row = {
                "firm_name": firm_name,
                "firm_id": firm_id,
                "fund_name": fund_name,
                "vintage_year": vintage_year,
                "fund_size": fund.get("fund_size"),
                "deal_count": fund.get("totals", {}).get("deal_count", 0),
                "net_irr": fund_net.get("net_irr"),
                "net_moic": fund_net.get("net_moic"),
                "net_dpi": fund_net.get("net_dpi"),
                "net_irr_conflict": bool(conflicts.get("net_irr")),
                "net_moic_conflict": bool(conflicts.get("net_moic")),
                "net_dpi_conflict": bool(conflicts.get("net_dpi")),
            }
            comparison_rows.append(row)

    # Sort by vintage then firm name
    comparison_rows.sort(
        key=lambda r: (
            r["vintage_year"] is None,
            r["vintage_year"] if r["vintage_year"] is not None else 9999,
            r["firm_name"].lower(),
            r["fund_name"].lower(),
        ),
    )

    # Build chart data: grouped bar charts by vintage, one series per firm
    vintage_labels = sorted(v for v in all_vintages if v is not None)
    firm_names = sorted({r["firm_name"] for r in comparison_rows})

    # Aggregate per-firm-per-vintage (average if multiple funds at same vintage)
    agg = defaultdict(lambda: defaultdict(list))
    for row in comparison_rows:
        vy = row["vintage_year"]
        if vy is None:
            continue
        fn = row["firm_name"]
        for metric in ("net_irr", "net_moic", "net_dpi"):
            conflict_key = f"{metric}_conflict"
            if not row.get(conflict_key) and row.get(metric) is not None:
                agg[(fn, metric)][vy].append(row[metric])

    chart_series = {}
    for metric in ("net_irr", "net_moic", "net_dpi"):
        series = []
        for fn in firm_names:
            data = []
            for vy in vintage_labels:
                values = agg[(fn, metric)].get(vy, [])
                avg = safe_divide(sum(values), len(values)) if values else None
                data.append(avg)
            series.append({"firm_name": fn, "data": data})
        chart_series[metric] = series

    chart_data = {
        "vintage_labels": vintage_labels,
        "series": chart_series,
    }

    # Build firm summaries
    firm_summaries = []
    by_firm = defaultdict(list)
    for row in comparison_rows:
        by_firm[row["firm_name"]].append(row)

    for fn in firm_names:
        rows = by_firm[fn]
        fund_count = len(rows)
        irr_vals = [r["net_irr"] for r in rows if r.get("net_irr") is not None and not r.get("net_irr_conflict")]
        moic_vals = [r["net_moic"] for r in rows if r.get("net_moic") is not None and not r.get("net_moic_conflict")]
        dpi_vals = [r["net_dpi"] for r in rows if r.get("net_dpi") is not None and not r.get("net_dpi_conflict")]

        firm_summaries.append({
            "firm_name": fn,
            "firm_id": rows[0]["firm_id"],
            "fund_count": fund_count,
            "avg_net_irr": safe_divide(sum(irr_vals), len(irr_vals)) if irr_vals else None,
            "avg_net_moic": safe_divide(sum(moic_vals), len(moic_vals)) if moic_vals else None,
            "avg_net_dpi": safe_divide(sum(dpi_vals), len(dpi_vals)) if dpi_vals else None,
        })

    sorted_vintages = sorted(all_vintages) if all_vintages else []

    # Build vintage groups for grouped table display
    vintage_groups = []
    for vy in vintage_labels:
        group_rows = [r for r in comparison_rows if r["vintage_year"] == vy]
        vintage_groups.append({"vintage_year": vy, "rows": group_rows})
    no_vintage = [r for r in comparison_rows if r["vintage_year"] is None]
    if no_vintage:
        vintage_groups.append({"vintage_year": None, "rows": no_vintage})

    return {
        "comparison_rows": comparison_rows,
        "vintage_groups": vintage_groups,
        "chart_data": chart_data,
        "firm_summaries": firm_summaries,
        "meta": {
            "firm_count": len(firm_names),
            "fund_count": len(comparison_rows),
            "vintage_range": [sorted_vintages[0], sorted_vintages[-1]] if sorted_vintages else [None, None],
            "selected_firms": [{"id": rows[0]["firm_id"], "name": fn} for fn, rows in by_firm.items()],
            "available_vintages": sorted_vintages,
        },
    }

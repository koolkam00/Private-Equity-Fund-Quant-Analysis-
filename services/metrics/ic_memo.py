"""IC memo payload builder using existing deal-only analytics primitives."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import math

from services.metrics.bridge import DRIVERS
from services.metrics.common import safe_divide
from services.metrics.deal import compute_deal_metrics
from services.metrics.portfolio import (
    compute_bridge_aggregate,
    compute_lead_partner_scorecard,
    compute_portfolio_analytics,
)
from services.metrics.risk import compute_loss_and_distribution


DRIVER_LABELS = {
    "revenue": "Revenue Growth",
    "margin": "Margin Expansion",
    "multiple": "Multiple Expansion",
    "leverage": "Leverage / Debt Paydown",
    "other": "Residual / Other",
}


def _deal_vintage_year(deal):
    if deal.year_invested is not None:
        return int(deal.year_invested)
    if deal.investment_date is not None:
        return int(deal.investment_date.year)
    return None


def normalize_status_rollup(raw_status):
    status = (raw_status or "").strip().lower()
    if "partial" in status and "realized" in status:
        return "Realized"
    if "fully" in status and "realized" in status:
        return "Realized"
    if status == "realized" or ("realized" in status and "unrealized" not in status):
        return "Realized"
    if "unrealized" in status or status == "":
        return "Unrealized"
    return "Other"


def build_top_bottom_deals_by_value_created(deals, metrics_by_id, n=5):
    rows = []
    for deal in deals:
        metrics = metrics_by_id[deal.id]
        rows.append(
            {
                "deal_id": deal.id,
                "company_name": deal.company_name or "Unknown Company",
                "fund_number": deal.fund_number or "Unknown Fund",
                "status": deal.status or "Unrealized",
                "invested_equity": metrics.get("equity") or 0.0,
                "total_value": metrics.get("value_total") or 0.0,
                "value_created": metrics.get("value_created") or 0.0,
                "moic": metrics.get("moic"),
                "implied_irr": metrics.get("implied_irr"),
            }
        )

    top = sorted(rows, key=lambda row: row["value_created"], reverse=True)[:n]
    bottom = sorted(rows, key=lambda row: row["value_created"])[:n]
    return top, bottom


def _new_group_bucket():
    return {
        "deal_count": 0,
        "invested_equity": 0.0,
        "total_value": 0.0,
        "value_created": 0.0,
        "_irr_num": 0.0,
        "_irr_den": 0.0,
    }


def _update_group_bucket(bucket, metrics):
    equity = metrics.get("equity") or 0.0
    value_total = metrics.get("value_total") or 0.0
    value_created = metrics.get("value_created") or 0.0
    irr = metrics.get("implied_irr")

    bucket["deal_count"] += 1
    bucket["invested_equity"] += equity
    bucket["total_value"] += value_total
    bucket["value_created"] += value_created
    if irr is not None and equity > 0:
        bucket["_irr_num"] += irr * equity
        bucket["_irr_den"] += equity


def _finalize_group_bucket(label, bucket):
    invested = bucket["invested_equity"]
    return {
        "label": label,
        "deal_count": bucket["deal_count"],
        "invested_equity": invested,
        "total_value": bucket["total_value"],
        "value_created": bucket["value_created"],
        "weighted_moic": safe_divide(bucket["total_value"], invested),
        "weighted_implied_irr": safe_divide(bucket["_irr_num"], bucket["_irr_den"]),
    }


def _rank_value(row, ranking_basis):
    value = row.get(ranking_basis)
    return value if value is not None else float("-inf")


def build_dimension_slice(groups, ranking_basis="weighted_moic"):
    rows = [_finalize_group_bucket(label, bucket) for label, bucket in groups.items()]
    rows.sort(key=lambda row: (_rank_value(row, ranking_basis), row["invested_equity"]), reverse=True)
    return rows


def pick_deciles(groups, ranking_basis="weighted_moic", pct=0.10, min_count=1):
    if not groups:
        return [], []

    count = len(groups)
    n = max(min_count, int(math.ceil(count * pct)))

    ranked_top = sorted(
        groups,
        key=lambda row: (row.get(ranking_basis) if row.get(ranking_basis) is not None else float("-inf"), row["invested_equity"]),
        reverse=True,
    )
    ranked_bottom = sorted(
        groups,
        key=lambda row: (row.get(ranking_basis) if row.get(ranking_basis) is not None else float("inf"), row["invested_equity"]),
    )
    return ranked_top[:n], ranked_bottom[:n]


def _build_dimension_groups(deals, metrics_by_id):
    dims = {
        "vintage_year": defaultdict(_new_group_bucket),
        "sector": defaultdict(_new_group_bucket),
        "geography": defaultdict(_new_group_bucket),
        "status_rollup": defaultdict(_new_group_bucket),
        "lead_partner": defaultdict(_new_group_bucket),
        "deal_type": defaultdict(_new_group_bucket),
        "exit_type": defaultdict(_new_group_bucket),
        "entry_channel": defaultdict(_new_group_bucket),
    }

    for deal in deals:
        metrics = metrics_by_id[deal.id]
        vintage = _deal_vintage_year(deal)
        vintage_label = str(vintage) if vintage is not None else "Unknown"

        _update_group_bucket(dims["vintage_year"][vintage_label], metrics)
        _update_group_bucket(dims["sector"][getattr(deal, "sector", None) or "Unknown"], metrics)
        _update_group_bucket(dims["geography"][getattr(deal, "geography", None) or "Unknown"], metrics)
        _update_group_bucket(dims["status_rollup"][normalize_status_rollup(getattr(deal, "status", None))], metrics)
        _update_group_bucket(dims["lead_partner"][getattr(deal, "lead_partner", None) or "Unassigned"], metrics)
        _update_group_bucket(dims["deal_type"][getattr(deal, "deal_type", None) or "Platform"], metrics)
        _update_group_bucket(dims["exit_type"][getattr(deal, "exit_type", None) or "Not Specified"], metrics)
        _update_group_bucket(dims["entry_channel"][getattr(deal, "entry_channel", None) or "Unknown"], metrics)

    return dims


def _build_entry_channel_table(dimension_groups):
    rows = build_dimension_slice(dimension_groups["entry_channel"], ranking_basis="weighted_moic")
    return [
        {
            "entry_channel": row["label"],
            "deal_count": row["deal_count"],
            "capital_deployed": row["invested_equity"],
            "value_created": row["value_created"],
            "weighted_moic": row["weighted_moic"],
            "weighted_implied_irr": row["weighted_implied_irr"],
        }
        for row in sorted(rows, key=lambda row: row["invested_equity"], reverse=True)
    ]


def _build_lead_partner_table(deals, metrics_by_id, dimension_groups):
    scorecard = compute_lead_partner_scorecard(deals, metrics_by_id=metrics_by_id)
    value_created_map = {
        row["label"]: row["value_created"]
        for row in build_dimension_slice(dimension_groups["lead_partner"], ranking_basis="weighted_moic")
    }

    out = []
    for row in scorecard:
        partner = row["lead_partner"]
        out.append(
            {
                "lead_partner": partner,
                "deal_count": row["deal_count"],
                "capital_deployed": row["capital_deployed"],
                "value_created": value_created_map.get(partner, 0.0),
                "weighted_moic": row["weighted_moic"],
                "hit_rate": row["hit_rate"],
                "loss_ratio": row["loss_ratio"],
            }
        )
    return out


def _dimension_payload(dimension_groups, ranking_basis, decile_pct, decile_min):
    out = {}
    for key in (
        "vintage_year",
        "sector",
        "geography",
        "status_rollup",
        "lead_partner",
        "deal_type",
        "exit_type",
    ):
        groups = build_dimension_slice(dimension_groups[key], ranking_basis=ranking_basis)
        top_decile, bottom_decile = pick_deciles(
            groups,
            ranking_basis=ranking_basis,
            pct=decile_pct,
            min_count=decile_min,
        )
        out[key] = {
            "groups": groups,
            "top_decile": top_decile,
            "bottom_decile": bottom_decile,
        }
    return out


def compute_ic_memo_payload(deals, metrics_by_id=None, ranking_basis="weighted_moic", decile_pct=0.10, decile_min=1):
    metrics_by_id = metrics_by_id or {deal.id: compute_deal_metrics(deal) for deal in deals}

    portfolio = compute_portfolio_analytics(deals, metrics_by_id=metrics_by_id)
    bridge = compute_bridge_aggregate(deals, basis="fund")
    risk = compute_loss_and_distribution(deals, metrics_by_id=metrics_by_id)
    dimension_groups = _build_dimension_groups(deals, metrics_by_id)

    top_5, bottom_5 = build_top_bottom_deals_by_value_created(deals, metrics_by_id=metrics_by_id, n=5)
    realized_value = sum(metrics_by_id[deal.id].get("realized") or 0.0 for deal in deals)
    unrealized_value = sum(metrics_by_id[deal.id].get("unrealized") or 0.0 for deal in deals)
    total_value = portfolio["total_value"]

    bridge_table_rows = []
    for driver in DRIVERS:
        bridge_table_rows.append(
            {
                "driver": driver,
                "label": DRIVER_LABELS[driver],
                "dollar": bridge["drivers"]["dollar"].get(driver),
                "moic": bridge["drivers"]["moic"].get(driver),
                "pct": bridge["drivers"]["pct"].get(driver),
            }
        )

    return {
        "meta": {
            "fund_scope": "All Funds",
            "as_of": date.today().isoformat(),
            "filters_applied": {},
            "deal_count": len(deals),
        },
        "executive": {
            "total_equity": portfolio["total_equity"],
            "realized_value": realized_value,
            "unrealized_value": unrealized_value,
            "total_value": total_value,
            "gross_moic": portfolio["returns"]["gross_moic"]["avg"],
            "implied_irr_wtd": portfolio["returns"]["implied_irr"]["wavg"],
            "total_value_created": portfolio["total_value_created"],
            "realized_pct_of_value": safe_divide(realized_value, total_value),
            "unrealized_pct_of_value": safe_divide(unrealized_value, total_value),
            "top_5_deals": top_5,
            "bottom_5_deals": bottom_5,
        },
        "bridge": {
            "ready_count": bridge["ready_count"],
            "start_end": bridge["start_end"],
            "drivers": bridge["drivers"],
            "table_rows": bridge_table_rows,
        },
        "risk": {
            "loss_ratio_count_pct": risk["loss_ratios"]["count_pct"],
            "loss_ratio_capital_pct": risk["loss_ratios"]["capital_pct"],
            "loss_count": risk["loss_ratios"]["loss_count"],
            "loss_total": risk["loss_ratios"]["total_count"],
            "leverage_entry_exit": {
                "net_debt_ebitda": {
                    "entry_avg": portfolio["entry"]["net_debt_ebitda"]["avg"],
                    "exit_avg": portfolio["exit"]["net_debt_ebitda"]["avg"],
                    "entry_wtd": portfolio["entry"]["net_debt_ebitda"]["wavg"],
                    "exit_wtd": portfolio["exit"]["net_debt_ebitda"]["wavg"],
                },
                "net_debt_tev": {
                    "entry_avg": portfolio["entry"]["net_debt_tev"]["avg"],
                    "exit_avg": portfolio["exit"]["net_debt_tev"]["avg"],
                    "entry_wtd": portfolio["entry"]["net_debt_tev"]["wavg"],
                    "exit_wtd": portfolio["exit"]["net_debt_tev"]["wavg"],
                },
            },
        },
        "operating": {
            "multiples": {
                "tev_ebitda": {
                    "entry_avg": portfolio["entry"]["tev_ebitda"]["avg"],
                    "exit_avg": portfolio["exit"]["tev_ebitda"]["avg"],
                    "entry_wtd": portfolio["entry"]["tev_ebitda"]["wavg"],
                    "exit_wtd": portfolio["exit"]["tev_ebitda"]["wavg"],
                },
                "tev_revenue": {
                    "entry_avg": portfolio["entry"]["tev_revenue"]["avg"],
                    "exit_avg": portfolio["exit"]["tev_revenue"]["avg"],
                    "entry_wtd": portfolio["entry"]["tev_revenue"]["wavg"],
                    "exit_wtd": portfolio["exit"]["tev_revenue"]["wavg"],
                },
            },
            "margin": {
                "ebitda_margin": {
                    "entry_avg": portfolio["entry"]["ebitda_margin"]["avg"],
                    "exit_avg": portfolio["exit"]["ebitda_margin"]["avg"],
                    "entry_wtd": portfolio["entry"]["ebitda_margin"]["wavg"],
                    "exit_wtd": portfolio["exit"]["ebitda_margin"]["wavg"],
                },
            },
            "growth": {
                "revenue_growth": {
                    "avg": portfolio["growth"]["revenue_growth"]["avg"],
                    "wavg": portfolio["growth"]["revenue_growth"]["wavg"],
                },
                "ebitda_growth": {
                    "avg": portfolio["growth"]["ebitda_growth"]["avg"],
                    "wavg": portfolio["growth"]["ebitda_growth"]["wavg"],
                },
                "revenue_cagr": {
                    "avg": portfolio["growth"]["revenue_cagr"]["avg"],
                    "wavg": portfolio["growth"]["revenue_cagr"]["wavg"],
                },
                "ebitda_cagr": {
                    "avg": portfolio["growth"]["ebitda_cagr"]["avg"],
                    "wavg": portfolio["growth"]["ebitda_cagr"]["wavg"],
                },
            },
        },
        "slicing": {
            "dimensions": _dimension_payload(
                dimension_groups,
                ranking_basis=ranking_basis,
                decile_pct=decile_pct,
                decile_min=decile_min,
            ),
        },
        "team": {
            "lead_partner_table": _build_lead_partner_table(deals, metrics_by_id=metrics_by_id, dimension_groups=dimension_groups),
            "entry_channel_table": _build_entry_channel_table(dimension_groups),
        },
    }

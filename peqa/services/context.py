from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any

from models import BenchmarkPoint, db
from sqlalchemy.exc import SQLAlchemyError
from peqa.services.filtering import (
    apply_deal_filters,
    build_deal_scope_query,
    build_filter_options,
    parse_request_filters,
)
from services.metrics.common import resolve_analysis_as_of_date
from services.metrics.deal import compute_deal_metrics


logger = logging.getLogger(__name__)


@dataclass
class AnalysisContext:
    membership: Any
    team_id: int | None
    active_team: Any
    active_firm: Any
    firm_id: int | None
    filters: dict[str, str]
    benchmark_asset_class: str
    display_as_of_date: Any
    reporting: dict[str, Any]
    deals: list[Any]
    metrics_by_id: dict[int, dict[str, Any]]
    funds: list[str] = field(default_factory=list)
    statuses: list[str] = field(default_factory=list)
    sectors: list[str] = field(default_factory=list)
    geographies: list[str] = field(default_factory=list)
    vintages: list[int] = field(default_factory=list)
    exit_types: list[str] = field(default_factory=list)
    lead_partners: list[str] = field(default_factory=list)
    security_types: list[str] = field(default_factory=list)
    deal_types: list[str] = field(default_factory=list)
    entry_channels: list[str] = field(default_factory=list)
    benchmark_asset_classes: list[str] = field(default_factory=list)

    def as_legacy_dict(self):
        return {
            "firm_id": self.firm_id,
            "deals": self.deals,
            "funds": self.funds,
            "statuses": self.statuses,
            "sectors": self.sectors,
            "geographies": self.geographies,
            "vintages": self.vintages,
            "exit_types": self.exit_types,
            "lead_partners": self.lead_partners,
            "security_types": self.security_types,
            "deal_types": self.deal_types,
            "entry_channels": self.entry_channels,
            "current_fund": self.filters.get("fund", ""),
            "current_status": self.filters.get("status", ""),
            "current_sector": self.filters.get("sector", ""),
            "current_geography": self.filters.get("geography", ""),
            "current_vintage": self.filters.get("vintage", ""),
            "current_exit_type": self.filters.get("exit_type", ""),
            "current_lead_partner": self.filters.get("lead_partner", ""),
            "current_security_type": self.filters.get("security_type", ""),
            "current_deal_type": self.filters.get("deal_type", ""),
            "current_entry_channel": self.filters.get("entry_channel", ""),
            "display_as_of_date": self.display_as_of_date,
            "benchmark_asset_classes": self.benchmark_asset_classes,
            "current_benchmark_asset_class": self.benchmark_asset_class,
            "active_firm": self.active_firm,
            "active_team": self.active_team,
            "active_membership": self.membership,
            "reporting": self.reporting,
            "metrics_by_id": self.metrics_by_id,
        }


def benchmark_asset_classes_for_team(team_id):
    if team_id is None:
        return []
    try:
        rows = (
            BenchmarkPoint.query.with_entities(BenchmarkPoint.asset_class)
            .filter(BenchmarkPoint.team_id == team_id)
            .distinct()
            .order_by(BenchmarkPoint.asset_class.asc())
            .all()
        )
    except SQLAlchemyError as exc:
        db.session.rollback()
        logger.exception(
            "Benchmark asset class lookup failed [%s]: %s",
            type(exc).__name__,
            getattr(exc, "orig", exc),
        )
        return []
    return [row[0] for row in rows if row[0]]


def resolve_benchmark_asset_class(session_store, request_values, benchmark_asset_classes, session_key="selected_benchmark_asset_class"):
    requested = request_values.get("benchmark_asset_class")
    if requested is not None:
        selected = (requested or "").strip()
        if selected and selected not in benchmark_asset_classes:
            selected = ""
        session_store[session_key] = selected
        return selected

    selected = (session_store.get(session_key, "") or "").strip()
    if selected and selected not in benchmark_asset_classes:
        selected = ""
        session_store[session_key] = ""
    return selected


def load_team_benchmark_thresholds(team_id, asset_class, strategy=None, region=None, size_bucket=None):
    asset = (asset_class or "").strip()
    if team_id is None or not asset:
        return {}

    requested_dims = {
        "strategy": (strategy or "").strip() or None,
        "region": (region or "").strip() or None,
        "size_bucket": (size_bucket or "").strip() or None,
    }
    try:
        rows = (
            BenchmarkPoint.query.filter(BenchmarkPoint.team_id == team_id, BenchmarkPoint.asset_class == asset)
            .order_by(BenchmarkPoint.vintage_year.asc(), BenchmarkPoint.metric.asc(), BenchmarkPoint.quartile.asc())
            .all()
        )
    except SQLAlchemyError as exc:
        db.session.rollback()
        logger.exception(
            "Benchmark threshold lookup failed [%s]: %s",
            type(exc).__name__,
            getattr(exc, "orig", exc),
        )
        return {}

    thresholds = {}
    scores = {}
    for row in rows:
        score = 0
        matched = True
        for key in ("strategy", "region", "size_bucket"):
            row_value = getattr(row, key, None)
            requested = requested_dims[key]
            if row_value:
                if requested is None or row_value.strip().lower() != requested.strip().lower():
                    matched = False
                    break
                score += 1
        if not matched:
            continue

        bucket_key = (int(row.vintage_year), row.metric, row.quartile)
        if score < scores.get(bucket_key, -1):
            continue
        scores[bucket_key] = score
        vintage_bucket = thresholds.setdefault(int(row.vintage_year), {})
        metric_bucket = vintage_bucket.setdefault(row.metric, {})
        metric_bucket[row.quartile] = row.value

    return thresholds


def build_analysis_context(
    *,
    membership,
    active_team,
    active_firm,
    request_values,
    session_store,
    reporting,
    fund_override=None,
):
    team_id = membership.team_id if membership is not None else None
    firm_id = active_firm.id if active_firm is not None else None
    all_deals = build_deal_scope_query(team_id=team_id, firm_id=firm_id).all()
    options = build_filter_options(all_deals)

    filters = parse_request_filters(request_values, fund_override=fund_override)
    if filters["fund"] and filters["fund"] not in options["funds"]:
        filters["fund"] = ""

    benchmark_asset_classes = benchmark_asset_classes_for_team(team_id)
    benchmark_asset_class = resolve_benchmark_asset_class(session_store, request_values, benchmark_asset_classes)

    deals = apply_deal_filters(
        build_deal_scope_query(team_id=team_id, firm_id=firm_id),
        filters,
    ).all()
    display_as_of_date = resolve_analysis_as_of_date(deals)
    metrics_by_id = {deal.id: compute_deal_metrics(deal) for deal in deals}

    return AnalysisContext(
        membership=membership,
        team_id=team_id,
        active_team=active_team,
        active_firm=active_firm,
        firm_id=firm_id,
        filters=filters,
        benchmark_asset_class=benchmark_asset_class,
        display_as_of_date=display_as_of_date,
        reporting=reporting,
        deals=deals,
        metrics_by_id=metrics_by_id,
        funds=options["funds"],
        statuses=options["statuses"],
        sectors=options["sectors"],
        geographies=options["geographies"],
        vintages=options["vintages"],
        exit_types=options["exit_types"],
        lead_partners=options["lead_partners"],
        security_types=options["security_types"],
        deal_types=options["deal_types"],
        entry_channels=options["entry_channels"],
        benchmark_asset_classes=benchmark_asset_classes,
    )

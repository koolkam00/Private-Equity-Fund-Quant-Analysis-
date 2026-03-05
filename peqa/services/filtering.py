from __future__ import annotations

from sqlalchemy import and_, or_

from models import Deal, db


FILTER_KEYS = (
    "fund",
    "status",
    "sector",
    "geography",
    "vintage",
    "exit_type",
    "lead_partner",
    "security_type",
    "deal_type",
    "entry_channel",
)


def deal_vintage_year(deal):
    if getattr(deal, "year_invested", None) is not None:
        try:
            return int(deal.year_invested)
        except (TypeError, ValueError):
            return None
    if getattr(deal, "investment_date", None) is not None:
        return int(deal.investment_date.year)
    return None


def parse_request_filters(values, fund_override=None):
    filters = {
        "fund": (fund_override if fund_override is not None else values.get("fund", "")) or "",
        "status": (values.get("status", "") or "").strip(),
        "sector": (values.get("sector", "") or "").strip(),
        "geography": (values.get("geography", "") or "").strip(),
        "vintage": (values.get("vintage", "") or "").strip(),
        "exit_type": (values.get("exit_type", "") or "").strip(),
        "lead_partner": (values.get("lead_partner", "") or "").strip(),
        "security_type": (values.get("security_type", "") or "").strip(),
        "deal_type": (values.get("deal_type", "") or "").strip(),
        "entry_channel": (values.get("entry_channel", "") or "").strip(),
    }
    filters["fund"] = (filters["fund"] or "").strip()
    return filters


def build_deal_scope_query(team_id=None, firm_id=None):
    if firm_id is None:
        return Deal.query.filter(Deal.id == -1)

    query = Deal.query.filter(Deal.firm_id == firm_id)
    if team_id is not None:
        query = query.filter(or_(Deal.team_id.is_(None), Deal.team_id == team_id))
    return query


def apply_deal_filters(query, filters):
    filters = filters or {}
    if filters.get("fund"):
        query = query.filter(Deal.fund_number == filters["fund"])
    if filters.get("status"):
        query = query.filter(Deal.status == filters["status"])
    if filters.get("sector"):
        query = query.filter(Deal.sector == filters["sector"])
    if filters.get("geography"):
        query = query.filter(db.func.coalesce(Deal.geography, "Unknown") == filters["geography"])
    if filters.get("exit_type"):
        query = query.filter(db.func.coalesce(Deal.exit_type, "Not Specified") == filters["exit_type"])
    if filters.get("lead_partner"):
        query = query.filter(db.func.coalesce(Deal.lead_partner, "Unassigned") == filters["lead_partner"])
    if filters.get("security_type"):
        query = query.filter(db.func.coalesce(Deal.security_type, "Common Equity") == filters["security_type"])
    if filters.get("deal_type"):
        query = query.filter(db.func.coalesce(Deal.deal_type, "Platform") == filters["deal_type"])
    if filters.get("entry_channel"):
        query = query.filter(db.func.coalesce(Deal.entry_channel, "Unknown") == filters["entry_channel"])
    if filters.get("vintage"):
        try:
            vintage_int = int(filters["vintage"])
        except (TypeError, ValueError):
            return query.filter(Deal.id == -1)
        query = query.filter(
            or_(
                Deal.year_invested == vintage_int,
                and_(
                    Deal.year_invested.is_(None),
                    db.extract("year", Deal.investment_date) == vintage_int,
                ),
            )
        )
    return query


def build_filter_options(deals):
    funds = sorted({d.fund_number for d in deals if d.fund_number})
    statuses = sorted({d.status for d in deals if d.status})
    sectors = sorted({d.sector for d in deals if d.sector})
    geographies = sorted({d.geography for d in deals if d.geography})
    vintages = sorted({deal_vintage_year(d) for d in deals if deal_vintage_year(d) is not None})
    exit_types = sorted({d.exit_type or "Not Specified" for d in deals})
    lead_partners = sorted({d.lead_partner or "Unassigned" for d in deals})
    security_types = sorted({d.security_type or "Common Equity" for d in deals})
    deal_types = sorted({d.deal_type or "Platform" for d in deals})
    entry_channels = sorted({d.entry_channel or "Unknown" for d in deals})
    return {
        "funds": funds,
        "statuses": statuses,
        "sectors": sectors,
        "geographies": geographies,
        "vintages": vintages,
        "exit_types": exit_types,
        "lead_partners": lead_partners,
        "security_types": security_types,
        "deal_types": deal_types,
        "entry_channels": entry_channels,
    }

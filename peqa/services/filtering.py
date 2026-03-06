from __future__ import annotations

from sqlalchemy import and_, or_

from models import Deal, FundMetadata, db


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


def _normalized_fund_name(value):
    return (value or "Unknown Fund").strip() or "Unknown Fund"


def _coerce_vintage_year(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_fund_vintage_lookup(deals, team_id=None, firm_id=None, fund_names=None, fund_metadata=None):
    lookup = {}
    normalized_funds = {_normalized_fund_name(name) for name in (fund_names or []) if name is not None}

    metadata_map = {}
    if isinstance(fund_metadata, dict):
        metadata_map = {
            _normalized_fund_name(name): row
            for name, row in fund_metadata.items()
            if row is not None
        }
    elif fund_metadata:
        metadata_map = {
            _normalized_fund_name(getattr(row, "fund_number", None)): row
            for row in fund_metadata
            if row is not None and getattr(row, "fund_number", None)
        }
    elif team_id is not None and firm_id is not None:
        query = FundMetadata.query.filter(
            FundMetadata.team_id == team_id,
            FundMetadata.firm_id == firm_id,
        )
        if normalized_funds:
            query = query.filter(FundMetadata.fund_number.in_(sorted(normalized_funds)))
        rows = query.order_by(FundMetadata.fund_number.asc(), FundMetadata.id.asc()).all()
        metadata_map = {_normalized_fund_name(row.fund_number): row for row in rows if row.fund_number}

    for fund_name, row in metadata_map.items():
        vintage_year = _coerce_vintage_year(getattr(row, "vintage_year", None))
        if vintage_year is not None:
            lookup[fund_name] = vintage_year

    for deal in deals or []:
        fund_name = _normalized_fund_name(getattr(deal, "fund_number", None))
        if normalized_funds and fund_name not in normalized_funds:
            continue
        if fund_name in lookup:
            continue
        vintage_year = deal_vintage_year(deal)
        if vintage_year is None:
            continue
        existing = lookup.get(fund_name)
        if existing is None or vintage_year < existing:
            lookup[fund_name] = vintage_year

    return lookup


def fund_vintage_sort_key(fund_name, vintage_lookup=None):
    normalized = _normalized_fund_name(fund_name)
    vintage_year = None if vintage_lookup is None else _coerce_vintage_year(vintage_lookup.get(normalized))
    return (
        vintage_year is None,
        vintage_year if vintage_year is not None else 9999,
        normalized.lower(),
    )


def sort_fund_rows_by_vintage(fund_rows, vintage_lookup=None, fund_key_candidates=("fund_number", "fund_name"), vintage_key="vintage_year"):
    def _row_value(row, key):
        if isinstance(row, dict):
            return row.get(key)
        return getattr(row, key, None)

    def _row_fund_name(row):
        for key in fund_key_candidates:
            value = _row_value(row, key)
            if value:
                return _normalized_fund_name(value)
        return "Unknown Fund"

    def _row_vintage_year(row):
        direct = _coerce_vintage_year(_row_value(row, vintage_key))
        if direct is not None:
            return direct
        if vintage_lookup is None:
            return None
        return _coerce_vintage_year(vintage_lookup.get(_row_fund_name(row)))

    return sorted(
        fund_rows,
        key=lambda row: (
            _row_vintage_year(row) is None,
            _row_vintage_year(row) if _row_vintage_year(row) is not None else 9999,
            _row_fund_name(row).lower(),
        ),
    )


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

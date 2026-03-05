import os
import tempfile
import uuid
from datetime import date

import pandas as pd
import pytest

from models import (
    Deal,
    DealCashflowEvent,
    DealQuarterSnapshot,
    DealUnderwriteBaseline,
    Firm,
    FundQuarterSnapshot,
    Team,
    TeamFirmAccess,
    UploadIssue,
    db,
)
from services.deal_parser import parse_deals


@pytest.fixture(autouse=True)
def stub_fx_resolver(monkeypatch):
    rates = {"EUR": 1.10, "GBP": 1.25, "CAD": 0.75}

    def _fake_resolver(currency_code, as_of_date):
        code = (currency_code or "USD").upper()
        if code == "USD":
            return {
                "ok": True,
                "rate": 1.0,
                "effective_date": as_of_date,
                "source": "Identity",
                "warning": None,
                "currency_code": code,
            }
        rate = rates.get(code)
        if rate is None:
            return {
                "ok": False,
                "rate": None,
                "effective_date": None,
                "source": "Frankfurter (ECB)",
                "warning": f"FX lookup failed for {code}",
                "currency_code": code,
            }
        return {
            "ok": True,
            "rate": rate,
            "effective_date": as_of_date if isinstance(as_of_date, date) else date.today(),
            "source": "Frankfurter (ECB)",
            "warning": None,
            "currency_code": code,
        }

    monkeypatch.setattr("services.deal_parser.resolve_rate_to_usd", _fake_resolver)


def create_temp_excel(data, sheet_name="Sheet1"):
    df = pd.DataFrame(data)
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return path


def create_temp_workbook(sheets):
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, data in sheets.items():
            pd.DataFrame(data).to_excel(writer, index=False, sheet_name=sheet_name)
    return path


def create_team(name_prefix="Parser Team"):
    suffix = uuid.uuid4().hex[:8]
    team = Team(name=f"{name_prefix} {suffix}", slug=f"{name_prefix.lower().replace(' ', '-')}-{suffix}")
    db.session.add(team)
    db.session.commit()
    return team


def create_firm(name_prefix="Parser Firm"):
    suffix = uuid.uuid4().hex[:8]
    firm = Firm(name=f"{name_prefix} {suffix}", slug=f"{name_prefix.lower().replace(' ', '-')}-{suffix}")
    db.session.add(firm)
    db.session.commit()
    return firm


def _with_firm_name(data, firm_name):
    cols = list(data.keys())
    row_count = len(data[cols[0]]) if cols else 0
    if "As Of Date" not in data:
        data = {"As Of Date": [date(2025, 12, 31)] * row_count, **data}
    if "Firm Name" not in data:
        data = {"Firm Name": [firm_name] * row_count, **data}
    return data


def test_parse_deals_valid(app_context):
    team = create_team()
    firm_name = "Firm Alpha"
    data = _with_firm_name(
        {
            "Company Name": ["Company A", "Company B"],
            "Fund": ["Fund I", "Fund II"],
            "Sector": ["Tech", "Health"],
            "Geography": ["US", "UK"],
            "Exit Type": ["Strategic Sale", "Secondary Buyout"],
            "Lead Partner": ["Jane Doe", "Alex Reed"],
            "Security Type": ["Common Equity", "Preferred Equity"],
            "Deal Type": ["Platform", "Add-on"],
            "Entry Channel": ["Proprietary", "Limited Auction"],
            "Year Invested": [2020, 2021],
            "Entry EV": [100, 200],
            "Entry EBITDA": [10, 20],
            "Exit EV": [150, 260],
            "Exit EBITDA": [15, 30],
            "Equity Invested": [50, 100],
            "Fund Size": [500, 600],
            "Realized Value": [80, 120],
            "Unrealized Value": [0, 20],
            "Net IRR": [0.22, 0.18],
            "Net MOIC": [2.1, 1.7],
            "DPI": [1.9, 1.3],
        },
        firm_name,
    )
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 2
        deal = Deal.query.filter_by(company_name="Company A", firm_id=result["firm_id"], team_id=team.id).first()
        assert deal is not None
        assert deal.geography == "US"
        assert deal.year_invested == 2020
        assert deal.exit_type == "Strategic Sale"
        assert deal.lead_partner == "Jane Doe"
        assert deal.security_type == "Common Equity"
        assert deal.deal_type == "Platform"
        assert deal.entry_channel == "Proprietary"
        assert abs(deal.fund_size - 500.0) < 1e-9
        assert abs(deal.net_irr - 0.22) < 1e-9
        assert abs(deal.net_moic - 2.1) < 1e-9
        assert abs(deal.net_dpi - 1.9) < 1e-9
        assert deal.as_of_date == date(2025, 12, 31)
        assert result["firm_name"] == firm_name
        assert result["firm_currency"] == "USD"
        assert result["fx_status"] == "ok"
        assert result["fx_rate_to_usd"] == 1.0
        assert result["fx_warning"] is None
        assert result["as_of_date"] == date(2025, 12, 31)
        firm = Firm.query.filter_by(id=result["firm_id"]).first()
        assert firm is not None
        assert firm.base_currency == "USD"
        assert firm.fx_rate_to_usd == 1.0
        assert firm.fx_last_status == "ok"
        assert TeamFirmAccess.query.filter_by(team_id=team.id, firm_id=result["firm_id"]).count() == 1
    finally:
        os.remove(file_path)


def test_parse_deals_requires_firm_name_column(app_context):
    team = create_team()
    data = {
        "Company Name": ["Missing Firm Co"],
        "Fund": ["Fund I"],
        "Equity Invested": [100],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 0
        assert any("Firm Name" in msg for msg in result["errors"])
    finally:
        os.remove(file_path)


def test_parse_deals_requires_as_of_date_column(app_context):
    team = create_team()
    data = {
        "Firm Name": ["Missing As Of Firm"],
        "Company Name": ["Missing As Of Co"],
        "Fund": ["Fund I"],
        "Equity Invested": [100],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 0
        assert any("As Of Date" in msg for msg in result["errors"])
    finally:
        os.remove(file_path)


def test_parse_deals_rejects_missing_as_of_date_values(app_context):
    team = create_team()
    data = {
        "Firm Name": ["Firm As Of", "Firm As Of"],
        "As Of Date": [date(2025, 12, 31), None],
        "Company Name": ["Co A", "Co B"],
        "Fund": ["Fund I", "Fund I"],
        "Equity Invested": [100, 120],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 0
        assert any("As Of Date is required" in msg for msg in result["errors"])
    finally:
        os.remove(file_path)


def test_parse_deals_rejects_mixed_as_of_dates(app_context):
    team = create_team()
    data = {
        "Firm Name": ["Firm As Of", "Firm As Of"],
        "As Of Date": [date(2025, 12, 31), date(2026, 1, 31)],
        "Company Name": ["Co A", "Co B"],
        "Fund": ["Fund I", "Fund I"],
        "Equity Invested": [100, 120],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 0
        assert any("exactly one As Of Date" in msg for msg in result["errors"])
    finally:
        os.remove(file_path)


def test_parse_deals_allows_multiple_firms_per_workbook(app_context):
    team = create_team()
    data = {
        "Firm Name": ["Firm A", "Firm B"],
        "As Of Date": [date(2025, 12, 31), date(2026, 1, 31)],
        "Firm Currency": ["USD", "EUR"],
        "Company Name": ["Co A", "Co B"],
        "Fund": ["Fund I", "Fund II"],
        "Equity Invested": [100, 120],
        "Realized Value": [130, 140],
        "Unrealized Value": [0, 0],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 2
        assert result["firm_count"] == 2
        assert len(result["firms_processed"]) == 2
        assert result["firm_name"] is None
        assert result["firm_currency"] is None
        assert any(row["firm_name"] == "Firm A" and row["currency"] == "USD" for row in result["firms_processed"])
        assert any(row["firm_name"] == "Firm B" and row["currency"] == "EUR" for row in result["firms_processed"])
        firm_a = Firm.query.filter_by(name="Firm A").first()
        firm_b = Firm.query.filter_by(name="Firm B").first()
        assert firm_a is not None and firm_b is not None
        assert Deal.query.filter_by(firm_id=firm_a.id, company_name="Co A", team_id=team.id).count() == 1
        assert Deal.query.filter_by(firm_id=firm_b.id, company_name="Co B", team_id=team.id).count() == 1
    finally:
        os.remove(file_path)


def test_parse_deals_auto_creates_unknown_firm(app_context):
    team = create_team()
    firm_name = f"Auto Firm {uuid.uuid4().hex[:6]}"
    data = _with_firm_name(
        {
            "Company Name": ["Auto Co"],
            "Fund": ["Fund Auto"],
            "Equity Invested": [100],
            "Realized Value": [130],
            "Unrealized Value": [0],
        },
        firm_name,
    )
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        firm = Firm.query.filter_by(name=firm_name).first()
        assert firm is not None
        assert result["firm_id"] == firm.id
        assert firm.base_currency == "USD"
        assert Deal.query.filter_by(company_name="Auto Co", firm_id=firm.id, team_id=team.id).count() == 1
        assert TeamFirmAccess.query.filter_by(team_id=team.id, firm_id=firm.id).count() == 1
    finally:
        os.remove(file_path)


def test_parse_deals_new_firm_not_auto_granted_to_other_teams(app_context):
    team_a = create_team("Team A")
    team_b = create_team("Team B")
    team_a_id = team_a.id
    team_b_id = team_b.id
    firm_name = f"Scoped Firm {uuid.uuid4().hex[:6]}"
    data = _with_firm_name(
        {
            "Company Name": ["Scoped Co"],
            "Fund": ["Fund Scoped"],
            "Equity Invested": [100],
            "Realized Value": [125],
            "Unrealized Value": [0],
        },
        firm_name,
    )
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team_a_id)
        assert result["success"] == 1
        assert TeamFirmAccess.query.filter_by(team_id=team_a_id, firm_id=result["firm_id"]).count() == 1
        assert TeamFirmAccess.query.filter_by(team_id=team_b_id, firm_id=result["firm_id"]).count() == 0
    finally:
        os.remove(file_path)


def test_parse_deals_normalizes_lowercase_firm_currency(app_context):
    team = create_team()
    firm_name = f"Currency Firm {uuid.uuid4().hex[:6]}"
    data = {
        "Firm Name": [firm_name],
        "As Of Date": [date(2025, 12, 31)],
        "Firm Currency": ["eur"],
        "Company Name": ["FX Co"],
        "Fund": ["Fund FX"],
        "Equity Invested": [100],
        "Realized Value": [130],
        "Unrealized Value": [0],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        assert result["firm_currency"] == "EUR"
        assert result["fx_status"] == "ok"
        assert abs(result["fx_rate_to_usd"] - 1.10) < 1e-9
        assert result["fx_warning"] is None
        firm = Firm.query.filter_by(id=result["firm_id"]).first()
        assert firm is not None
        assert firm.base_currency == "EUR"
        assert abs(firm.fx_rate_to_usd - 1.10) < 1e-9
        assert firm.fx_last_status == "ok"
    finally:
        os.remove(file_path)


def test_parse_deals_rejects_mixed_firm_currencies(app_context):
    team = create_team()
    data = {
        "Firm Name": ["Firm Multi CCY", "Firm Multi CCY"],
        "As Of Date": [date(2025, 12, 31), date(2025, 12, 31)],
        "Firm Currency": ["USD", "EUR"],
        "Company Name": ["Co A", "Co B"],
        "Fund": ["Fund I", "Fund I"],
        "Equity Invested": [100, 120],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 0
        assert any("exactly one Firm Currency" in msg for msg in result["errors"])
    finally:
        os.remove(file_path)


def test_parse_deals_allows_different_firm_currency_and_as_of_between_firms(app_context):
    team = create_team()
    data = {
        "Firm Name": ["Firm Alpha", "Firm Beta"],
        "As Of Date": [date(2025, 12, 31), date(2026, 3, 31)],
        "Firm Currency": ["USD", "GBP"],
        "Company Name": ["Alpha Co", "Beta Co"],
        "Fund": ["Fund 1", "Fund 2"],
        "Equity Invested": [100, 100],
        "Realized Value": [115, 135],
        "Unrealized Value": [0, 0],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 2
        assert result["firm_count"] == 2
        as_of_map = {row["firm_name"]: row["as_of_date"] for row in result["firms_processed"]}
        ccy_map = {row["firm_name"]: row["currency"] for row in result["firms_processed"]}
        assert as_of_map["Firm Alpha"] == date(2025, 12, 31)
        assert as_of_map["Firm Beta"] == date(2026, 3, 31)
        assert ccy_map["Firm Alpha"] == "USD"
        assert ccy_map["Firm Beta"] == "GBP"
    finally:
        os.remove(file_path)


def test_parse_deals_rejects_invalid_firm_currency(app_context):
    team = create_team()
    data = {
        "Firm Name": ["Firm Invalid CCY"],
        "As Of Date": [date(2025, 12, 31)],
        "Firm Currency": ["USDX"],
        "Company Name": ["Co A"],
        "Fund": ["Fund I"],
        "Equity Invested": [100],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 0
        assert any("Firm Currency must be a valid ISO-3 code" in msg for msg in result["errors"])
    finally:
        os.remove(file_path)


def test_parse_deals_updates_existing_firm_currency_from_upload(app_context):
    team = create_team()
    firm = create_firm("Update Currency Firm")
    firm.base_currency = "USD"
    db.session.commit()

    data = {
        "Firm Name": [firm.name],
        "As Of Date": [date(2025, 12, 31)],
        "Firm Currency": ["GBP"],
        "Company Name": ["Co New"],
        "Fund": ["Fund U"],
        "Equity Invested": [100],
        "Realized Value": [120],
        "Unrealized Value": [0],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        assert result["firm_currency"] == "GBP"
        assert result["fx_status"] == "ok"
        assert abs(result["fx_rate_to_usd"] - 1.25) < 1e-9
        refreshed = Firm.query.filter_by(id=firm.id).first()
        assert refreshed is not None
        assert refreshed.base_currency == "GBP"
        assert abs(refreshed.fx_rate_to_usd - 1.25) < 1e-9
        assert refreshed.fx_last_status == "ok"
    finally:
        os.remove(file_path)


def test_parse_deals_allows_upload_when_fx_lookup_fails(app_context, monkeypatch):
    team = create_team()
    firm_name = f"NoFX Firm {uuid.uuid4().hex[:6]}"

    def _fail_fx(currency_code, as_of_date):
        return {
            "ok": False,
            "rate": None,
            "effective_date": None,
            "source": "Frankfurter (ECB)",
            "warning": "FX service unavailable",
            "currency_code": currency_code,
        }

    monkeypatch.setattr("services.deal_parser.resolve_rate_to_usd", _fail_fx)

    data = {
        "Firm Name": [firm_name],
        "As Of Date": [date(2025, 12, 31)],
        "Firm Currency": ["EUR"],
        "Company Name": ["NoFX Co"],
        "Fund": ["Fund FX"],
        "Equity Invested": [100],
        "Realized Value": [120],
        "Unrealized Value": [0],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        assert result["firm_currency"] == "EUR"
        assert result["fx_status"] == "lookup_failed"
        assert result["fx_rate_to_usd"] is None
        assert result["fx_warning"] is not None
        assert "showing native currency values" in result["fx_warning"].lower()
        firm = Firm.query.filter_by(id=result["firm_id"]).first()
        assert firm is not None
        assert firm.base_currency == "EUR"
        assert firm.fx_rate_to_usd is None
        assert firm.fx_last_status == "lookup_failed"
    finally:
        os.remove(file_path)


def test_parse_deals_fallback_geography_and_vintage(app_context):
    team = create_team()
    firm_name = "Firm Fallback"
    data = _with_firm_name(
        {
            "Company Name": ["Fallback Co"],
            "Investment Date": ["2022-06-01"],
            "Equity Invested": [100],
            "Realized Value": [0],
            "Unrealized Value": [120],
        },
        firm_name,
    )
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        deal = Deal.query.filter_by(company_name="Fallback Co", firm_id=result["firm_id"], team_id=team.id).first()
        assert deal is not None
        assert deal.geography == "Unknown"
        assert deal.year_invested == 2022
    finally:
        os.remove(file_path)


def test_parse_deals_quarantine_invalid_row(app_context):
    team = create_team()
    firm_name = "Firm Invalid"
    data = _with_firm_name(
        {
            "Company Name": ["Bad Deal"],
            "Investment Date": ["2024-01-01"],
            "Exit Date": ["2023-01-01"],
            "Equity Invested": [100],
        },
        firm_name,
    )
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id, replace_mode="append")
        assert result["success"] == 0
        assert result["quarantined_count"] == 1
        assert UploadIssue.query.filter_by(firm_id=result["firm_id"], team_id=team.id).count() >= 1
    finally:
        os.remove(file_path)


def test_parse_deals_duplicate_detection(app_context):
    team = create_team()
    firm = create_firm("Dup Firm")
    db.session.add(Deal(company_name="DupCo", fund_number="Fund I", firm_id=firm.id, team_id=team.id))
    db.session.commit()

    data = _with_firm_name(
        {
            "Company Name": ["DupCo"],
            "Fund": ["Fund I"],
            "Equity Invested": [100],
        },
        firm.name,
    )
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id, replace_mode="append")
        assert result["duplicates_skipped"] == 1
        assert result["success"] == 0
    finally:
        os.remove(file_path)


def test_parse_deals_optional_track_record_numeric_coercion(app_context):
    team = create_team()
    firm_name = "Firm Numeric"
    data = _with_firm_name(
        {
            "Company Name": ["Numeric Co"],
            "Fund": ["Fund X"],
            "Equity Invested": [100],
            "Fund Size": ["540.0"],
            "Net IRR": ["0.183"],
            "Net MOIC": ["2.1"],
            "DPI": ["1.2"],
        },
        firm_name,
    )
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        deal = Deal.query.filter_by(company_name="Numeric Co", firm_id=result["firm_id"], team_id=team.id).first()
        assert deal is not None
        assert abs(deal.fund_size - 540.0) < 1e-9
        assert abs(deal.net_irr - 0.183) < 1e-9
        assert abs(deal.net_moic - 2.1) < 1e-9
        assert abs(deal.net_dpi - 1.2) < 1e-9
    finally:
        os.remove(file_path)


def test_parse_deals_multisheet_optional_sections(app_context):
    team = create_team()
    firm_name = "Firm Multi"
    sheets = {
        "Deals": {
            "Firm Name": [firm_name],
            "As Of Date": [date(2025, 12, 31)],
            "Company Name": ["Multi Co"],
            "Fund": ["Fund IX"],
            "Investment Date": ["2021-01-01"],
            "Equity Invested": [100],
            "Realized Value": [20],
            "Unrealized Value": [130],
        },
        "Cashflows": {
            "Company Name": ["Multi Co", "Multi Co"],
            "Fund": ["Fund IX", "Fund IX"],
            "Event Date": ["2021-03-31", "2023-12-31"],
            "Event Type": ["Capital Call", "Distribution"],
            "Amount": [-40, 12],
            "Notes": ["Q1 call", "partial realization"],
        },
        "Deal Quarterly": {
            "Company Name": ["Multi Co", "Multi Co"],
            "Fund": ["Fund IX", "Fund IX"],
            "Quarter End": ["2021-12-31", "2022-12-31"],
            "Revenue": [50, 58],
            "EBITDA": [10, 12],
            "Enterprise Value": [150, 170],
            "Net Debt": [35, 32],
            "Equity Value": [115, 138],
            "Valuation Basis": ["Quarterly Mark", "Quarterly Mark"],
            "Source": ["Finance", "Finance"],
        },
        "Fund Quarterly": {
            "Fund": ["Fund IX"],
            "Quarter End": ["2022-12-31"],
            "Committed Capital": [805],
            "Paid In Capital": [420],
            "Distributed Capital": [110],
            "NAV": [520],
            "Unfunded Commitment": [385],
        },
        "Underwrite": {
            "Company Name": ["Multi Co"],
            "Fund": ["Fund IX"],
            "Baseline Date": ["2021-01-01"],
            "Target IRR": [0.20],
            "Target MOIC": [2.5],
            "Target Hold Years": [5.0],
            "Target Exit Multiple": [12.0],
            "Target Revenue CAGR": [0.08],
            "Target EBITDA CAGR": [0.10],
        },
    }

    file_path = create_temp_workbook(sheets)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        assert result["supplemental_counts"]["cashflows"] == 2
        assert result["supplemental_counts"]["deal_quarterly"] == 2
        assert result["supplemental_counts"]["fund_quarterly"] == 1
        assert result["supplemental_counts"]["underwrite"] == 1

        deal = Deal.query.filter_by(company_name="Multi Co", firm_id=result["firm_id"], team_id=team.id).first()
        assert deal is not None
        assert DealCashflowEvent.query.filter_by(deal_id=deal.id, firm_id=result["firm_id"], team_id=team.id).count() == 2
        assert DealQuarterSnapshot.query.filter_by(deal_id=deal.id, firm_id=result["firm_id"], team_id=team.id).count() == 2
        assert DealUnderwriteBaseline.query.filter_by(deal_id=deal.id, firm_id=result["firm_id"], team_id=team.id).count() == 1
        assert FundQuarterSnapshot.query.filter_by(fund_number="Fund IX", firm_id=result["firm_id"], team_id=team.id).count() == 1
    finally:
        os.remove(file_path)


def test_parse_deals_multisheet_missing_optional_tabs_is_backward_compatible(app_context):
    team = create_team()
    firm_name = "Firm Deals Only"
    sheets = {
        "Deals": {
            "Firm Name": [firm_name],
            "As Of Date": [date(2025, 12, 31)],
            "Company Name": ["Only Deals Co"],
            "Fund": ["Fund II"],
            "Equity Invested": [75],
            "Realized Value": [0],
            "Unrealized Value": [95],
        }
    }
    file_path = create_temp_workbook(sheets)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        assert result["supplemental_counts"] == {
            "cashflows": 0,
            "deal_quarterly": 0,
            "fund_quarterly": 0,
            "underwrite": 0,
        }
    finally:
        os.remove(file_path)


def test_parse_deals_multifirm_optional_sheets_require_firm_name(app_context):
    team = create_team()
    sheets = {
        "Deals": {
            "Firm Name": ["Firm A", "Firm B"],
            "As Of Date": [date(2025, 12, 31), date(2026, 1, 31)],
            "Company Name": ["Co A", "Co B"],
            "Fund": ["Fund I", "Fund II"],
            "Equity Invested": [100, 120],
            "Realized Value": [120, 150],
            "Unrealized Value": [0, 0],
        },
        "Cashflows": {
            "Company Name": ["Co A", "Co B"],
            "Fund": ["Fund I", "Fund II"],
            "Event Date": ["2022-01-01", "2022-02-01"],
            "Event Type": ["Capital Call", "Capital Call"],
            "Amount": [-10, -12],
        },
    }
    file_path = create_temp_workbook(sheets)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 2
        assert result["supplemental_counts"]["cashflows"] == 0
        assert any("Cashflows sheet requires a Firm Name column" in msg for msg in result["errors"])
    finally:
        os.remove(file_path)


def test_parse_deals_replaces_existing_fund_by_default(app_context):
    team = create_team()
    firm = create_firm("Replace Firm")
    old = Deal(company_name="Old Co", fund_number="Fund Replace", equity_invested=10, firm_id=firm.id, team_id=team.id)
    db.session.add(old)
    db.session.commit()

    data = _with_firm_name(
        {
            "Company Name": ["New Co"],
            "Fund": ["Fund Replace"],
            "Equity Invested": [100],
            "Realized Value": [120],
            "Unrealized Value": [0],
        },
        firm.name,
    )
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        assert result["replaced_funds"]["Fund Replace"] == 1
        assert Deal.query.filter_by(firm_id=firm.id, fund_number="Fund Replace", company_name="Old Co").count() == 0
        assert Deal.query.filter_by(firm_id=firm.id, fund_number="Fund Replace", company_name="New Co", team_id=team.id).count() == 1
    finally:
        os.remove(file_path)


def test_parse_deals_replace_is_scoped_to_firm(app_context):
    team = create_team()
    firm_a = create_firm("Firm A")
    firm_b = create_firm("Firm B")
    firm_a_id = firm_a.id
    firm_b_id = firm_b.id

    db.session.add_all(
        [
            Deal(company_name="Old Firm A", fund_number="Fund Shared", equity_invested=10, firm_id=firm_a_id, team_id=team.id),
            Deal(company_name="Old Firm B", fund_number="Fund Shared", equity_invested=10, firm_id=firm_b_id, team_id=team.id),
        ]
    )
    db.session.commit()

    data = _with_firm_name(
        {
            "Company Name": ["New Firm A"],
            "Fund": ["Fund Shared"],
            "Equity Invested": [100],
            "Realized Value": [130],
            "Unrealized Value": [0],
        },
        firm_a.name,
    )
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 1
        assert result["replaced_funds"]["Fund Shared"] == 1

        assert Deal.query.filter_by(firm_id=firm_a_id, fund_number="Fund Shared", company_name="Old Firm A").count() == 0
        assert Deal.query.filter_by(firm_id=firm_a_id, fund_number="Fund Shared", company_name="New Firm A", team_id=team.id).count() == 1

        assert Deal.query.filter_by(firm_id=firm_b_id, fund_number="Fund Shared", company_name="Old Firm B").count() == 1
        assert Deal.query.filter_by(firm_id=firm_b_id, fund_number="Fund Shared", company_name="New Firm A").count() == 0
    finally:
        os.remove(file_path)


def test_parse_deals_replace_mode_applies_per_firm_in_multi_firm_workbook(app_context):
    team = create_team()
    firm_a = create_firm("Multi Replace A")
    firm_b = create_firm("Multi Replace B")
    db.session.add_all(
        [
            Deal(company_name="Old A", fund_number="Fund Shared", equity_invested=10, firm_id=firm_a.id, team_id=team.id),
            Deal(company_name="Old B", fund_number="Fund Shared", equity_invested=10, firm_id=firm_b.id, team_id=team.id),
        ]
    )
    db.session.commit()

    data = {
        "Firm Name": [firm_a.name, firm_b.name],
        "As Of Date": [date(2025, 12, 31), date(2025, 12, 31)],
        "Company Name": ["New A", "New B"],
        "Fund": ["Fund Shared", "Fund Shared"],
        "Equity Invested": [100, 100],
        "Realized Value": [130, 140],
        "Unrealized Value": [0, 0],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team.id)
        assert result["success"] == 2
        assert result["firm_count"] == 2
        by_firm = {row["firm_name"]: row["replaced_funds"] for row in result["firms_processed"]}
        assert by_firm[firm_a.name]["Fund Shared"] == 1
        assert by_firm[firm_b.name]["Fund Shared"] == 1
        assert Deal.query.filter_by(firm_id=firm_a.id, company_name="Old A", fund_number="Fund Shared").count() == 0
        assert Deal.query.filter_by(firm_id=firm_b.id, company_name="Old B", fund_number="Fund Shared").count() == 0
        assert Deal.query.filter_by(firm_id=firm_a.id, company_name="New A", fund_number="Fund Shared").count() == 1
        assert Deal.query.filter_by(firm_id=firm_b.id, company_name="New B", fund_number="Fund Shared").count() == 1
    finally:
        os.remove(file_path)

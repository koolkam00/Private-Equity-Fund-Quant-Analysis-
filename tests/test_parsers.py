import os
import tempfile
import uuid

import pandas as pd

from models import (
    Deal,
    DealCashflowEvent,
    DealQuarterSnapshot,
    DealUnderwriteBaseline,
    FundQuarterSnapshot,
    Team,
    UploadIssue,
    db,
)
from services.deal_parser import parse_deals


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


def create_team():
    team = Team(name="Parser Team", slug=f"parser-team-{uuid.uuid4().hex[:8]}")
    db.session.add(team)
    db.session.commit()
    return team.id


def test_parse_deals_valid(app_context):
    team_id = create_team()
    data = {
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
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team_id)
        assert result["success"] == 2
        deal = Deal.query.filter_by(company_name="Company A", team_id=team_id).first()
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
    finally:
        os.remove(file_path)


def test_parse_deals_fallback_geography_and_vintage(app_context):
    team_id = create_team()
    data = {
        "Company Name": ["Fallback Co"],
        "Investment Date": ["2022-06-01"],
        "Equity Invested": [100],
        "Realized Value": [0],
        "Unrealized Value": [120],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team_id)
        assert result["success"] == 1
        deal = Deal.query.filter_by(company_name="Fallback Co", team_id=team_id).first()
        assert deal.geography == "Unknown"
        assert deal.year_invested == 2022
    finally:
        os.remove(file_path)


def test_parse_deals_quarantine_invalid_row(app_context):
    team_id = create_team()
    data = {
        "Company Name": ["Bad Deal"],
        "Investment Date": ["2024-01-01"],
        "Exit Date": ["2023-01-01"],
        "Equity Invested": [100],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team_id, replace_mode="append")
        assert result["success"] == 0
        assert result["quarantined_count"] == 1
        assert UploadIssue.query.filter_by(team_id=team_id).count() >= 1
    finally:
        os.remove(file_path)


def test_parse_deals_duplicate_detection(app_context):
    team_id = create_team()

    db.session.add(Deal(company_name="DupCo", fund_number="Fund I", team_id=team_id))
    db.session.commit()

    data = {
        "Company Name": ["DupCo"],
        "Fund": ["Fund I"],
        "Equity Invested": [100],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team_id, replace_mode="append")
        assert result["duplicates_skipped"] == 1
        assert result["success"] == 0
    finally:
        os.remove(file_path)


def test_parse_deals_optional_track_record_numeric_coercion(app_context):
    team_id = create_team()
    data = {
        "Company Name": ["Numeric Co"],
        "Fund": ["Fund X"],
        "Equity Invested": [100],
        "Fund Size": ["540.0"],
        "Net IRR": ["0.183"],
        "Net MOIC": ["2.1"],
        "DPI": ["1.2"],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team_id)
        assert result["success"] == 1
        deal = Deal.query.filter_by(company_name="Numeric Co", team_id=team_id).first()
        assert abs(deal.fund_size - 540.0) < 1e-9
        assert abs(deal.net_irr - 0.183) < 1e-9
        assert abs(deal.net_moic - 2.1) < 1e-9
        assert abs(deal.net_dpi - 1.2) < 1e-9
    finally:
        os.remove(file_path)


def test_parse_deals_multisheet_optional_sections(app_context):
    team_id = create_team()
    sheets = {
        "Deals": {
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
        result = parse_deals(file_path, team_id=team_id)
        assert result["success"] == 1
        assert result["supplemental_counts"]["cashflows"] == 2
        assert result["supplemental_counts"]["deal_quarterly"] == 2
        assert result["supplemental_counts"]["fund_quarterly"] == 1
        assert result["supplemental_counts"]["underwrite"] == 1

        deal = Deal.query.filter_by(company_name="Multi Co", team_id=team_id).first()
        assert deal is not None
        assert DealCashflowEvent.query.filter_by(deal_id=deal.id, team_id=team_id).count() == 2
        assert DealQuarterSnapshot.query.filter_by(deal_id=deal.id, team_id=team_id).count() == 2
        assert DealUnderwriteBaseline.query.filter_by(deal_id=deal.id, team_id=team_id).count() == 1
        assert FundQuarterSnapshot.query.filter_by(fund_number="Fund IX", team_id=team_id).count() == 1
    finally:
        os.remove(file_path)


def test_parse_deals_multisheet_missing_optional_tabs_is_backward_compatible(app_context):
    team_id = create_team()
    sheets = {
        "Deals": {
            "Company Name": ["Only Deals Co"],
            "Fund": ["Fund II"],
            "Equity Invested": [75],
            "Realized Value": [0],
            "Unrealized Value": [95],
        }
    }
    file_path = create_temp_workbook(sheets)
    try:
        result = parse_deals(file_path, team_id=team_id)
        assert result["success"] == 1
        assert result["supplemental_counts"] == {
            "cashflows": 0,
            "deal_quarterly": 0,
            "fund_quarterly": 0,
            "underwrite": 0,
        }
    finally:
        os.remove(file_path)


def test_parse_deals_replaces_existing_fund_by_default(app_context):
    team_id = create_team()
    old = Deal(company_name="Old Co", fund_number="Fund Replace", equity_invested=10, team_id=team_id)
    db.session.add(old)
    db.session.commit()

    data = {
        "Company Name": ["New Co"],
        "Fund": ["Fund Replace"],
        "Equity Invested": [100],
        "Realized Value": [120],
        "Unrealized Value": [0],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team_id)
        assert result["success"] == 1
        assert result["replaced_funds"]["Fund Replace"] == 1
        assert Deal.query.filter_by(team_id=team_id, fund_number="Fund Replace", company_name="Old Co").count() == 0
        assert Deal.query.filter_by(team_id=team_id, fund_number="Fund Replace", company_name="New Co").count() == 1
    finally:
        os.remove(file_path)


def test_parse_deals_replace_is_scoped_to_team(app_context):
    team_a = create_team()
    team_b = create_team()

    db.session.add_all(
        [
            Deal(company_name="Old Team A", fund_number="Fund Shared", equity_invested=10, team_id=team_a),
            Deal(company_name="Old Team B", fund_number="Fund Shared", equity_invested=10, team_id=team_b),
        ]
    )
    db.session.commit()

    data = {
        "Company Name": ["New Team A"],
        "Fund": ["Fund Shared"],
        "Equity Invested": [100],
        "Realized Value": [130],
        "Unrealized Value": [0],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path, team_id=team_a)
        assert result["success"] == 1
        assert result["replaced_funds"]["Fund Shared"] == 1

        assert Deal.query.filter_by(team_id=team_a, fund_number="Fund Shared", company_name="Old Team A").count() == 0
        assert Deal.query.filter_by(team_id=team_a, fund_number="Fund Shared", company_name="New Team A").count() == 1

        assert Deal.query.filter_by(team_id=team_b, fund_number="Fund Shared", company_name="Old Team B").count() == 1
        assert Deal.query.filter_by(team_id=team_b, fund_number="Fund Shared", company_name="New Team A").count() == 0
    finally:
        os.remove(file_path)

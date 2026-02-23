import os
import tempfile

import pandas as pd

from models import Deal, UploadIssue
from services.deal_parser import parse_deals


def create_temp_excel(data, sheet_name="Sheet1"):
    df = pd.DataFrame(data)
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return path


def test_parse_deals_valid(app_context):
    data = {
        "Company Name": ["Company A", "Company B"],
        "Fund": ["Fund I", "Fund II"],
        "Sector": ["Tech", "Health"],
        "Geography": ["US", "UK"],
        "Year Invested": [2020, 2021],
        "Entry EV": [100, 200],
        "Entry EBITDA": [10, 20],
        "Exit EV": [150, 260],
        "Exit EBITDA": [15, 30],
        "Equity Invested": [50, 100],
        "Realized Value": [80, 120],
        "Unrealized Value": [0, 20],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path)
        assert result["success"] == 2
        deal = Deal.query.filter_by(company_name="Company A").first()
        assert deal.geography == "US"
        assert deal.year_invested == 2020
    finally:
        os.remove(file_path)


def test_parse_deals_fallback_geography_and_vintage(app_context):
    data = {
        "Company Name": ["Fallback Co"],
        "Investment Date": ["2022-06-01"],
        "Equity Invested": [100],
        "Realized Value": [0],
        "Unrealized Value": [120],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path)
        assert result["success"] == 1
        deal = Deal.query.filter_by(company_name="Fallback Co").first()
        assert deal.geography == "Unknown"
        assert deal.year_invested == 2022
    finally:
        os.remove(file_path)


def test_parse_deals_quarantine_invalid_row(app_context):
    data = {
        "Company Name": ["Bad Deal"],
        "Investment Date": ["2024-01-01"],
        "Exit Date": ["2023-01-01"],
        "Equity Invested": [100],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path)
        assert result["success"] == 0
        assert result["quarantined_count"] == 1
        assert UploadIssue.query.count() >= 1
    finally:
        os.remove(file_path)


def test_parse_deals_duplicate_detection(app_context):
    from models import db

    db.session.add(Deal(company_name="DupCo", fund_number="Fund I"))
    db.session.commit()

    data = {
        "Company Name": ["DupCo"],
        "Fund": ["Fund I"],
        "Equity Invested": [100],
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path)
        assert result["duplicates_skipped"] == 1
        assert result["success"] == 0
    finally:
        os.remove(file_path)

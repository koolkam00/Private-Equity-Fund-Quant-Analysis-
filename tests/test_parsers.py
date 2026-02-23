import os
import tempfile
import pandas as pd
import pytest
from services.deal_parser import parse_deals
from services.cashflow_parser import parse_cashflows
from models import Deal, Cashflow

def create_temp_excel(data, sheet_name="Sheet1"):
    """Helper to create a temporary Excel file."""
    df = pd.DataFrame(data)
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return path

def test_parse_deals_valid(app_context):
    """Test parsing a valid deals file."""
    data = {
        "Company Name": ["Company A", "Company B"],
        "Fund": ["Fund I", "Fund II"],
        "Sector": ["Tech", "Health"],
        "Status": ["Realized", "Unrealized"],
        "Investment Date": ["2020-01-01", "2021-01-01"],
        "Equity Invested": [100, 200],
        "MOIC": [2.0, 1.5],
        "IRR": [0.20, 0.15]
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path)
        assert result["success"] == 2
        assert len(result["errors"]) == 0
        assert Deal.query.count() == 2
        
        deal_a = Deal.query.filter_by(company_name="Company A").first()
        assert deal_a.fund_number == "Fund I"
        assert deal_a.equity_invested == 100.0
    finally:
        os.remove(file_path)

def test_parse_deals_missing_columns(app_context):
    """Test parsing deals with missing required columns."""
    data = {
        "Fund": ["Fund I"],  # Missing "Company Name"
        "Sector": ["Tech"]
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path)
        assert result["success"] == 0
        assert len(result["errors"]) > 0
        assert "Could not find a 'Company Name'" in result["errors"][0]
    finally:
        os.remove(file_path)

def test_parse_deals_invalid_data(app_context):
    """Test parsing deals with some invalid rows."""
    data = {
        "Company Name": ["Good Company", None, "   "],
        "Fund": ["Fund I", "Fund II", "Fund III"],
        "Equity Invested": [100, "Not a number", 300]
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_deals(file_path)
        # Should parse "Good Company", skip None/Empty company rows.
        # "Not a number" in equity should be coerced to NaN/None but row might still be saved or handled.
        # The parser logic says: `_clean_val` returns None for errors.
        
        # Row 1: Valid
        # Row 2: None company -> Skip
        # Row 3: Empty string company -> Skip
        
        assert result["success"] == 1
        assert len(result["errors"]) >= 2 # Errors for skipped rows
        
        deal = Deal.query.filter_by(company_name="Good Company").first()
        assert deal is not None
        assert deal.equity_invested == 100.0
    finally:
        os.remove(file_path)

def test_parse_cashflows_valid(app_context):
    """Test parsing minimal valid cashflows."""
    # First create a deal to link to
    deal = Deal(company_name="Test Company", fund_number="Fund I")
    from models import db
    db.session.add(deal)
    db.session.commit()
    
    data = {
        "Company Name": ["Test Company", "Unknown Company"],
        "Date": ["2022-01-01", "2022-02-01"],
        "Capital Called": [50, 60],
        "Distributions": [0, 10]
    }
    file_path = create_temp_excel(data)
    try:
        result = parse_cashflows(file_path)
        
        # "Test Company" should match and accept
        # "Unknown Company" should be added but with deal_id=None (logic: parse_cashflows calls deal_lookup.get(...), if not found returns None)
        # schema: deal_id is nullable.
        
        assert result["success"] == 2
        assert len(result["errors"]) == 0
        
        cf1 = Cashflow.query.filter_by(company_name="Test Company").first()
        assert cf1.deal_id == deal.id
        assert cf1.capital_called == 50.0
        
        cf2 = Cashflow.query.filter_by(company_name="Unknown Company").first()
        assert cf2.deal_id is None
    finally:
        os.remove(file_path)

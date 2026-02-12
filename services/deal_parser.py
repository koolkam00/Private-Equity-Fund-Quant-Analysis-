import uuid

import pandas as pd

from models import db, Deal

# Mapping from common Excel column headers (lowercased, stripped) to Deal model fields.
COLUMN_MAP = {
    # Company name
    "company": "company_name",
    "company name": "company_name",
    "company/deal name": "company_name",
    "deal name": "company_name",
    "deal": "company_name",
    "portfolio company": "company_name",
    # Fund
    "fund": "fund_number",
    "fund #": "fund_number",
    "fund number": "fund_number",
    "fund name": "fund_number",
    # Sector
    "sector": "sector",
    "industry": "sector",
    "sector/industry": "sector",
    # Status
    "status": "status",
    "deal status": "status",
    "realization status": "status",
    # Dates
    "investment date": "investment_date",
    "entry date": "investment_date",
    "exit date": "exit_date",
    # Equity
    "equity invested": "equity_invested",
    "equity": "equity_invested",
    "invested": "equity_invested",
    "investment amount": "equity_invested",
    # Entry operating metrics
    "entry revenue": "entry_revenue",
    "revenue at entry": "entry_revenue",
    "entry ebitda": "entry_ebitda",
    "ebitda at entry": "entry_ebitda",
    "entry ev": "entry_enterprise_value",
    "entry enterprise value": "entry_enterprise_value",
    "enterprise value at entry": "entry_enterprise_value",
    "entry net debt": "entry_net_debt",
    "net debt at entry": "entry_net_debt",
    # Exit operating metrics
    "exit revenue": "exit_revenue",
    "revenue at exit": "exit_revenue",
    "exit ebitda": "exit_ebitda",
    "ebitda at exit": "exit_ebitda",
    "exit ev": "exit_enterprise_value",
    "exit enterprise value": "exit_enterprise_value",
    "enterprise value at exit": "exit_enterprise_value",
    "exit net debt": "exit_net_debt",
    "net debt at exit": "exit_net_debt",
    # Performance
    "moic": "moic",
    "multiple": "moic",
    "irr": "irr",
    "gross irr": "irr",
}

VALID_FIELDS = {
    "company_name", "fund_number", "sector", "status",
    "investment_date", "exit_date", "equity_invested",
    "entry_revenue", "entry_ebitda", "entry_enterprise_value", "entry_net_debt",
    "exit_revenue", "exit_ebitda", "exit_enterprise_value", "exit_net_debt",
    "moic", "irr",
}

DATE_COLS = {"investment_date", "exit_date"}

FLOAT_COLS = {
    "equity_invested",
    "entry_revenue", "entry_ebitda", "entry_enterprise_value", "entry_net_debt",
    "exit_revenue", "exit_ebitda", "exit_enterprise_value", "exit_net_debt",
    "moic", "irr",
}

STR_COLS = {"company_name", "fund_number", "sector", "status"}


def parse_deals(file_path):
    """
    Parse an Excel file of deal data and insert rows into the database.

    Returns: {"success": int, "errors": list[str], "batch_id": str}
    """
    batch_id = str(uuid.uuid4())[:8]

    df = pd.read_excel(file_path, engine="openpyxl")

    # Normalize headers
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Map columns
    rename_map = {col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP}
    df = df.rename(columns=rename_map)

    # Check for required column
    if "company_name" not in df.columns:
        return {
            "success": 0,
            "errors": ["Could not find a 'Company Name' (or similar) column in the spreadsheet."],
            "batch_id": batch_id,
        }

    # Keep only valid columns
    df = df[[c for c in df.columns if c in VALID_FIELDS]]

    # Coerce types
    for col in DATE_COLS & set(df.columns):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    for col in FLOAT_COLS & set(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in STR_COLS & set(df.columns):
        df[col] = df[col].astype(str).replace("nan", None)

    success = 0
    errors = []

    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row (1-indexed header + 0-indexed data)

        if row.isna().all():
            continue

        company = row.get("company_name")
        if pd.isna(company) or company is None or str(company).strip() in ("", "None"):
            errors.append(f"Row {row_num}: Skipped — missing company name.")
            continue

        try:
            deal = Deal(
                company_name=str(company).strip(),
                fund_number=_clean_str(row.get("fund_number")),
                sector=_clean_str(row.get("sector")),
                status=_clean_str(row.get("status")) or "Unrealized",
                investment_date=_clean_val(row.get("investment_date")),
                exit_date=_clean_val(row.get("exit_date")),
                equity_invested=_clean_val(row.get("equity_invested")),
                entry_revenue=_clean_val(row.get("entry_revenue")),
                entry_ebitda=_clean_val(row.get("entry_ebitda")),
                entry_enterprise_value=_clean_val(row.get("entry_enterprise_value")),
                entry_net_debt=_clean_val(row.get("entry_net_debt")),
                exit_revenue=_clean_val(row.get("exit_revenue")),
                exit_ebitda=_clean_val(row.get("exit_ebitda")),
                exit_enterprise_value=_clean_val(row.get("exit_enterprise_value")),
                exit_net_debt=_clean_val(row.get("exit_net_debt")),
                moic=_clean_val(row.get("moic")),
                irr=_clean_val(row.get("irr")),
                upload_batch=batch_id,
            )
            db.session.add(deal)
            success += 1
        except Exception as e:
            errors.append(f"Row {row_num}: {str(e)}")

    db.session.commit()
    return {"success": success, "errors": errors, "batch_id": batch_id}


def _clean_val(val):
    """Return None for NaN/NaT, otherwise the value as-is."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def _clean_str(val):
    """Return None for NaN/'None'/empty, otherwise stripped string."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("", "nan", "none", "nat", "n/a", "na", "-", "--", "#n/a", "#ref!"):
        return None
    return s

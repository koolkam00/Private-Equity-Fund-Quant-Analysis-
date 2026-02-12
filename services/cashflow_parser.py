import uuid

import pandas as pd

from models import db, Deal, Cashflow

# Mapping from common Excel column headers (lowercased, stripped) to Cashflow model fields.
COLUMN_MAP = {
    # Company name
    "company": "company_name",
    "company name": "company_name",
    "deal": "company_name",
    "deal name": "company_name",
    "portfolio company": "company_name",
    # Fund
    "fund": "fund_number",
    "fund #": "fund_number",
    "fund number": "fund_number",
    "fund name": "fund_number",
    # Date
    "date": "date",
    "cashflow date": "date",
    "period": "date",
    "period date": "date",
    # Capital called
    "capital called": "capital_called",
    "capital call": "capital_called",
    "capital calls": "capital_called",
    "contributions": "capital_called",
    "contribution": "capital_called",
    # Distributions
    "distributions": "distributions",
    "distribution": "distributions",
    "proceeds": "distributions",
    # Fees
    "fees": "fees",
    "fee": "fees",
    "management fee": "fees",
    "management fees": "fees",
    # NAV
    "nav": "nav",
    "net asset value": "nav",
}

VALID_FIELDS = {
    "company_name", "fund_number", "date",
    "capital_called", "distributions", "fees", "nav",
}

DATE_COLS = {"date"}
FLOAT_COLS = {"capital_called", "distributions", "fees", "nav"}
STR_COLS = {"company_name", "fund_number"}


def parse_cashflows(file_path):
    """
    Parse an Excel file of cashflow data and insert rows into the database.
    Attempts to link each cashflow to an existing Deal by company name.

    Returns: {"success": int, "errors": list[str], "batch_id": str}
    """
    batch_id = str(uuid.uuid4())[:8]

    df = pd.read_excel(file_path, engine="openpyxl")

    # Normalize headers
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Map columns
    rename_map = {col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP}
    df = df.rename(columns=rename_map)

    # Check required columns
    missing = []
    if "company_name" not in df.columns:
        missing.append("Company Name")
    if "date" not in df.columns:
        missing.append("Date")
    if missing:
        return {
            "success": 0,
            "errors": [f"Could not find required column(s): {', '.join(missing)}."],
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

    # Build a lookup of existing deals by company name (case-insensitive)
    deals = Deal.query.all()
    deal_lookup = {}
    for deal in deals:
        deal_lookup[deal.company_name.strip().lower()] = deal.id

    success = 0
    errors = []

    for idx, row in df.iterrows():
        row_num = idx + 2

        if row.isna().all():
            continue

        company = row.get("company_name")
        if pd.isna(company) or company is None or str(company).strip() in ("", "None"):
            errors.append(f"Row {row_num}: Skipped — missing company name.")
            continue

        date_val = _clean_val(row.get("date"))
        if date_val is None:
            errors.append(f"Row {row_num}: Skipped — missing date.")
            continue

        try:
            company_clean = str(company).strip()
            deal_id = deal_lookup.get(company_clean.lower())

            cashflow = Cashflow(
                deal_id=deal_id,
                company_name=company_clean,
                fund_number=_clean_str(row.get("fund_number")),
                date=date_val,
                capital_called=_clean_val(row.get("capital_called")),
                distributions=_clean_val(row.get("distributions")),
                fees=_clean_val(row.get("fees")),
                nav=_clean_val(row.get("nav")),
                upload_batch=batch_id,
            )
            db.session.add(cashflow)
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
    if s in ("", "nan", "None", "NaT"):
        return None
    return s

import json
import math
import uuid

import pandas as pd

from models import Deal, UploadIssue, db
from services.utils import clean_str, clean_val


COLUMN_MAP = {
    # Identification
    "company": "company_name",
    "company name": "company_name",
    "company/deal name": "company_name",
    "deal name": "company_name",
    "deal": "company_name",
    "portfolio company": "company_name",
    "fund": "fund_number",
    "fund #": "fund_number",
    "fund number": "fund_number",
    "fund name": "fund_number",
    "sector": "sector",
    "industry": "sector",
    "sector/industry": "sector",
    "geography": "geography",
    "region": "geography",
    "country": "geography",
    "status": "status",
    "deal status": "status",
    "realization status": "status",
    # Dates
    "investment date": "investment_date",
    "entry date": "investment_date",
    "year invested": "year_invested",
    "vintage": "year_invested",
    "vintage year": "year_invested",
    "exit date": "exit_date",
    # Investment values
    "equity invested": "equity_invested",
    "equity": "equity_invested",
    "invested": "equity_invested",
    "investment amount": "equity_invested",
    "realized value": "realized_value",
    "realized": "realized_value",
    "unrealized value": "unrealized_value",
    "unrealized": "unrealized_value",
    # Ownership
    "ownership %": "ownership_pct",
    "ownership pct": "ownership_pct",
    "ownership": "ownership_pct",
    # Entry metrics
    "entry revenue": "entry_revenue",
    "revenue at entry": "entry_revenue",
    "entry ebitda": "entry_ebitda",
    "ebitda at entry": "entry_ebitda",
    "entry ev": "entry_enterprise_value",
    "entry tev": "entry_enterprise_value",
    "entry enterprise value": "entry_enterprise_value",
    "entry total enterprise value": "entry_enterprise_value",
    "enterprise value at entry": "entry_enterprise_value",
    "entry net debt": "entry_net_debt",
    "net debt at entry": "entry_net_debt",
    # Exit metrics
    "exit revenue": "exit_revenue",
    "revenue at exit": "exit_revenue",
    "exit ebitda": "exit_ebitda",
    "ebitda at exit": "exit_ebitda",
    "exit ev": "exit_enterprise_value",
    "exit tev": "exit_enterprise_value",
    "exit enterprise value": "exit_enterprise_value",
    "exit total enterprise value": "exit_enterprise_value",
    "enterprise value at exit": "exit_enterprise_value",
    "exit net debt": "exit_net_debt",
    "net debt at exit": "exit_net_debt",
    # Optional legacy
    "irr": "irr",
    "gross irr": "irr",
}

VALID_FIELDS = {
    "company_name",
    "fund_number",
    "sector",
    "geography",
    "status",
    "investment_date",
    "year_invested",
    "exit_date",
    "equity_invested",
    "realized_value",
    "unrealized_value",
    "ownership_pct",
    "entry_revenue",
    "entry_ebitda",
    "entry_enterprise_value",
    "entry_net_debt",
    "exit_revenue",
    "exit_ebitda",
    "exit_enterprise_value",
    "exit_net_debt",
    "irr",
}

DATE_COLS = {"investment_date", "exit_date"}
FLOAT_COLS = {
    "equity_invested",
    "realized_value",
    "unrealized_value",
    "ownership_pct",
    "entry_revenue",
    "entry_ebitda",
    "entry_enterprise_value",
    "entry_net_debt",
    "exit_revenue",
    "exit_ebitda",
    "exit_enterprise_value",
    "exit_net_debt",
    "irr",
}
INT_COLS = {"year_invested"}
STR_COLS = {"company_name", "fund_number", "sector", "geography", "status"}


def _normalize_ownership(val):
    if val is None:
        return None
    try:
        num = float(val)
    except (TypeError, ValueError):
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    if num > 1.0:
        num /= 100.0
    return num


def _record_issue(issue_report_id, batch_id, row_num, company, severity, message, payload):
    db.session.add(
        UploadIssue(
            issue_report_id=issue_report_id,
            upload_batch=batch_id,
            file_type="deals",
            row_number=row_num,
            company_name=company,
            severity=severity,
            message=message,
            payload=json.dumps(payload, default=str),
        )
    )


def _warn_extreme_multiples(deal):
    warnings = []
    if deal.entry_ebitda and deal.entry_ebitda != 0 and deal.entry_enterprise_value is not None:
        entry_mult = deal.entry_enterprise_value / deal.entry_ebitda
        if abs(entry_mult) > 50:
            warnings.append(f"Entry TEV/EBITDA {entry_mult:.1f}x appears extreme")
    if deal.exit_ebitda and deal.exit_ebitda != 0 and deal.exit_enterprise_value is not None:
        exit_mult = deal.exit_enterprise_value / deal.exit_ebitda
        if abs(exit_mult) > 50:
            warnings.append(f"Exit TEV/EBITDA {exit_mult:.1f}x appears extreme")
    return warnings


def parse_deals(file_path):
    """Parse deal-level template and insert rows.

    Returns: {success, errors, batch_id, bridge_complete, duplicates_skipped,
              quarantined_count, issue_report_id}
    """
    batch_id = str(uuid.uuid4())[:8]
    issue_report_id = str(uuid.uuid4())

    df = pd.read_excel(file_path, engine="openpyxl")
    df.columns = [str(c).strip().lower() for c in df.columns]

    rename_map = {col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP}
    df = df.rename(columns=rename_map)

    if "company_name" not in df.columns:
        return {
            "success": 0,
            "errors": ["Could not find a 'Company Name' (or similar) column in the spreadsheet."],
            "batch_id": batch_id,
            "bridge_complete": 0,
            "duplicates_skipped": 0,
            "quarantined_count": 0,
            "issue_report_id": issue_report_id,
        }

    df = df[[c for c in df.columns if c in VALID_FIELDS]]

    for col in DATE_COLS & set(df.columns):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    for col in FLOAT_COLS & set(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in INT_COLS & set(df.columns):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in STR_COLS & set(df.columns):
        df[col] = df[col].astype(str).replace("nan", None)

    existing_keys = {
        (d.company_name.strip().lower(), (d.fund_number or "").strip().lower())
        for d in Deal.query.all()
    }

    success = 0
    errors = []
    bridge_complete = 0
    duplicates_skipped = 0
    quarantined_count = 0

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        row_payload = {k: clean_val(v) for k, v in row.to_dict().items()}
        company = row.get("company_name")

        if pd.isna(company) or company is None or str(company).strip() in ("", "None"):
            msg = f"Row {row_num}: Quarantined — missing company name."
            errors.append(msg)
            quarantined_count += 1
            _record_issue(issue_report_id, batch_id, row_num, None, "error", msg, row_payload)
            continue

        fund_val = clean_str(row.get("fund_number")) or ""
        deal_key = (str(company).strip().lower(), fund_val.strip().lower())
        if deal_key in existing_keys:
            msg = f"Row {row_num}: Skipped duplicate — '{company}' already exists."
            errors.append(msg)
            duplicates_skipped += 1
            _record_issue(issue_report_id, batch_id, row_num, str(company).strip(), "warning", msg, row_payload)
            continue
        existing_keys.add(deal_key)

        year_invested_val = clean_val(row.get("year_invested"))
        if year_invested_val is not None:
            try:
                year_invested_val = int(year_invested_val)
            except (TypeError, ValueError):
                year_invested_val = None

        investment_date = clean_val(row.get("investment_date"))
        if year_invested_val is None and investment_date is not None:
            year_invested_val = investment_date.year

        ownership = _normalize_ownership(clean_val(row.get("ownership_pct")))

        try:
            deal = Deal(
                company_name=str(company).strip(),
                fund_number=clean_str(row.get("fund_number")),
                sector=clean_str(row.get("sector")),
                geography=clean_str(row.get("geography")) or "Unknown",
                status=clean_str(row.get("status")) or "Unrealized",
                investment_date=investment_date,
                year_invested=year_invested_val,
                exit_date=clean_val(row.get("exit_date")),
                equity_invested=clean_val(row.get("equity_invested")),
                realized_value=clean_val(row.get("realized_value")),
                unrealized_value=clean_val(row.get("unrealized_value")),
                ownership_pct=ownership,
                entry_revenue=clean_val(row.get("entry_revenue")),
                entry_ebitda=clean_val(row.get("entry_ebitda")),
                entry_enterprise_value=clean_val(row.get("entry_enterprise_value")),
                entry_net_debt=clean_val(row.get("entry_net_debt")),
                exit_revenue=clean_val(row.get("exit_revenue")),
                exit_ebitda=clean_val(row.get("exit_ebitda")),
                exit_enterprise_value=clean_val(row.get("exit_enterprise_value")),
                exit_net_debt=clean_val(row.get("exit_net_debt")),
                irr=clean_val(row.get("irr")),
                upload_batch=batch_id,
            )

            # Quarantine conditions
            if deal.equity_invested is not None and deal.equity_invested < 0:
                msg = f"Row {row_num}: Quarantined — negative equity invested ({deal.equity_invested})."
                errors.append(msg)
                quarantined_count += 1
                _record_issue(issue_report_id, batch_id, row_num, deal.company_name, "error", msg, row_payload)
                continue

            if deal.investment_date and deal.exit_date and deal.exit_date < deal.investment_date:
                msg = (
                    f"Row {row_num}: Quarantined — exit date ({deal.exit_date}) is before "
                    f"investment date ({deal.investment_date})."
                )
                errors.append(msg)
                quarantined_count += 1
                _record_issue(issue_report_id, batch_id, row_num, deal.company_name, "error", msg, row_payload)
                continue

            # Warnings only
            if ownership is not None and ownership > 1.5:
                msg = f"Row {row_num}: Ownership {ownership:.2%} is above expected range."
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, row_num, deal.company_name, "warning", msg, row_payload)

            for warn in _warn_extreme_multiples(deal):
                msg = f"Row {row_num}: {warn}"
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, row_num, deal.company_name, "warning", msg, row_payload)

            bridge_fields = [
                deal.entry_revenue,
                deal.entry_ebitda,
                deal.entry_enterprise_value,
                deal.entry_net_debt,
                deal.exit_revenue,
                deal.exit_ebitda,
                deal.exit_enterprise_value,
                deal.exit_net_debt,
            ]
            if not all(v is not None for v in bridge_fields):
                msg = f"Row {row_num}: Missing entry/exit fields for full bridge decomposition."
                errors.append(msg)
                _record_issue(issue_report_id, batch_id, row_num, deal.company_name, "warning", msg, row_payload)
            else:
                bridge_complete += 1

            db.session.add(deal)
            success += 1
        except Exception as exc:
            msg = f"Row {row_num}: Quarantined — {str(exc)}"
            errors.append(msg)
            quarantined_count += 1
            _record_issue(issue_report_id, batch_id, row_num, str(company).strip(), "error", msg, row_payload)

    db.session.commit()
    return {
        "success": success,
        "errors": errors,
        "batch_id": batch_id,
        "bridge_complete": bridge_complete,
        "duplicates_skipped": duplicates_skipped,
        "quarantined_count": quarantined_count,
        "issue_report_id": issue_report_id,
    }

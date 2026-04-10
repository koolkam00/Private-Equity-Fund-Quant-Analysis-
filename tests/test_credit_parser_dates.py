from datetime import date
from io import BytesIO

import pandas as pd
from sqlalchemy import text

from models import CreditFundPerformance, CreditLoan, CreditLoanSnapshot, db, sanitize_credit_date_columns
from services.credit_parser import parse_credit_loan_tape


def _build_credit_workbook(loans, snapshots=None, fund_performance=None):
    workbook = BytesIO()
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame(loans).to_excel(writer, index=False, sheet_name="Credit Loans")
        if snapshots is not None:
            pd.DataFrame(snapshots).to_excel(writer, index=False, sheet_name="Snapshots")
        if fund_performance is not None:
            pd.DataFrame(fund_performance).to_excel(writer, index=False, sheet_name="Fund Performance")
    workbook.seek(0)
    return workbook


def test_credit_parser_handles_excel_serial_dates_and_numeric_strings(app_context):
    workbook = _build_credit_workbook(
        loans={
            "Company Name": ["Alpha Borrower"],
            "Fund Name": ["Fund I"],
            "Entry Date": ["45292"],
            "As Of Date": [45352],
            "Maturity Date": [48113],
        },
        snapshots={
            "Company Name": ["Alpha Borrower"],
            "Snapshot Date": ["45444"],
        },
        fund_performance={
            "Fund Name": ["Fund I"],
            "Report Date": ["45475"],
        },
    )

    result = parse_credit_loan_tape(workbook, firm_name="Serial Date Firm")

    assert result["loans"] == 1
    loan = CreditLoan.query.filter_by(company_name="Alpha Borrower").one()
    snapshot = CreditLoanSnapshot.query.filter_by(credit_loan_id=loan.id).one()
    fund_perf = CreditFundPerformance.query.filter_by(fund_name="Fund I").one()

    assert loan.close_date == date(2024, 1, 1)
    assert loan.as_of_date == date(2024, 3, 1)
    assert loan.maturity_date == date(2031, 9, 22)
    assert snapshot.snapshot_date == date(2024, 6, 1)
    assert fund_perf.report_date == date(2024, 7, 2)


def test_credit_parser_ignores_large_year_date_strings_instead_of_crashing(app_context):
    workbook = _build_credit_workbook(
        loans={
            "Company Name": ["Bad Date Borrower"],
            "Fund Name": ["Fund X"],
            "Entry Date": ["2024-01-01"],
            "Maturity Date": ["48113-11-21"],
        },
        snapshots={
            "Company Name": ["Bad Date Borrower"],
            "Snapshot Date": ["48113-11-21"],
        },
        fund_performance={
            "Fund Name": ["Fund X"],
            "Report Date": ["48113-11-21"],
        },
    )

    result = parse_credit_loan_tape(workbook, firm_name="Malformed Date Firm")

    loan = CreditLoan.query.filter_by(company_name="Bad Date Borrower").one()
    fund_perf = CreditFundPerformance.query.filter_by(fund_name="Fund X").one()

    assert result["loans"] == 1
    assert result["snapshots"] == 0
    assert loan.maturity_date is None
    assert fund_perf.report_date is None
    assert any("Snapshot row missing company or date" in issue["message"] for issue in result["issues"])


def test_sanitize_credit_date_columns_nulls_out_out_of_range_values(app_context):
    db.session.execute(
        text(
            """
            INSERT INTO credit_loans
                (company_name, fund_name, close_date, status, default_status, pik_toggle)
            VALUES
                (:company_name, :fund_name, :close_date, :status, :default_status, :pik_toggle)
            """
        ),
        {
            "company_name": "Corrupted Borrower",
            "fund_name": "Corrupted Fund",
            "close_date": "48113-11-21",
            "status": "Unrealized",
            "default_status": "Performing",
            "pik_toggle": False,
        },
    )
    db.session.commit()

    sanitize_credit_date_columns()

    cleaned_loan = CreditLoan.query.filter_by(company_name="Corrupted Borrower").one()

    assert cleaned_loan.close_date == date(1900, 1, 1)

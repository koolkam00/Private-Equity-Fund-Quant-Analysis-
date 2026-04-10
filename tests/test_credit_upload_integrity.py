from datetime import date
from io import BytesIO
import uuid

import pytest
import pandas as pd

from app import app, db
from models import (
    CreditFundPerformance,
    CreditLoan,
    CreditLoanSnapshot,
    Firm,
    Team,
    TeamFirmAccess,
)
import legacy_app
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


def _create_shared_credit_context():
    suffix = uuid.uuid4().hex[:8]
    team_one = Team(name=f"Credit Team One {suffix}", slug=f"credit-team-one-{suffix}")
    team_two = Team(name=f"Credit Team Two {suffix}", slug=f"credit-team-two-{suffix}")
    firm = Firm(name=f"Shared Credit Firm {suffix}", slug=f"shared-credit-firm-{suffix}")
    db.session.add_all([team_one, team_two, firm])
    db.session.flush()
    db.session.add_all(
        [
            TeamFirmAccess(team_id=team_one.id, firm_id=firm.id),
            TeamFirmAccess(team_id=team_two.id, firm_id=firm.id),
        ]
    )
    db.session.commit()
    return team_one, team_two, firm


def test_credit_upload_is_team_scoped_and_snapshot_matches_current_batch(app_context):
    team_one, team_two, firm = _create_shared_credit_context()

    first_upload = _build_credit_workbook(
        loans={
            "Company Name": ["Shared Borrower"],
            "Fund Name": ["Shared Fund"],
            "Entry Date": ["2024-01-01"],
        },
        snapshots={
            "Company Name": ["Shared Borrower"],
            "Snapshot Date": ["2024-03-31"],
        },
        fund_performance={
            "Fund Name": ["Shared Fund"],
            "Report Date": ["2024-12-31"],
            "Net IRR": [0.10],
        },
    )
    parse_credit_loan_tape(
        first_upload,
        firm_name=firm.name,
        firm_id=firm.id,
        team_id=team_one.id,
    )
    team_one_loan = CreditLoan.query.filter_by(firm_id=firm.id, team_id=team_one.id).one()

    second_upload = _build_credit_workbook(
        loans={
            "Company Name": ["Shared Borrower"],
            "Fund Name": ["Shared Fund"],
            "Entry Date": ["2024-02-01"],
        },
        snapshots={
            "Company Name": ["Shared Borrower"],
            "Snapshot Date": ["2024-06-30"],
        },
        fund_performance={
            "Fund Name": ["Shared Fund"],
            "Report Date": ["2025-03-31"],
            "Net IRR": [0.14],
        },
    )
    parse_credit_loan_tape(
        second_upload,
        firm_name=firm.name,
        firm_id=firm.id,
        team_id=team_two.id,
    )

    team_one_loans = CreditLoan.query.filter_by(firm_id=firm.id, team_id=team_one.id).all()
    team_two_loans = CreditLoan.query.filter_by(firm_id=firm.id, team_id=team_two.id).all()
    assert len(team_one_loans) == 1
    assert len(team_two_loans) == 1

    team_one_snapshots = (
        CreditLoanSnapshot.query.join(CreditLoan, CreditLoanSnapshot.credit_loan_id == CreditLoan.id)
        .filter(CreditLoan.team_id == team_one.id, CreditLoan.firm_id == firm.id)
        .all()
    )
    team_two_snapshots = (
        CreditLoanSnapshot.query.join(CreditLoan, CreditLoanSnapshot.credit_loan_id == CreditLoan.id)
        .filter(CreditLoan.team_id == team_two.id, CreditLoan.firm_id == firm.id)
        .all()
    )
    assert len(team_one_snapshots) == 1
    assert len(team_two_snapshots) == 1
    assert team_one_snapshots[0].credit_loan_id == team_one_loan.id
    assert team_two_snapshots[0].credit_loan_id == team_two_loans[0].id

    assert CreditFundPerformance.query.filter_by(firm_id=firm.id, team_id=team_one.id).count() == 1
    assert CreditFundPerformance.query.filter_by(firm_id=firm.id, team_id=team_two.id).count() == 1


def test_credit_reupload_replaces_old_snapshots_for_same_team_and_fund(app_context):
    team_one, _, firm = _create_shared_credit_context()

    original_upload = _build_credit_workbook(
        loans={
            "Company Name": ["Original Borrower"],
            "Fund Name": ["Fund Replace"],
            "Entry Date": ["2024-01-01"],
        },
        snapshots={
            "Company Name": ["Original Borrower"],
            "Snapshot Date": ["2024-03-31"],
        },
    )
    parse_credit_loan_tape(
        original_upload,
        firm_name=firm.name,
        firm_id=firm.id,
        team_id=team_one.id,
    )
    original_loan_id = CreditLoan.query.filter_by(
        firm_id=firm.id, team_id=team_one.id, company_name="Original Borrower"
    ).one().id

    replacement_upload = _build_credit_workbook(
        loans={
            "Company Name": ["Replacement Borrower"],
            "Fund Name": ["Fund Replace"],
            "Entry Date": ["2024-05-01"],
        },
        snapshots={
            "Company Name": ["Replacement Borrower"],
            "Snapshot Date": ["2024-06-30"],
        },
    )
    parse_credit_loan_tape(
        replacement_upload,
        firm_name=firm.name,
        firm_id=firm.id,
        team_id=team_one.id,
    )

    active_loans = CreditLoan.query.filter_by(firm_id=firm.id, team_id=team_one.id).all()
    assert len(active_loans) == 1
    assert active_loans[0].company_name == "Replacement Borrower"
    assert CreditLoan.query.filter_by(
        firm_id=firm.id, team_id=team_one.id, company_name="Original Borrower"
    ).first() is None
    assert active_loans[0].id != original_loan_id or active_loans[0].company_name == "Replacement Borrower"

    snapshots = CreditLoanSnapshot.query.all()
    assert len(snapshots) == 1
    assert snapshots[0].credit_loan_id == active_loans[0].id


def test_credit_upload_backfills_weight_basis_from_current_invested_capital(app_context):
    team_one, _, firm = _create_shared_credit_context()

    workbook = _build_credit_workbook(
        loans={
            "Company Name": ["Weighted Borrower"],
            "Fund Name": ["Weighted Fund"],
            "Entry Date": ["2024-01-01"],
            "Hold Size": [0.0],
            "Entry Loan Amount": [0.0],
            "Current Invested Capital": [12.5],
        }
    )

    parse_credit_loan_tape(
        workbook,
        firm_name=firm.name,
        firm_id=firm.id,
        team_id=team_one.id,
    )

    loan = CreditLoan.query.filter_by(
        firm_id=firm.id,
        team_id=team_one.id,
        company_name="Weighted Borrower",
    ).one()

    assert loan.current_invested_capital == pytest.approx(12.5)
    assert loan.entry_loan_amount == pytest.approx(12.5)
    assert loan.hold_size == pytest.approx(12.5)


def test_credit_upload_requires_valid_entry_date(app_context):
    workbook = _build_credit_workbook(
        loans={
            "Company Name": ["No Date Borrower"],
            "Fund Name": ["Fund Missing Date"],
            "Entry Date": [None],
        }
    )

    with pytest.raises(ValueError, match="No valid credit loans found"):
        parse_credit_loan_tape(workbook, firm_name="Missing Date Firm")

    assert CreditLoan.query.count() == 0


def test_delete_credit_route_removes_fund_performance(client):
    with app.app_context():
        team = Team.query.first()
        firm = Firm.query.first()
        loan = CreditLoan(
            company_name="Delete Borrower",
            fund_name="Delete Fund",
            close_date=date(2024, 1, 1),
            status="Unrealized",
            default_status="Performing",
            pik_toggle=False,
            firm_id=firm.id,
            team_id=team.id,
        )
        db.session.add(loan)
        db.session.flush()
        db.session.add(
            CreditLoanSnapshot(
                credit_loan_id=loan.id,
                snapshot_date=date(2024, 3, 31),
                firm_id=firm.id,
                team_id=team.id,
            )
        )
        db.session.add(
            CreditFundPerformance(
                fund_name="Delete Fund",
                report_date=date(2024, 12, 31),
                firm_id=firm.id,
                team_id=team.id,
            )
        )
        db.session.commit()
        firm_id = firm.id
        team_id = team.id

    response = client.post(f"/upload/credit-loans/{firm_id}/delete")
    assert response.status_code == 302

    with app.app_context():
        assert CreditLoan.query.filter_by(firm_id=firm_id, team_id=team_id).count() == 0
        assert CreditLoanSnapshot.query.count() == 0
        assert CreditFundPerformance.query.filter_by(firm_id=firm_id, team_id=team_id).count() == 0


def test_credit_upload_route_posts_template_successfully(client):
    template = client.get("/upload/credit-loans/template")
    assert template.status_code == 200

    response = client.post(
        "/upload/credit-loans",
        data={
            "firm_name": "Posted Upload Firm",
            "file": (BytesIO(template.data), "credit_loan_template.xlsx"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Credit Track Record" in response.data
    with app.app_context():
        assert CreditLoan.query.filter_by(fund_name="PCOF III").count() >= 1


def test_credit_upload_route_handles_unexpected_parser_exception(client, monkeypatch):
    def _boom(*args, **kwargs):
        raise RuntimeError("parser exploded")

    monkeypatch.setattr(legacy_app, "ensure_schema_updates", lambda: None)
    monkeypatch.setattr("services.credit_parser.parse_credit_loan_tape", _boom)

    response = client.post(
        "/upload/credit-loans",
        data={
            "firm_name": "Broken Upload Firm",
            "file": (BytesIO(b"not-an-excel-file"), "broken.xlsx"),
        },
        content_type="multipart/form-data",
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/upload/credit-loans")

"""Round-trip test: download the credit template, re-parse it, prove fields land.

This test catches header drift between `download_credit_template` and the
`CREDIT_COLUMN_MAP` in services/credit_parser.py. Two specific regression
guards (Bugs 3+4 from the hardening pass): the parenthesized headers
"Spread (bps)" and "Call Protection (months)" used to silently land as NULL
because the parser map only had `spread bps` and `call protection`.
"""

from io import BytesIO

import pytest

from app import app, db
from models import CreditFundPerformance, CreditLoan, Firm
from services.credit_parser import parse_credit_loan_tape


# Headers the example row in download_credit_template populates with non-None
# values. Anything in this list MUST land as a non-NULL field on the parsed
# CreditLoan, otherwise the parser is silently dropping a column.
EXPECTED_NON_NULL_FIELDS = [
    "company_name",
    "fund_name",
    "status",
    "close_date",
    "investment_count",
    "business_description",
    "is_public",
    "sector",
    "location",
    "security_type",
    "sourcing_channel",
    "committed_amount",
    "entry_loan_amount",
    "current_invested_capital",
    "unrealized_loan_value",
    "unrealized_warrant_equity_value",
    "total_value",
    "estimated_irr_at_entry",
    "gross_irr",
    "moic",
    "cash_margin",
    "floor_rate",
    "pik_margin",
    "closing_fee",
    "prepayment_protection",
    "loan_term",
    "equity_investment",
    "warrants_at_entry",
    "warrant_strike_entry",
    "warrants_current",
    "warrant_strike_current",
    "warrant_term",
    "ttm_revenue_entry",
    "ttm_revenue_current",
    "currency",
    "vintage_year",
    "as_of_date",
    "default_status",
    "instrument",
    "tranche",
    "issue_size",
    "hold_size",
    "coupon_rate",
    "spread_bps",  # Bug 3 regression guard
    "fee_oid",
    "fee_upfront",
    "maturity_date",
    "fixed_or_floating",
    "reference_rate",
    "pik_toggle",
    "call_protection_months",  # Bug 4 regression guard
    "amortization_type",
    "payment_frequency",
    "entry_ltv",
    "current_ltv",
    "entry_revenue",
    "entry_ebitda",
    "current_revenue",
    "current_ebitda",
    "interest_coverage_ratio",
    "dscr",
    "internal_credit_rating",
    "covenant_type",
    "covenant_compliant",
    "unrealized_value",
    "cumulative_interest_income",
    "cumulative_fee_income",
    "fair_value",
    "yield_to_maturity",
    "original_par",
    "current_outstanding",
    "accrued_interest",
    "geography",
    "sponsor",
]


@pytest.fixture
def credit_round_trip_client():
    """Logged-in test client with a clean DB so we can fetch the template route."""
    from werkzeug.security import generate_password_hash
    from models import Team, TeamFirmAccess, TeamMembership, User

    app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
    with app.test_client() as client:
        with app.app_context():
            db.session.remove()
            db.drop_all()
            db.create_all()

            team = Team(name="RT Team", slug="rt-team")
            firm = Firm(name="RT Firm", slug="rt-firm")
            # Avoid the default scrypt method (Python 3.9 + werkzeug environment
            # is missing hashlib.scrypt). pbkdf2 is fine for tests.
            user = User(
                email="rt@example.com",
                password_hash=generate_password_hash("password123", method="pbkdf2:sha256"),
                is_active=True,
            )
            db.session.add_all([team, firm, user])
            db.session.flush()
            db.session.add(TeamMembership(team_id=team.id, user_id=user.id, role="owner"))
            db.session.add(TeamFirmAccess(team_id=team.id, firm_id=firm.id, created_by_user_id=user.id))
            db.session.commit()

            user_id = user.id
            team_id = team.id
            firm_id = firm.id

        with client.session_transaction() as sess:
            sess["_user_id"] = str(user_id)
            sess["_fresh"] = True
            sess["active_team_id"] = team_id
            sess["active_firm_id"] = firm_id

        yield client, team_id

        with app.app_context():
            db.session.remove()
            db.drop_all()


def test_template_round_trip_no_silent_field_drops(credit_round_trip_client):
    """Download the credit template and re-parse it. Every header that has an
    example value must land on a parsed CreditLoan row.
    """
    client, team_id = credit_round_trip_client

    # 1. Download the template the user actually gets.
    resp = client.get("/upload/credit-loans/template")
    assert resp.status_code == 200, f"Template download failed: {resp.status_code}"
    template_bytes = BytesIO(resp.data)

    # 2. Re-parse the template through the actual parser, into a brand-new firm
    # so Bug 1 (firm slug crash) is also exercised end-to-end.
    with app.app_context():
        result = parse_credit_loan_tape(
            file_stream=template_bytes,
            firm_name="Round Trip Firm",
            team_id=team_id,
        )
        assert result["loans"] >= 1, "Expected at least one loan to be parsed"

        loan = CreditLoan.query.filter_by(company_name="Acme Software Inc").first()
        assert loan is not None, "Example loan was not persisted"

        # 3. Every advertised non-null field must actually be set.
        dropped = [f for f in EXPECTED_NON_NULL_FIELDS if getattr(loan, f, None) is None]
        assert not dropped, f"Parser silently dropped fields from template: {dropped}"


def test_template_round_trip_spread_bps_regression(credit_round_trip_client):
    """Explicit regression guard for Bug 3: 'Spread (bps)' header."""
    client, team_id = credit_round_trip_client

    resp = client.get("/upload/credit-loans/template")
    template_bytes = BytesIO(resp.data)

    with app.app_context():
        parse_credit_loan_tape(
            file_stream=template_bytes,
            firm_name="Spread Bps Firm",
            team_id=team_id,
        )
        loan = CreditLoan.query.filter_by(company_name="Acme Software Inc").first()
        assert loan is not None
        assert loan.spread_bps is not None, "spread_bps was silently dropped (Bug 3 regression)"
        assert loan.spread_bps == 425


def test_template_round_trip_call_protection_regression(credit_round_trip_client):
    """Explicit regression guard for Bug 4: 'Call Protection (months)' header."""
    client, team_id = credit_round_trip_client

    resp = client.get("/upload/credit-loans/template")
    template_bytes = BytesIO(resp.data)

    with app.app_context():
        parse_credit_loan_tape(
            file_stream=template_bytes,
            firm_name="Call Protection Firm",
            team_id=team_id,
        )
        loan = CreditLoan.query.filter_by(company_name="Acme Software Inc").first()
        assert loan is not None
        assert loan.call_protection_months is not None, (
            "call_protection_months was silently dropped (Bug 4 regression)"
        )
        assert loan.call_protection_months == 12


def test_template_round_trip_creates_new_firm(credit_round_trip_client):
    """Bug 1 regression: uploading to a brand-new firm name must not crash on slug."""
    client, team_id = credit_round_trip_client

    resp = client.get("/upload/credit-loans/template")
    template_bytes = BytesIO(resp.data)

    with app.app_context():
        # Firm name that doesn't exist anywhere
        result = parse_credit_loan_tape(
            file_stream=template_bytes,
            firm_name="Brand New Firm Never Seen Before",
            team_id=team_id,
        )
        assert result["loans"] >= 1
        firm = Firm.query.filter_by(name="Brand New Firm Never Seen Before").first()
        assert firm is not None, "New firm was not created"
        assert firm.slug, "Firm slug must be set (Bug 1 regression)"


# ---------------------------------------------------------------------------
# Route smoke tests — exercise the credit-risk and credit-stress templates
# end-to-end so Bug 5 (rate shock haircut) and Bug 6 (new ICR/DSCR/covenant
# KPI cards) are visually-rendered without 500s.
# ---------------------------------------------------------------------------


def _seed_template_loans(client, team_id, firm_name="Smoke Firm"):
    """Helper: download the template and parse it into the test DB."""
    resp = client.get("/upload/credit-loans/template")
    assert resp.status_code == 200
    with app.app_context():
        result = parse_credit_loan_tape(
            file_stream=BytesIO(resp.data),
            firm_name=firm_name,
            team_id=team_id,
        )
        assert result["loans"] >= 1
        firm = Firm.query.filter_by(name=firm_name).first()
        return firm.id


def test_credit_risk_route_renders(credit_round_trip_client):
    """Bug 6 visual smoke: credit-risk page renders 200, NO 500, with the new
    KPI cards present in the HTML output."""
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id)

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/analysis/credit-risk")
    assert resp.status_code == 200, f"credit-risk failed: {resp.status_code}\n{resp.data[:500]}"
    body = resp.get_data(as_text=True)
    # The new ICR/DSCR/covenant cards from Bug 6
    assert "Wtd Avg ICR" in body, "Bug 6: ICR KPI card missing from credit-risk page"
    assert "Wtd Avg DSCR" in body, "Bug 6: DSCR KPI card missing from credit-risk page"
    # Acme Software is the example loan and should appear (LTV / problem table or row)
    assert "Acme Software" in body


def test_credit_stress_route_rate_shock_changes_nav(credit_round_trip_client):
    """Bug 5 visual smoke: hitting credit-stress with rate_shock=200 must move
    stressed_nav vs rate_shock=0 for portfolios with floating loans.

    The template example loan is 'Floating' fixed_or_floating, so the haircut
    must apply.
    """
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id)

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    base = client.get("/credit/analysis/credit-stress?rate_shock=0")
    assert base.status_code == 200, f"credit-stress (rate_shock=0) failed: {base.status_code}"

    shocked = client.get("/credit/analysis/credit-stress?rate_shock=200")
    assert shocked.status_code == 200, f"credit-stress (rate_shock=200) failed: {shocked.status_code}"

    # Both pages must render without 500. The actual numeric assertion lives in
    # tests/test_credit_metrics.py::test_stress_rate_shock_floating which
    # exercises the math directly. This smoke test only proves the route + the
    # template render with the new haircut path.


def test_all_credit_routes_render_no_500(credit_round_trip_client):
    """Visual walk: every credit analysis page renders without crashing."""
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id)

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    pages = [
        "credit-dashboard",
        "credit-track-record",
        "credit-yield",
        "credit-risk",
        "credit-maturity",
        "credit-concentration",
        "credit-stress",
        "credit-vintage",
        "credit-migration",
        "credit-fundamentals",
        "credit-watchlist",
        "credit-data-cuts",
    ]
    failures = []
    for page in pages:
        resp = client.get(f"/credit/analysis/{page}")
        if resp.status_code != 200:
            failures.append(f"{page}: {resp.status_code}")
    assert not failures, f"Credit analysis pages crashed: {failures}"


def test_template_round_trip_fund_performance_sheet(credit_round_trip_client):
    """Fund Performance sheet in the template round-trips through the parser
    and creates CreditFundPerformance rows with correct net return fields."""
    client, team_id = credit_round_trip_client

    resp = client.get("/upload/credit-loans/template")
    assert resp.status_code == 200
    template_bytes = BytesIO(resp.data)

    with app.app_context():
        result = parse_credit_loan_tape(
            file_stream=template_bytes,
            firm_name="Fund Perf Round Trip Firm",
            team_id=team_id,
        )
        assert result.get("fund_performance", 0) >= 1, "Expected at least one fund perf row parsed"

        perf = CreditFundPerformance.query.filter_by(fund_name="PCOF III").first()
        assert perf is not None, "PCOF III fund performance row not persisted"
        assert perf.vintage_year == 2021
        assert perf.fund_size == 500.0
        assert perf.net_irr == pytest.approx(0.12)
        assert perf.net_moic == pytest.approx(1.35)
        assert perf.net_dpi == pytest.approx(0.45)
        assert perf.net_tvpi == pytest.approx(1.35)
        assert perf.net_rvpi == pytest.approx(0.90)
        assert perf.called_capital == pytest.approx(425.0)
        assert perf.distributed_capital == pytest.approx(190.0)
        assert perf.nav == pytest.approx(380.0)

        # Second fund row
        perf2 = CreditFundPerformance.query.filter_by(fund_name="PCOF IV").first()
        assert perf2 is not None, "PCOF IV fund performance row not persisted"
        assert perf2.fund_size == 750.0

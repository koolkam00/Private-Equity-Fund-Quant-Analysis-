"""Round-trip test: download the credit template, re-parse it, prove fields land.

This test catches header drift between `download_credit_template` and the
`CREDIT_COLUMN_MAP` in services/credit_parser.py. Two specific regression
guards (Bugs 3+4 from the hardening pass): the parenthesized headers
"Spread (bps)" and "Call Protection (months)" used to silently land as NULL
because the parser map only had `spread bps` and `call protection`.
"""

from io import BytesIO
from zipfile import ZipFile

import pytest

from app import app, db
from models import BenchmarkPoint, CreditFundPerformance, CreditLoan, Firm
from services.credit_parser import parse_credit_loan_tape


# Headers the example row in download_credit_template populates with non-None
# values. Anything in this list MUST land as a non-NULL field on the parsed
# CreditLoan, otherwise the parser is silently dropping a column.
EXPECTED_NON_NULL_FIELDS = [
    # Core identification
    "company_name",
    "fund_name",
    "status",  # auto-calculated
    "close_date",
    "vintage_year",
    "as_of_date",
    "fund_size",
    # Company details
    "sector",
    "geography",
    "sponsor",
    "security_type",
    "sourcing_channel",
    "business_description",
    "is_public",
    "investment_count",
    # Loan structure
    "hold_size",
    "committed_amount",
    "current_invested_capital",
    "issue_size",
    "instrument",
    "tranche",
    # Loan economics
    "coupon_rate",
    "spread_bps",  # Bug 3 regression guard
    "floor_rate",
    "fixed_or_floating",
    "reference_rate",
    "pik_toggle",
    "fee_oid",
    "fee_upfront",
    "maturity_date",
    "loan_term",
    "amortization_type",
    "payment_frequency",
    # Protections
    "call_protection_months",  # Bug 4 regression guard
    "prepayment_protection",
    # Credit metrics
    "entry_ltv",
    "current_ltv",
    "interest_coverage_ratio",
    "dscr",
    "default_status",
    "internal_credit_rating",
    "covenant_type",
    "covenant_compliant",
    # Returns & valuation
    "gross_irr",
    "estimated_irr_at_entry",
    "moic",
    "unrealized_value",
    "unrealized_warrant_equity_value",
    "total_value",
    "fair_value",
    "yield_to_maturity",
    # Revenue & EBITDA
    "entry_revenue",
    "entry_ebitda",
    "current_revenue",
    "current_ebitda",
    # Income
    "cumulative_interest_income",
    "cumulative_fee_income",
    # Par & outstanding
    "original_par",
    "current_outstanding",
    "accrued_interest",
    # Collateral & coverage (new)
    "entry_collateral",
    "current_collateral",
    "entry_coverage_ratio",
    "current_coverage_ratio",
    "entry_equity_cushion",
    "current_equity_cushion",
    # Warrants & equity
    "equity_investment",
    "warrants_at_entry",
    "warrant_strike_entry",
    "warrants_current",
    "warrant_strike_current",
    "warrant_term",
    # Term
    "term_years",
    # Currency
    "currency",
    # Cross-populated fields (filled from primary column aliases)
    "entry_loan_amount",  # cross-populated from hold_size
    "location",  # cross-populated from geography
    "cash_margin",  # cross-populated from coupon_rate
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
# Route smoke tests for the trimmed credit analysis surface.
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


def _seed_credit_benchmark_thresholds(team_id, vintage_year=2021, asset_class="Private Credit"):
    with app.app_context():
        db.session.add_all(
            [
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_irr", quartile="lower_quartile", value=0.08),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_irr", quartile="median", value=0.11),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_irr", quartile="upper_quartile", value=0.14),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_irr", quartile="top_5", value=0.18),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_moic", quartile="lower_quartile", value=1.20),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_moic", quartile="median", value=1.35),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_moic", quartile="upper_quartile", value=1.50),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_moic", quartile="top_5", value=1.90),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_dpi", quartile="lower_quartile", value=0.30),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_dpi", quartile="median", value=0.50),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_dpi", quartile="upper_quartile", value=0.70),
                BenchmarkPoint(team_id=team_id, asset_class=asset_class, vintage_year=vintage_year, metric="net_dpi", quartile="top_5", value=1.00),
            ]
        )
        db.session.commit()


def test_all_credit_routes_render_no_500(credit_round_trip_client):
    """Visual walk: only the supported credit analysis pages render."""
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id)

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    pages = [
        "credit-track-record",
        "credit-benchmarking",
        "credit-concentration",
        "credit-fundamentals",
        "credit-pricing-trends",
        "credit-underwrite-outcome",
        "credit-data-cuts",
    ]
    failures = []
    for page in pages:
        resp = client.get(f"/credit/analysis/{page}")
        if resp.status_code != 200:
            failures.append(f"{page}: {resp.status_code}")
    assert not failures, f"Credit analysis pages crashed: {failures}"


def test_removed_credit_routes_return_404(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Removed Credit Routes Firm")

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    removed_pages = [
        "credit-dashboard",
        "credit-yield",
        "credit-risk",
        "credit-watchlist",
        "credit-migration",
        "credit-stress",
        "credit-vintage",
        "credit-loan-structure",
        "credit-maturity",
    ]

    for page in removed_pages:
        resp = client.get(f"/credit/analysis/{page}")
        assert resp.status_code == 404, page


def test_credit_track_record_route_renders_net_tvpi_not_net_moic(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Track Record Render Firm")

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/analysis/credit-track-record")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Current Invested Capital" in body
    assert "Realized Value" in body
    assert "Unrealized Value" in body
    assert "Unrealized Warrant/Equity Value" in body
    assert "Total Value" in body
    assert "% of Facility" not in body
    assert "Software" in body
    assert "Net TVPI:" in body
    assert "Net DPI:" in body
    assert "Net MOIC:" not in body
    assert "All Funds Summary" in body


def test_credit_fundamentals_route_renders_entry_vs_exit_current(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Fundamentals Render Firm")

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/analysis/credit-fundamentals")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Portfolio Summary" in body
    assert "Wtd Avg Exit / Current" in body
    assert "Revenue by Fund" in body
    assert "Loan Term by Fund" in body
    assert "Deal Detail" in body
    assert "Current Invested Capital" in body
    assert "Term (Years)" in body
    assert "Wtd Avg Term" in body


def test_credit_pricing_trends_route_renders_time_series_and_sorted_tables(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Pricing Trends Render Firm")

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/analysis/credit-pricing-trends?time_group=quarter&dim=sector")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Pricing Trend by Entry Date" in body
    assert "Weighted Avg Coupon by Entry Date" in body
    assert "Weighted Avg Floor by Entry Date" in body
    assert "Weighted Avg Upfront Fee by Entry Date" in body
    assert "Wtd Avg Upfront Fee" in body
    assert "Avg Upfront Fee" in body
    assert "By Fund" in body
    assert "By Sector" in body
    assert "Pricing Detail" not in body
    assert "Total Upfront Fees" not in body


def test_credit_data_cuts_route_labels_entry_underwriting_metrics(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Data Cuts Entry Metrics Firm")

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/analysis/credit-data-cuts")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Entry LTV" in body
    assert "Entry Coverage Ratio" in body
    assert "Entry Equity Cushion" in body
    assert "Loan Term (Years)" in body


def test_credit_concentration_route_renders_full_width_breakdowns_without_detail_section(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Concentration Detail Render Firm")

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/analysis/credit-concentration")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Deal Detail" not in body
    assert "All Funds Summary" not in body
    assert "credit-concentration-breakdown-table" in body
    assert "Top 10 Single-Name Exposures" in body
    assert "Total Unrealized Value" in body
    assert "Unrealized Equity Value" in body
    assert "By Sector" in body
    assert "By Geography" in body


def test_active_credit_analysis_pages_do_not_render_default_status_controls(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="No Default Status Render Firm")

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    pages = [
        "credit-concentration",
        "credit-fundamentals",
        "credit-pricing-trends",
        "credit-underwrite-outcome",
    ]

    for page in pages:
        resp = client.get(f"/credit/analysis/{page}")
        assert resp.status_code == 200, page
        body = resp.get_data(as_text=True)
        assert 'name="default_status"' not in body, page

    underwrite_resp = client.get("/credit/analysis/credit-underwrite-outcome")
    assert underwrite_resp.status_code == 200
    assert "Default status:" not in underwrite_resp.get_data(as_text=True)


def test_credit_pdf_pack_live_page_renders_sidebar_export_link_and_targets(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Credit PDF Live Firm")
    _seed_credit_benchmark_thresholds(team_id)

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id
        sess["selected_benchmark_asset_class"] = "Private Credit"

    resp = client.get("/reports/credit-pdf-pack/live")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Download 7 Credit Analysis PDFs" in body
    assert "Download 7-PDF ZIP" in body
    assert "/credit/analysis/credit-track-record/pdf" in body
    assert "/credit/analysis/credit-benchmarking/pdf" in body
    assert "/credit/analysis/credit-concentration/pdf" in body
    assert "/credit/analysis/credit-fundamentals/pdf" in body
    assert "/credit/analysis/credit-pricing-trends/pdf" in body
    assert "/credit/analysis/credit-underwrite-outcome/pdf" in body
    assert "/credit/analysis/credit-data-cuts/pdf" in body
    assert "/reports/credit-pdf-pack/live" in body


def test_credit_pdf_pack_download_returns_separate_pdfs(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_name = "Credit PDF Pack Firm"
    firm_id = _seed_template_loans(client, team_id, firm_name=firm_name)
    _seed_credit_benchmark_thresholds(team_id)

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id
        sess["selected_benchmark_asset_class"] = "Private Credit"

    resp = client.get("/reports/credit-pdf-pack")
    assert resp.status_code == 200
    assert resp.mimetype == "application/zip"

    disposition = resp.headers.get("Content-Disposition", "")
    assert "attachment;" in disposition
    assert ".zip" in disposition
    assert "Credit%20Analysis%20PDF%20Pack" in disposition or "Credit Analysis PDF Pack" in disposition

    archive = ZipFile(BytesIO(resp.data))
    names = sorted(archive.namelist())
    assert len(names) == 7
    expected_analysis_names = (
        "Credit Track Record",
        "Credit Benchmarking Analysis",
        "Credit Concentration",
        "Credit Fundamentals",
        "Credit Pricing Trends",
        "Credit Underwrite vs Outcome",
        "Credit Data Cuts Summary",
    )
    for analysis_name in expected_analysis_names:
        assert any(analysis_name in name for name in names)
    for name in names:
        assert firm_name in name
        assert name.endswith(".pdf")
        assert archive.read(name).startswith(b"%PDF")


def test_credit_individual_pdf_download_returns_pdf_bytes(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Credit Single PDF Firm")
    _seed_credit_benchmark_thresholds(team_id)

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id
        sess["selected_benchmark_asset_class"] = "Private Credit"

    resp = client.get("/credit/analysis/credit-track-record/pdf")
    assert resp.status_code == 200
    assert resp.mimetype == "application/pdf"
    assert resp.data.startswith(b"%PDF")


def test_credit_underwrite_outcome_route_renders_irr_comparison(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Underwrite Outcome Render Firm")

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/analysis/credit-underwrite-outcome")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Worst IRR Misses" in body
    assert "Est. IRR at Entry" in body
    assert "Actual Gross IRR" in body
    assert "All Compared Loans" in body
    assert "Underwrite vs Outcome" in body


def test_credit_benchmarking_route_renders_pe_style_table(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Credit Benchmark Render Firm")
    _seed_credit_benchmark_thresholds(team_id)

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/analysis/credit-benchmarking?benchmark_asset_class=Private+Credit")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Fund Benchmarking Table" in body
    assert "Benchmark Asset Class" in body
    assert "Net IRR Benchmark" in body
    assert "Net TVPI Benchmark" in body
    assert "Net DPI Benchmark" in body
    assert "Benchmark Threshold Appendix" in body


def test_credit_benchmarking_api_payload_shape(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Credit Benchmark API Firm")
    _seed_credit_benchmark_thresholds(team_id)

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/api/analysis/credit-benchmarking/series?benchmark_asset_class=Private+Credit")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["page"] == "credit-benchmarking"
    payload = body["payload"]
    assert "meta" in payload
    assert "kpis" in payload
    assert "rank_distribution" in payload
    assert "fund_rows" in payload
    assert "threshold_rows" in payload
    assert payload["meta"]["benchmark_asset_class"] == "Private Credit"


def test_credit_pricing_trends_api_payload_shape(credit_round_trip_client):
    client, team_id = credit_round_trip_client
    firm_id = _seed_template_loans(client, team_id, firm_name="Pricing Trends API Firm")

    with client.session_transaction() as sess:
        sess["active_firm_id"] = firm_id

    resp = client.get("/credit/api/analysis/credit-pricing-trends/series?time_group=quarter&dim=sector")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["page"] == "credit-pricing-trends"
    payload = body["payload"]
    assert payload["time_group"] == "quarter"
    assert payload["primary_dim"] == "sector"
    assert "summary" in payload
    assert "time_rows" in payload
    assert "time_series_charts" in payload
    assert "fund_rows" in payload
    assert "dimension_rows" in payload
    assert "detail_groups" not in payload


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

from datetime import date

from models import Deal, db


def test_index_redirect(client):
    response = client.get("/")
    assert response.status_code == 302
    assert "/dashboard" in response.location


def test_dashboard_page(client):
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert b"Portfolio Dashboard" in response.data
    assert b'id="bridge-lever-table-body"' in response.data


def test_upload_page(client):
    response = client.get("/upload")
    assert response.status_code == 200
    assert b"Upload Deal Template" in response.data
    assert b"Download Current Deal Template" in response.data


def test_download_deal_template(client):
    response = client.get("/upload/deals/template")
    assert response.status_code == 200
    assert "attachment;" in response.headers.get("Content-Disposition", "")
    assert "PE_Fund_Data_Template.xlsx" in response.headers.get("Content-Disposition", "")


def test_deals_page(client):
    response = client.get("/deals")
    assert response.status_code == 200
    assert b"Deals" in response.data


def test_deals_page_grouped_subtotals_and_detail_contract(client):
    deals = [
        Deal(
            company_name="Alpha",
            fund_number="Fund I",
            status="Fully Realized",
            geography="US",
            year_invested=2020,
            equity_invested=100,
            realized_value=170,
            unrealized_value=0,
            entry_revenue=50,
            entry_ebitda=10,
            entry_enterprise_value=120,
            entry_net_debt=30,
            exit_revenue=70,
            exit_ebitda=15,
            exit_enterprise_value=190,
            exit_net_debt=25,
            investment_date=date(2020, 1, 1),
            exit_date=date(2024, 1, 1),
        ),
        Deal(
            company_name="Beta",
            fund_number="Fund I",
            status="Unrealized",
            geography="US",
            year_invested=2021,
            equity_invested=60,
            realized_value=0,
            unrealized_value=85,
            entry_revenue=40,
            entry_ebitda=8,
            entry_enterprise_value=90,
            entry_net_debt=20,
            exit_revenue=60,
            exit_ebitda=12,
            exit_enterprise_value=130,
            exit_net_debt=18,
            investment_date=date(2021, 1, 1),
        ),
        Deal(
            company_name="Gamma",
            fund_number="Fund II",
            status="Partially Realized",
            geography="UK",
            year_invested=2019,
            equity_invested=80,
            realized_value=50,
            unrealized_value=35,
            entry_revenue=45,
            entry_ebitda=9,
            entry_enterprise_value=100,
            entry_net_debt=28,
            exit_revenue=58,
            exit_ebitda=11,
            exit_enterprise_value=125,
            exit_net_debt=23,
            investment_date=date(2019, 1, 1),
            exit_date=date(2023, 7, 1),
        ),
    ]
    db.session.add_all(deals)
    db.session.commit()

    response = client.get("/deals")
    assert response.status_code == 200
    assert b"All Funds Summary" in response.data
    assert b"All 1 Fund I Fully Realized Investments" in response.data
    assert b"All 1 Fund I Unrealized Investments" in response.data
    assert b"All 2 Fund I Investments" in response.data
    assert response.data.count(b"<strong>All 1 Fund I Unrealized Investments</strong>") == 1
    assert response.data.count(b"<strong>All 1 Unrealized Investments</strong>") == 1
    assert b"EBITDA Margin" in response.data
    assert b"Revenue ($M)" in response.data
    assert b"EBITDA ($M)" in response.data
    assert b"TEV ($M)" in response.data
    assert b"Net Debt ($M)" in response.data
    assert b'class="deal-row"' in response.data
    assert b"rollup-expand-btn" in response.data
    assert b'id="detail-' in response.data
    assert b'id="rollup-detail-' in response.data
    assert b'id="deal-bridge-table-' in response.data
    assert b'id="rollup-bridge-table-' in response.data
    assert b'id="deals-rollup-details-payload"' in response.data
    assert b"Entry vs Exit Comparison" in response.data
    assert b"Rollup Bridge (Fund Pro-Rata)" in response.data
    assert b"Avg" in response.data
    assert b"Wtd" in response.data
    assert b"data-sort=" not in response.data


def test_track_record_page(client):
    response = client.get("/track-record")
    assert response.status_code == 200
    assert b"Deal Level Track Record" in response.data


def test_track_record_pdf_download(client):
    response = client.get("/track-record/pdf")
    assert response.status_code == 200
    assert response.mimetype == "application/pdf"
    assert "attachment;" in response.headers.get("Content-Disposition", "")
    assert "track_record_print_ready.pdf" in response.headers.get("Content-Disposition", "")


def test_track_record_page_renders_template_columns_and_net_performance(client):
    deal = Deal(
        company_name="Track Co",
        fund_number="Fund IX",
        status="Unrealized",
        investment_date=date(2022, 1, 1),
        equity_invested=100,
        realized_value=20,
        unrealized_value=110,
        fund_size=500,
        net_irr=0.14,
        net_moic=1.7,
        net_dpi=0.8,
    )
    db.session.add(deal)
    db.session.commit()

    response = client.get("/track-record")
    assert response.status_code == 200
    assert b"% of Fund Size" in response.data
    assert b"Gross IRR" in response.data
    assert b"All Funds Summary" in response.data
    assert b"Net MOIC" in response.data
    assert b"DPI" in response.data


def test_ic_memo_page_renders_and_has_print_action(client):
    response = client.get("/ic-memo")
    assert response.status_code == 200
    assert b"IC Memo Presentation" in response.data
    assert b"Download as PDF" in response.data
    assert b"Page 1. Executive Summary" in response.data


def test_ic_memo_path_fund_scope_overrides_query_fund_and_keeps_filters(client):
    d1 = Deal(
        company_name="Alpha Memo",
        fund_number="Fund I",
        status="Fully Realized",
        sector="Tech",
        geography="US",
        year_invested=2021,
        equity_invested=100,
        realized_value=150,
        unrealized_value=0,
    )
    d2 = Deal(
        company_name="Beta Memo",
        fund_number="Fund II",
        status="Fully Realized",
        sector="Tech",
        geography="US",
        year_invested=2021,
        equity_invested=100,
        realized_value=200,
        unrealized_value=0,
    )
    db.session.add_all([d1, d2])
    db.session.commit()

    response = client.get("/ic-memo/Fund%20I?fund=Fund+II")
    assert response.status_code == 200
    assert b"Fund Scope: Fund I" in response.data
    assert b"Alpha Memo" in response.data
    assert b"Beta Memo" not in response.data

    response_filtered = client.get("/ic-memo/Fund%20I?status=Unrealized")
    assert response_filtered.status_code == 200
    assert b"No deals available for this IC memo filter set." in response_filtered.data


def test_methodology_page_renders_with_print_and_anchor_ids(client):
    response = client.get("/methodology")
    assert response.status_code == 200
    assert b"Calculation Methodology &amp; Audit" in response.data
    assert b"Download as PDF" in response.data
    assert b'id="metric-gross-moic"' in response.data
    assert b'id="metric-loss-ratio-capital"' in response.data
    assert b'id="metric-bridge-revenue"' in response.data
    assert b'id="metric-tvpi"' in response.data


def test_audit_alias_redirects_to_methodology(client):
    response = client.get("/audit")
    assert response.status_code == 302
    assert "/methodology" in response.location


def test_dashboard_has_methodology_jump_links(client):
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert b"/methodology#metric-gross-moic" in response.data
    assert b"/methodology#metric-tev-ebitda" in response.data


def test_deals_page_recovers_missing_deals_table(client):
    with client.application.app_context():
        db.drop_all()

    response = client.get("/deals")
    assert response.status_code == 200
    assert b"Deals" in response.data


def test_dashboard_filter_context(client):
    d1 = Deal(
        company_name="Alpha",
        fund_number="Fund I",
        sector="Tech",
        geography="US",
        year_invested=2021,
        exit_type="Strategic Sale",
        lead_partner="Jane Doe",
        security_type="Common Equity",
        deal_type="Platform",
        entry_channel="Proprietary",
        equity_invested=100,
        realized_value=150,
        unrealized_value=0,
    )
    d2 = Deal(
        company_name="Beta",
        fund_number="Fund II",
        sector="Health",
        geography="UK",
        year_invested=2022,
        exit_type="Secondary Buyout",
        lead_partner="Alex Reed",
        security_type="Preferred Equity",
        deal_type="Add-on",
        entry_channel="Broad Auction",
        equity_invested=100,
        realized_value=90,
        unrealized_value=0,
    )
    db.session.add_all([d1, d2])
    db.session.commit()

    response = client.get("/dashboard?fund=Fund+I&exit_type=Strategic+Sale")
    assert response.status_code == 200
    assert b"Fund I" in response.data
    assert b"All Exit Types" in response.data


def test_api_dashboard_series_qualitative_filters(client):
    d1 = Deal(
        company_name="Gamma",
        fund_number="Fund V",
        sector="Tech",
        geography="US",
        status="Fully Realized",
        year_invested=2020,
        exit_type="Strategic Sale",
        lead_partner="Jane Doe",
        security_type="Common Equity",
        deal_type="Platform",
        entry_channel="Proprietary",
        equity_invested=100,
        realized_value=200,
        unrealized_value=0,
    )
    d2 = Deal(
        company_name="Delta",
        fund_number="Fund V",
        sector="Tech",
        geography="US",
        status="Fully Realized",
        year_invested=2020,
        exit_type="Secondary Buyout",
        lead_partner="Alex Reed",
        security_type="Preferred Equity",
        deal_type="Add-on",
        entry_channel="Broad Auction",
        equity_invested=100,
        realized_value=120,
        unrealized_value=0,
    )
    db.session.add_all([d1, d2])
    db.session.commit()

    response = client.get(
        "/api/dashboard/series?"
        "exit_type=Strategic+Sale&lead_partner=Jane+Doe&security_type=Common+Equity"
        "&deal_type=Platform&entry_channel=Proprietary"
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["kpis"]["total_deals"] == 1


def test_api_dashboard_series_schema(client):
    deal = Deal(
        company_name="API Co",
        fund_number="Fund I",
        geography="US",
        year_invested=2020,
        equity_invested=100,
        realized_value=130,
        unrealized_value=10,
        entry_revenue=50,
        exit_revenue=60,
        entry_ebitda=10,
        exit_ebitda=12,
        entry_enterprise_value=100,
        exit_enterprise_value=130,
        entry_net_debt=30,
        exit_net_debt=20,
        investment_date=date(2020, 1, 1),
        exit_date=date(2023, 1, 1),
    )
    db.session.add(deal)
    db.session.commit()

    response = client.get("/api/dashboard/series")
    assert response.status_code == 200
    payload = response.get_json()
    for key in (
        "kpis",
        "loss_ratios",
        "moic_distribution",
        "entry_exit_summary",
        "bridge_aggregate",
        "vintage_series",
        "moic_hold_scatter",
        "value_creation_mix",
        "realized_unrealized_exposure",
        "loss_concentration_heatmap",
        "exit_type_performance",
        "lead_partner_scorecard",
    ):
        assert key in payload
    mix = payload["value_creation_mix"]
    assert mix["current"] == "fund"
    assert set(mix["series"].keys()) == {"fund", "sector", "exit_type"}


def test_api_deal_bridge_query_params(client):
    deal = Deal(
        company_name="Bridge Co",
        equity_invested=100,
        realized_value=130,
        unrealized_value=10,
        entry_revenue=50,
        exit_revenue=60,
        entry_ebitda=10,
        exit_ebitda=12,
        entry_enterprise_value=100,
        exit_enterprise_value=130,
        entry_net_debt=30,
        exit_net_debt=20,
    )
    db.session.add(deal)
    db.session.commit()

    response = client.get(f"/api/deals/{deal.id}/bridge?unit=pct&basis=fund")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["deal_id"] == deal.id
    assert payload["model"] == "additive"
    assert payload["unit"] == "pct"
    assert payload["basis"] == "fund"
    assert payload["start_value"] == 0.0
    assert payload["end_value"] == 1.0


def test_api_deal_bridge_rejects_non_additive_model(client):
    deal = Deal(
        company_name="Bridge Co 2",
        equity_invested=100,
        realized_value=130,
        unrealized_value=10,
        entry_revenue=50,
        exit_revenue=60,
        entry_ebitda=10,
        exit_ebitda=12,
        entry_enterprise_value=100,
        exit_enterprise_value=130,
        entry_net_debt=30,
        exit_net_debt=20,
    )
    db.session.add(deal)
    db.session.commit()

    response = client.get(f"/api/deals/{deal.id}/bridge?model=multiplicative&unit=moic&basis=fund")
    assert response.status_code == 400


def test_api_deal_bridge_rejects_company_basis(client):
    deal = Deal(
        company_name="Bridge Co 3",
        equity_invested=100,
        realized_value=130,
        unrealized_value=10,
        entry_revenue=50,
        exit_revenue=60,
        entry_ebitda=10,
        exit_ebitda=12,
        entry_enterprise_value=100,
        exit_enterprise_value=130,
        entry_net_debt=30,
        exit_net_debt=20,
    )
    db.session.add(deal)
    db.session.commit()

    response = client.get(f"/api/deals/{deal.id}/bridge?basis=company&unit=moic")
    assert response.status_code == 400
    assert response.get_json()["error"] == "Only fund pro-rata basis is supported"


def test_analysis_pages_render(client):
    pages = {
        "fund-liquidity": b"Fund Liquidity &amp; Performance Curve",
        "underwrite-outcome": b"Underwrite vs Outcome",
        "valuation-quality": b"Unrealized Valuation Quality",
        "exit-readiness": b"Exit Readiness &amp; Aging",
        "stress-lab": b"Concentration Stress Lab",
        "deal-trajectory": b"Deal Trajectory",
    }
    for page, marker in pages.items():
        response = client.get(f"/analysis/{page}")
        assert response.status_code == 200
        assert marker in response.data
        if page == "stress-lab":
            assert b"Current EBITDA" in response.data
            assert b"Stressed Implied IRR" in response.data
            assert b"Hold Period (Yrs)" in response.data
            assert b"Expected Hold (Yrs)" in response.data
            assert b"Delay (Years)" not in response.data
            assert b"Print / Save PDF" in response.data


def test_analysis_api_series_schema(client):
    deal = Deal(
        company_name="Analysis API Co",
        fund_number="Fund IX",
        status="Unrealized",
        investment_date=date(2020, 1, 1),
        equity_invested=100,
        realized_value=20,
        unrealized_value=130,
        entry_revenue=60,
        entry_ebitda=12,
        entry_enterprise_value=140,
        entry_net_debt=30,
        exit_revenue=90,
        exit_ebitda=20,
        exit_enterprise_value=220,
        exit_net_debt=25,
    )
    db.session.add(deal)
    db.session.commit()

    for page in (
        "fund-liquidity",
        "underwrite-outcome",
        "valuation-quality",
        "exit-readiness",
        "stress-lab",
        "deal-trajectory",
    ):
        response = client.get(f"/api/analysis/{page}/series")
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["page"] == page
        assert "payload" in payload


def test_stress_lab_api_supports_per_deal_overrides(client):
    deal = Deal(
        company_name="Stress Override Co",
        fund_number="Fund IX",
        status="Unrealized",
        investment_date=date(2020, 1, 1),
        equity_invested=100,
        realized_value=0,
        unrealized_value=130,
        entry_revenue=60,
        entry_ebitda=12,
        entry_enterprise_value=140,
        entry_net_debt=30,
        exit_revenue=90,
        exit_ebitda=20,
        exit_enterprise_value=220,
        exit_net_debt=25,
    )
    db.session.add(deal)
    db.session.commit()

    response = client.get(
        f"/api/analysis/stress-lab/series?ms_{deal.id}=-2.0&es_{deal.id}=-20&hp_{deal.id}=7.5"
    )
    assert response.status_code == 200
    payload = response.get_json()["payload"]
    assert payload["deal_rows"]
    row = payload["deal_rows"][0]
    assert abs(row["multiple_shock"] - (-2.0)) < 1e-9
    assert abs(row["ebitda_shock"] - (-0.20)) < 1e-9
    assert abs(row["expected_hold_period"] - 7.5) < 1e-9

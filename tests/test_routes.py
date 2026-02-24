from datetime import date
from io import BytesIO

from openpyxl import load_workbook

from models import (
    Deal,
    DealCashflowEvent,
    DealQuarterSnapshot,
    DealUnderwriteBaseline,
    Firm,
    FundQuarterSnapshot,
    Team,
    TeamFirmAccess,
    TeamMembership,
    UploadIssue,
    db,
)


def _with_active_scope(deal):
    membership = TeamMembership.query.order_by(TeamMembership.id.asc()).first()
    assert membership is not None
    access = (
        TeamFirmAccess.query.filter_by(team_id=membership.team_id)
        .order_by(TeamFirmAccess.id.asc())
        .first()
    )
    assert access is not None
    deal.team_id = membership.team_id
    deal.firm_id = access.firm_id
    return deal


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
    assert b"Recent Uploads" in response.data
    assert b"Firm Currency" in response.data


def test_upload_page_lists_recent_batches_for_active_firm(client):
    membership = TeamMembership.query.order_by(TeamMembership.id.asc()).first()
    assert membership is not None
    access = TeamFirmAccess.query.filter_by(team_id=membership.team_id).first()
    assert access is not None

    deal = Deal(
        company_name="Upload History Co",
        fund_number="Fund Hist",
        team_id=membership.team_id,
        firm_id=access.firm_id,
        upload_batch="batchhist",
        equity_invested=10,
        realized_value=12,
        unrealized_value=0,
    )
    db.session.add(deal)
    db.session.commit()

    response = client.get("/upload")
    assert response.status_code == 200
    assert b"batchhist" in response.data
    assert b"Delete Upload" in response.data


def test_delete_upload_batch_removes_only_active_firm_records(client):
    membership = TeamMembership.query.order_by(TeamMembership.id.asc()).first()
    assert membership is not None
    access = TeamFirmAccess.query.filter_by(team_id=membership.team_id).first()
    assert access is not None
    active_firm_id = access.firm_id
    other_firm = Firm(name="Other Upload Firm", slug="other-upload-firm")
    db.session.add(other_firm)
    db.session.flush()

    deal = Deal(
        company_name="Delete Upload Co",
        fund_number="Fund Del",
        team_id=membership.team_id,
        firm_id=active_firm_id,
        upload_batch="deadbeef",
        equity_invested=20,
        realized_value=25,
        unrealized_value=0,
        investment_date=date(2020, 1, 1),
        exit_date=date(2023, 1, 1),
    )
    db.session.add(deal)
    db.session.flush()
    db.session.add_all(
        [
            DealCashflowEvent(
                deal_id=deal.id,
                event_date=date(2021, 1, 1),
                event_type="Distribution",
                amount=5,
                team_id=membership.team_id,
                firm_id=active_firm_id,
                upload_batch="deadbeef",
            ),
            DealQuarterSnapshot(
                deal_id=deal.id,
                quarter_end=date(2022, 12, 31),
                team_id=membership.team_id,
                firm_id=active_firm_id,
                upload_batch="deadbeef",
            ),
            DealUnderwriteBaseline(
                deal_id=deal.id,
                baseline_date=date(2020, 1, 1),
                team_id=membership.team_id,
                firm_id=active_firm_id,
                upload_batch="deadbeef",
            ),
            FundQuarterSnapshot(
                fund_number="Fund Del",
                quarter_end=date(2022, 12, 31),
                team_id=membership.team_id,
                firm_id=active_firm_id,
                upload_batch="deadbeef",
            ),
            UploadIssue(
                issue_report_id="issue-del-1",
                team_id=membership.team_id,
                firm_id=active_firm_id,
                upload_batch="deadbeef",
                file_type="deals",
                row_number=2,
                severity="warning",
                message="Synthetic issue",
            ),
        ]
    )

    foreign_deal = Deal(
        company_name="Other Firm Batch Co",
        fund_number="Fund X",
        team_id=membership.team_id,
        firm_id=other_firm.id,
        upload_batch="deadbeef",
        equity_invested=15,
        realized_value=16,
        unrealized_value=0,
    )
    db.session.add(foreign_deal)
    db.session.commit()

    response = client.post("/upload/batches/deadbeef/delete", follow_redirects=False)
    assert response.status_code == 302
    assert response.location.endswith("/upload")

    assert Deal.query.filter_by(firm_id=active_firm_id, upload_batch="deadbeef").count() == 0
    assert DealCashflowEvent.query.filter_by(firm_id=active_firm_id, upload_batch="deadbeef").count() == 0
    assert DealQuarterSnapshot.query.filter_by(firm_id=active_firm_id, upload_batch="deadbeef").count() == 0
    assert DealUnderwriteBaseline.query.filter_by(firm_id=active_firm_id, upload_batch="deadbeef").count() == 0
    assert FundQuarterSnapshot.query.filter_by(firm_id=active_firm_id, upload_batch="deadbeef").count() == 0
    assert UploadIssue.query.filter_by(firm_id=active_firm_id, upload_batch="deadbeef").count() == 0
    assert Deal.query.filter_by(firm_id=other_firm.id, upload_batch="deadbeef").count() == 1


def test_download_deal_template(client):
    response = client.get("/upload/deals/template")
    assert response.status_code == 200
    assert "attachment;" in response.headers.get("Content-Disposition", "")
    assert "PE_Fund_Data_Template.xlsx" in response.headers.get("Content-Disposition", "")
    wb = load_workbook(BytesIO(response.data), read_only=True)
    ws = wb["Deals"]
    headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    assert "Firm Currency" in headers


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
    db.session.add_all([_with_active_scope(d) for d in deals])
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
    assert b"Revenue (USD $M)" in response.data
    assert b"EBITDA (USD $M)" in response.data
    assert b"TEV (USD $M)" in response.data
    assert b"Net Debt (USD $M)" in response.data
    assert b"Revenue CAGR" in response.data
    assert b"Revenue Cumulative" in response.data
    assert b"EBITDA CAGR" in response.data
    assert b"EBITDA Cumulative" in response.data
    assert b"<th>Sector</th>" not in response.data
    assert b"<th>Geo</th>" not in response.data
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
    db.session.add(_with_active_scope(deal))
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
    db.session.add_all([_with_active_scope(d1), _with_active_scope(d2)])
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
    assert response.status_code == 302
    assert "/auth/login" in response.location


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
    db.session.add_all([_with_active_scope(d1), _with_active_scope(d2)])
    db.session.commit()

    response = client.get("/dashboard?fund=Fund+I&exit_type=Strategic+Sale")
    assert response.status_code == 200
    assert b"Fund I" in response.data
    assert b"All Exit Types" in response.data


def test_dashboard_fund_summary_table(client):
    d1 = Deal(
        company_name="Alpha",
        fund_number="Fund I",
        status="Unrealized",
        investment_date=date(2017, 1, 1),
        equity_invested=100,
        realized_value=0,
        unrealized_value=140,
        fund_size=500,
        net_irr=0.14,
        net_moic=1.8,
        net_dpi=0.4,
    )
    d2 = Deal(
        company_name="Beta",
        fund_number="Fund II",
        status="Fully Realized",
        investment_date=date(2019, 1, 1),
        equity_invested=120,
        realized_value=210,
        unrealized_value=0,
        fund_size=600,
        net_irr=0.22,
        net_moic=2.1,
        net_dpi=1.1,
    )
    db.session.add_all([_with_active_scope(d1), _with_active_scope(d2)])
    db.session.commit()

    response = client.get("/dashboard")
    assert response.status_code == 200
    assert b"Fund Summary" in response.data
    assert b"Vintage Year" in response.data
    assert b"Fund Size" in response.data
    assert b"Net IRR" in response.data
    assert b"Net MOIC" in response.data
    assert b"Net DPI" in response.data
    assert b"Fund I" in response.data
    assert b"Fund II" in response.data
    assert b"2017" in response.data
    assert b"2019" in response.data
    assert b"14.0%" in response.data
    assert b"1.80x" in response.data
    assert b"0.40x" in response.data


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
    db.session.add_all([_with_active_scope(d1), _with_active_scope(d2)])
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
    db.session.add(_with_active_scope(deal))
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
    assert "fallback_ready_count" in payload["bridge_aggregate"]


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
    db.session.add(_with_active_scope(deal))
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
    assert payload["calculation_method"] == "ebitda_additive"
    assert payload["fallback_reason"] is None


def test_api_deal_bridge_negative_ebitda_uses_fallback_method(client):
    deal = Deal(
        company_name="Bridge Fallback Co",
        equity_invested=100,
        realized_value=130,
        unrealized_value=10,
        entry_revenue=50,
        exit_revenue=60,
        entry_ebitda=-10,
        exit_ebitda=-8,
        entry_enterprise_value=100,
        exit_enterprise_value=130,
        entry_net_debt=30,
        exit_net_debt=20,
    )
    db.session.add(_with_active_scope(deal))
    db.session.commit()

    response = client.get(f"/api/deals/{deal.id}/bridge?unit=moic&basis=fund")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ready"] is True
    assert payload["calculation_method"] == "revenue_multiple_fallback"
    assert payload["fallback_reason"] == "negative_ebitda"


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
    db.session.add(_with_active_scope(deal))
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
    db.session.add(_with_active_scope(deal))
    db.session.commit()

    response = client.get(f"/api/deals/{deal.id}/bridge?basis=company&unit=moic")
    assert response.status_code == 400
    assert response.get_json()["error"] == "Only fund pro-rata basis is supported"


def test_api_deal_bridge_firm_scope_returns_404_for_other_firm_deal(client):
    other_firm = Firm(name="Other Firm", slug="other-firm")
    db.session.add(other_firm)
    db.session.flush()

    foreign_deal = Deal(
        company_name="Foreign Bridge Co",
        fund_number="Fund Other",
        firm_id=other_firm.id,
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
    db.session.add(foreign_deal)
    db.session.commit()

    response = client.get(f"/api/deals/{foreign_deal.id}/bridge?unit=moic&basis=fund")
    assert response.status_code == 404


def test_manage_firms_page_and_scope_switch(client):
    membership = TeamMembership.query.order_by(TeamMembership.id.asc()).first()
    assert membership is not None

    firm_a = Firm(name="Firm Scope A", slug="firm-scope-a")
    firm_b = Firm(name="Firm Scope B", slug="firm-scope-b")
    firm_hidden = Firm(name="Firm Hidden", slug="firm-hidden")
    db.session.add_all([firm_a, firm_b])
    db.session.flush()
    db.session.add(firm_hidden)
    db.session.flush()

    db.session.add_all(
        [
            TeamFirmAccess(team_id=membership.team_id, firm_id=firm_a.id),
            TeamFirmAccess(team_id=membership.team_id, firm_id=firm_b.id),
        ]
    )

    db.session.add(
        Deal(
            company_name="Firm Scope Deal B",
            fund_number="Fund B",
            firm_id=firm_b.id,
            equity_invested=75,
            realized_value=95,
            unrealized_value=0,
        )
    )
    db.session.commit()

    page = client.get("/firms")
    assert page.status_code == 200
    assert b"Manage Firms" in page.data
    assert b"Firm Scope B" in page.data
    assert b"Firm Hidden" not in page.data
    assert b"Currency" in page.data

    select = client.post(f"/firms/{firm_b.id}/select", follow_redirects=False)
    assert select.status_code == 302

    with client.session_transaction() as sess:
        assert sess.get("active_firm_id") == firm_b.id

    scoped = client.get("/deals")
    assert scoped.status_code == 200
    assert b"Firm Scope Deal B" in scoped.data

    denied = client.post(f"/firms/{firm_hidden.id}/select", follow_redirects=False)
    assert denied.status_code == 302


def test_rendered_pages_expose_active_currency_metadata(client):
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert b'data-currency-code="USD"' in response.data


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
    db.session.add(_with_active_scope(deal))
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
    db.session.add(_with_active_scope(deal))
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


def test_protected_routes_redirect_when_not_logged_in(anonymous_client):
    response = anonymous_client.get("/dashboard")
    assert response.status_code == 302
    assert "/auth/login" in response.location


def test_manage_funds_routes_are_deprecated(client):
    page = client.get("/funds", follow_redirects=False)
    assert page.status_code == 302
    assert "/firms" in page.location

    select = client.post("/funds/Fund%20One/select", follow_redirects=False)
    assert select.status_code == 302

    delete_resp = client.post("/funds/Fund%20One/delete", follow_redirects=False)
    assert delete_resp.status_code == 410

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


def test_upload_page(client):
    response = client.get("/upload")
    assert response.status_code == 200
    assert b"Upload Deal Template" in response.data


def test_deals_page(client):
    response = client.get("/deals")
    assert response.status_code == 200
    assert b"Deals" in response.data


def test_dashboard_filter_context(client):
    d1 = Deal(company_name="Alpha", fund_number="Fund I", sector="Tech", geography="US", year_invested=2021, equity_invested=100, realized_value=150, unrealized_value=0)
    d2 = Deal(company_name="Beta", fund_number="Fund II", sector="Health", geography="UK", year_invested=2022, equity_invested=100, realized_value=90, unrealized_value=0)
    db.session.add_all([d1, d2])
    db.session.commit()

    response = client.get("/dashboard?fund=Fund+I")
    assert response.status_code == 200
    assert b"Fund I" in response.data


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
    for key in ("kpis", "loss_ratios", "moic_distribution", "entry_exit_summary", "bridge_aggregate", "vintage_series"):
        assert key in payload


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

    response = client.get(f"/api/deals/{deal.id}/bridge?model=multiplicative&unit=pct&basis=fund")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["deal_id"] == deal.id
    assert payload["model"] == "multiplicative"
    assert payload["unit"] == "pct"
    assert payload["basis"] == "fund"

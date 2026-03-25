"""Tests for cross-firm deal-level comparison feature."""

from datetime import date

from app import app, db
from models import Deal, Firm, Team, TeamFirmAccess, TeamMembership


# ---------------------------------------------------------------------------
# Service layer tests
# ---------------------------------------------------------------------------


def test_compute_deal_level_comparison_basic(client):
    """Basic multi-firm deal comparison returns correct structure."""
    with app.app_context():
        firm = Firm.query.filter_by(slug="test-firm").first()
        d1 = Deal(
            company_name="Alpha Corp",
            fund_number="Fund I",
            sector="Tech",
            geography="North America",
            status="Realized",
            equity_invested=10.0,
            realized_value=25.0,
            unrealized_value=0.0,
            irr=0.35,
            investment_date=date(2018, 1, 1),
            exit_date=date(2022, 1, 1),
            year_invested=2018,
            firm_id=firm.id,
        )
        d2 = Deal(
            company_name="Beta Inc",
            fund_number="Fund I",
            sector="Healthcare",
            geography="Europe",
            status="Unrealized",
            equity_invested=20.0,
            realized_value=0.0,
            unrealized_value=30.0,
            irr=0.15,
            investment_date=date(2020, 6, 1),
            as_of_date=date(2024, 6, 1),
            year_invested=2020,
            firm_id=firm.id,
        )
        db.session.add_all([d1, d2])
        db.session.commit()

        from services.metrics.deal_comparison import compute_deal_level_comparison

        firms_data = [{
            "firm_id": firm.id,
            "firm_name": firm.name,
            "deals": [d1, d2],
            "fund_vintage_lookup": {},
        }]

        result = compute_deal_level_comparison(firms_data)

        assert "deal_rows" in result
        assert "firm_summaries" in result
        assert "filter_options" in result
        assert "kpi" in result
        assert len(result["deal_rows"]) == 2
        assert result["kpi"]["total_deals"] == 2

        # Check deal rows have required fields
        row = result["deal_rows"][0]
        for field in ("firm_id", "firm_name", "deal_id", "company_name",
                      "fund_name", "sector", "moic", "irr", "hold_period"):
            assert field in row


def test_compute_deal_level_comparison_filters(client):
    """Filters correctly narrow deal rows."""
    with app.app_context():
        firm = Firm.query.filter_by(slug="test-firm").first()
        d1 = Deal(
            company_name="Tech Co",
            fund_number="Fund I",
            sector="Tech",
            equity_invested=10.0,
            realized_value=20.0,
            firm_id=firm.id,
        )
        d2 = Deal(
            company_name="Health Co",
            fund_number="Fund I",
            sector="Healthcare",
            equity_invested=15.0,
            realized_value=18.0,
            firm_id=firm.id,
        )
        db.session.add_all([d1, d2])
        db.session.commit()

        from services.metrics.deal_comparison import compute_deal_level_comparison

        firms_data = [{
            "firm_id": firm.id,
            "firm_name": firm.name,
            "deals": [d1, d2],
            "fund_vintage_lookup": {},
        }]

        result = compute_deal_level_comparison(firms_data, filters={"sector": ["Tech"]})
        assert len(result["deal_rows"]) == 1
        assert result["deal_rows"][0]["company_name"] == "Tech Co"


def test_compute_deal_level_comparison_empty_firm(client):
    """Firm with no deals returns empty gracefully."""
    with app.app_context():
        from services.metrics.deal_comparison import compute_deal_level_comparison

        firms_data = [{
            "firm_id": 999,
            "firm_name": "Empty Firm",
            "deals": [],
            "fund_vintage_lookup": {},
        }]

        result = compute_deal_level_comparison(firms_data)
        assert result["deal_rows"] == []
        assert result["kpi"]["total_deals"] == 0


def test_compute_deal_level_comparison_none_metrics(client):
    """Deals with None metrics don't crash, show up with None values."""
    with app.app_context():
        firm = Firm.query.filter_by(slug="test-firm").first()
        d = Deal(
            company_name="Null Co",
            fund_number="Fund I",
            equity_invested=None,
            realized_value=None,
            unrealized_value=None,
            irr=None,
            firm_id=firm.id,
        )
        db.session.add(d)
        db.session.commit()

        from services.metrics.deal_comparison import compute_deal_level_comparison

        firms_data = [{
            "firm_id": firm.id,
            "firm_name": firm.name,
            "deals": [d],
            "fund_vintage_lookup": {},
        }]

        result = compute_deal_level_comparison(firms_data)
        assert len(result["deal_rows"]) == 1
        row = result["deal_rows"][0]
        assert row["moic"] is None
        assert row["irr"] is None
        assert row["hold_period"] is None


def test_compute_deal_level_comparison_moic_calculation(client):
    """MOIC is calculated correctly as (realized + unrealized) / equity."""
    with app.app_context():
        firm = Firm.query.filter_by(slug="test-firm").first()
        d = Deal(
            company_name="MOIC Test",
            fund_number="Fund I",
            equity_invested=10.0,
            realized_value=15.0,
            unrealized_value=10.0,
            firm_id=firm.id,
        )
        db.session.add(d)
        db.session.commit()

        from services.metrics.deal_comparison import compute_deal_level_comparison

        firms_data = [{
            "firm_id": firm.id,
            "firm_name": firm.name,
            "deals": [d],
            "fund_vintage_lookup": {},
        }]

        result = compute_deal_level_comparison(firms_data)
        assert result["deal_rows"][0]["moic"] == 2.5  # (15+10)/10


# ---------------------------------------------------------------------------
# Route tests
# ---------------------------------------------------------------------------


def test_deal_comparison_route_renders(client):
    """Standalone deal comparison page renders with selected firms."""
    with app.app_context():
        firm = Firm.query.filter_by(slug="test-firm").first()
        firm_id = firm.id

    resp = client.get(f"/analysis/deal-comparison?firm_ids={firm_id}")
    assert resp.status_code == 200
    assert b"Deal Performance Comparison" in resp.data


def test_deal_comparison_route_no_firms(client):
    """Deal comparison page with no firms selected shows empty state."""
    resp = client.get("/analysis/deal-comparison")
    assert resp.status_code == 200
    assert b"Select firms above to compare deals" in resp.data


def test_deal_comparison_api_returns_json(client):
    """API endpoint returns JSON with deals array."""
    with app.app_context():
        firm = Firm.query.filter_by(slug="test-firm").first()
        d = Deal(
            company_name="API Test Co",
            fund_number="Fund I",
            sector="Tech",
            equity_invested=10.0,
            realized_value=20.0,
            firm_id=firm.id,
        )
        db.session.add(d)
        db.session.commit()
        firm_id = firm.id

    resp = client.get(f"/api/fund-comparison/deals?firm_id={firm_id}&fund_name=Fund+I")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "deals" in data
    assert len(data["deals"]) == 1
    assert data["deals"][0]["company_name"] == "API Test Co"


def test_deal_comparison_api_empty_fund(client):
    """API endpoint returns empty deals for nonexistent fund."""
    with app.app_context():
        firm = Firm.query.filter_by(slug="test-firm").first()
        firm_id = firm.id

    resp = client.get(f"/api/fund-comparison/deals?firm_id={firm_id}&fund_name=NoSuchFund")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deals"] == []


def test_deal_comparison_api_access_control(client):
    """API endpoint rejects access to firms not in TeamFirmAccess."""
    resp = client.get("/api/fund-comparison/deals?firm_id=99999&fund_name=Fund+I")
    assert resp.status_code == 403


def test_deal_comparison_api_missing_params(client):
    """API endpoint returns empty deals when params are missing."""
    resp = client.get("/api/fund-comparison/deals")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deals"] == []


def test_deal_comparison_api_special_chars_fund_name(client):
    """API handles fund names with special characters."""
    with app.app_context():
        firm = Firm.query.filter_by(slug="test-firm").first()
        d = Deal(
            company_name="Special Co",
            fund_number="Fund I/II",
            equity_invested=10.0,
            firm_id=firm.id,
        )
        db.session.add(d)
        db.session.commit()
        firm_id = firm.id

    resp = client.get(f"/api/fund-comparison/deals?firm_id={firm_id}&fund_name=Fund+I%2FII")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["deals"]) == 1
    assert data["deals"][0]["company_name"] == "Special Co"

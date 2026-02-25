from datetime import date

import pytest

from models import BenchmarkPoint, Deal, Firm, Team, TeamFirmAccess, db
from services.metrics import build_chart_field_catalog, resolve_auto_chart_type, run_chart_query


def _seed_scope():
    team = Team(name="Chart Team", slug="chart-team")
    firm = Firm(name="Chart Firm", slug="chart-firm")
    db.session.add_all([team, firm])
    db.session.flush()
    db.session.add(TeamFirmAccess(team_id=team.id, firm_id=firm.id))
    db.session.commit()
    return team, firm


def test_chart_builder_catalog_contains_all_sources(app_context):
    team, firm = _seed_scope()
    catalog = build_chart_field_catalog(team.id, firm.id, {})
    keys = [row["key"] for row in catalog["sources"]]
    assert keys == ["deals", "deal_quarterly", "fund_quarterly", "cashflows", "underwrite", "benchmarks"]
    deals_source = next(row for row in catalog["sources"] if row["key"] == "deals")
    assert any(field["field"] == "company_name" for field in deals_source["dimensions"])
    assert any(field["field"] == "gross_moic" for field in deals_source["measures"])


def test_chart_builder_auto_type_rules():
    assert (
        resolve_auto_chart_type({"chart_type": "auto"}, {"x_type": "date", "has_numeric_y": True, "x_is_numeric": False, "has_size": False, "series_present": False, "x_cardinality": 12, "y_count": 1})
        == "line"
    )
    assert (
        resolve_auto_chart_type({"chart_type": "auto"}, {"x_type": "number", "has_numeric_y": True, "x_is_numeric": True, "has_size": True, "series_present": False, "x_cardinality": 50, "y_count": 1})
        == "bubble"
    )
    assert (
        resolve_auto_chart_type({"chart_type": "auto"}, {"x_type": "enum", "has_numeric_y": True, "x_is_numeric": False, "has_size": False, "series_present": False, "x_cardinality": 6, "y_count": 1})
        == "donut"
    )
    assert resolve_auto_chart_type({"chart_type": "bar"}, {"x_type": "date"}) == "bar"


def test_chart_builder_query_wavg_on_deals(app_context):
    team, firm = _seed_scope()
    db.session.add_all(
        [
            Deal(
                company_name="Alpha",
                fund_number="Fund I",
                investment_date=date(2020, 1, 1),
                team_id=team.id,
                firm_id=firm.id,
                equity_invested=100,
                realized_value=200,
                unrealized_value=0,
            ),
            Deal(
                company_name="Beta",
                fund_number="Fund I",
                investment_date=date(2021, 1, 1),
                team_id=team.id,
                firm_id=firm.id,
                equity_invested=300,
                realized_value=450,
                unrealized_value=0,
            ),
        ]
    )
    db.session.commit()

    payload = run_chart_query(
        {
            "source": "deals",
            "chart_type": "bar",
            "x": {"field": "fund_number"},
            "y": [{"field": "gross_moic", "agg": "wavg"}],
            "limit": 50,
        },
        team_id=team.id,
        firm_id=firm.id,
        global_filters={},
    )
    assert payload["chart_type_resolved"] == "bar"
    assert payload["labels"] == ["Fund I"]
    assert abs(payload["datasets"][0]["data"][0] - 1.625) < 1e-9


def test_chart_builder_query_invalid_field_raises(app_context):
    team, firm = _seed_scope()
    with pytest.raises(ValueError):
        run_chart_query(
            {
                "source": "deals",
                "chart_type": "auto",
                "x": {"field": "fund_number"},
                "y": [{"field": "not_a_real_field", "agg": "sum"}],
            },
            team_id=team.id,
            firm_id=firm.id,
            global_filters={},
        )


def test_chart_builder_query_applies_local_filters(app_context):
    team, firm = _seed_scope()
    db.session.add_all(
        [
            Deal(
                company_name="Alpha",
                fund_number="Fund I",
                investment_date=date(2020, 1, 1),
                team_id=team.id,
                firm_id=firm.id,
                equity_invested=120,
                realized_value=180,
                unrealized_value=0,
            ),
            Deal(
                company_name="Beta",
                fund_number="Fund II",
                investment_date=date(2021, 1, 1),
                team_id=team.id,
                firm_id=firm.id,
                equity_invested=80,
                realized_value=96,
                unrealized_value=0,
            ),
        ]
    )
    db.session.commit()

    payload = run_chart_query(
        {
            "source": "deals",
            "chart_type": "bar",
            "x": {"field": "fund_number"},
            "y": [{"field": "equity_invested", "agg": "sum"}],
            "filters": [
                {"field": "fund_number", "op": "eq", "value": "Fund II"},
            ],
        },
        team_id=team.id,
        firm_id=firm.id,
        global_filters={},
    )
    assert payload["labels"] == ["Fund II"]
    assert payload["datasets"][0]["data"] == [80.0]


def test_chart_builder_benchmarks_source_team_scoped(app_context):
    team, firm = _seed_scope()
    other_team = Team(name="Other Chart Team", slug="other-chart-team")
    db.session.add(other_team)
    db.session.flush()
    db.session.add_all(
        [
            BenchmarkPoint(team_id=team.id, asset_class="Buyout", vintage_year=2020, metric="net_irr", quartile="median", value=0.18),
            BenchmarkPoint(team_id=other_team.id, asset_class="Buyout", vintage_year=2020, metric="net_irr", quartile="median", value=0.30),
        ]
    )
    db.session.commit()

    payload = run_chart_query(
        {
            "source": "benchmarks",
            "chart_type": "auto",
            "x": {"field": "asset_class"},
            "y": [{"field": "value", "agg": "avg"}],
        },
        team_id=team.id,
        firm_id=firm.id,
        global_filters={},
    )
    assert payload["labels"] == ["Buyout"]
    assert abs(payload["datasets"][0]["data"][0] - 0.18) < 1e-9

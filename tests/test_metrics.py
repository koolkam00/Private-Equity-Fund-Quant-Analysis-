from datetime import date
from types import SimpleNamespace

from services.metrics import (
    compute_bridge_aggregate,
    compute_bridge_view,
    compute_deal_metrics,
    compute_loss_and_distribution,
    compute_portfolio_analytics,
    safe_divide,
    safe_log,
    safe_power,
)


def _make_deal(**kwargs):
    defaults = {
        "id": 1,
        "company_name": "Test Co",
        "fund_number": "Fund I",
        "sector": "Tech",
        "geography": "US",
        "status": "Fully Realized",
        "investment_date": date(2020, 1, 1),
        "year_invested": 2020,
        "exit_date": date(2023, 1, 1),
        "equity_invested": 100,
        "realized_value": 200,
        "unrealized_value": 0,
        "ownership_pct": None,
        "entry_revenue": 50,
        "entry_ebitda": 10,
        "entry_enterprise_value": 200,
        "entry_net_debt": 100,
        "exit_revenue": 100,
        "exit_ebitda": 25,
        "exit_enterprise_value": 400,
        "exit_net_debt": 50,
        "irr": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_safe_helpers():
    assert safe_divide(10, 5) == 2
    assert safe_divide(10, 0) is None
    assert abs(safe_power(4, 0.5) - 2.0) < 1e-9
    assert safe_power(-1, 0.5) is None
    assert safe_log(1) == 0
    assert safe_log(-1) is None


def test_deal_metrics_core_identity():
    deal = _make_deal()
    m = compute_deal_metrics(deal)
    assert abs(m["moic"] - 2.0) < 1e-9
    assert abs(m["value_created"] - 100.0) < 1e-9
    assert m["implied_irr"] is not None


def test_additive_bridge_reconciles_exactly():
    deal = _make_deal()
    b = compute_bridge_view(deal, model="additive", basis="fund", unit="dollar", warnings=[])
    assert b["ready"] is True
    drivers = b["drivers_dollar"]
    total = drivers["revenue"] + drivers["margin"] + drivers["multiple"] + drivers["leverage"] + drivers["other"]
    expected = (deal.realized_value + deal.unrealized_value - deal.equity_invested)
    assert abs(total - expected) < 1e-6


def test_multiplicative_bridge_ready_positive_inputs():
    deal = _make_deal(
        entry_revenue=100,
        exit_revenue=140,
        entry_ebitda=20,
        exit_ebitda=35,
        entry_enterprise_value=200,
        exit_enterprise_value=420,
    )
    b = compute_bridge_view(deal, model="multiplicative", basis="fund", unit="dollar", warnings=[])
    assert b["ready"] is True


def test_multiplicative_bridge_low_confidence_negative_inputs():
    deal = _make_deal(entry_revenue=-10)
    b = compute_bridge_view(deal, model="multiplicative", basis="fund", unit="dollar", warnings=[])
    assert b["ready"] is False
    assert b["low_confidence_bridge"] is True


def test_ownership_override_precedence():
    deal = _make_deal(ownership_pct=0.4)
    b = compute_bridge_view(deal, model="additive", basis="fund", unit="dollar", warnings=[])
    assert abs(b["ownership_pct"] - 0.4) < 1e-9


def test_loss_ratios_dynamic():
    d1 = _make_deal(id=1, equity_invested=100, realized_value=80, unrealized_value=0)  # 0.8x
    d2 = _make_deal(id=2, equity_invested=100, realized_value=150, unrealized_value=0)  # 1.5x
    metrics = {1: compute_deal_metrics(d1), 2: compute_deal_metrics(d2)}
    risk = compute_loss_and_distribution([d1, d2], metrics_by_id=metrics)
    assert abs(risk["loss_ratios"]["count_pct"] - 50.0) < 1e-9
    assert abs(risk["loss_ratios"]["capital_pct"] - 50.0) < 1e-9


def test_portfolio_analytics_entry_exit_weighted():
    d1 = _make_deal(id=1, equity_invested=100, entry_enterprise_value=100, entry_ebitda=10)
    d2 = _make_deal(id=2, equity_invested=300, entry_enterprise_value=600, entry_ebitda=30)
    metrics = {1: compute_deal_metrics(d1), 2: compute_deal_metrics(d2)}
    p = compute_portfolio_analytics([d1, d2], metrics_by_id=metrics)
    # Entry TEV/EBITDA: d1=10x, d2=20x, weighted=(10*100 + 20*300)/400 = 17.5x
    assert abs(p["entry"]["tev_ebitda"]["wavg"] - 17.5) < 1e-9


def test_bridge_aggregate_outputs_three_units():
    deals = [_make_deal(id=1), _make_deal(id=2, equity_invested=200, realized_value=420, unrealized_value=0)]
    agg = compute_bridge_aggregate(deals, model="additive", basis="fund")
    assert "dollar" in agg["drivers"]
    assert "moic" in agg["drivers"]
    assert "pct" in agg["drivers"]

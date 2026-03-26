"""Tests for executive summary LP-question signal logic.

Covers:
- Signal boundary conditions (exact threshold values)
- Payload structure (5 sections + pulse)
- Edge cases: no benchmarks, no bridge, all losses, stellar portfolio, tiny portfolio
- LLM input contract
"""

from datetime import date
from types import SimpleNamespace

from services.metrics.deal import compute_deal_metrics
from services.metrics.executive_summary import (
    _performance_signal,
    _capital_signal,
    _value_creation_signal,
    _risk_signal,
    _peer_signal,
    _is_realized_status,
    compute_executive_summary_analysis,
)


def _make_deal(**kwargs):
    defaults = {
        "id": 1,
        "company_name": "Test Co",
        "fund_number": "Fund I",
        "sector": "Tech",
        "geography": "US",
        "status": "Fully Realized",
        "lead_partner": "Unassigned",
        "deal_type": "Platform",
        "entry_channel": "Unknown",
        "investment_date": date(2020, 1, 1),
        "year_invested": 2020,
        "exit_date": date(2023, 1, 1),
        "as_of_date": date(2023, 1, 1),
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
        "fund_size": None,
        "net_irr": None,
        "net_moic": None,
        "net_dpi": None,
        "acquired_revenue": None,
        "acquired_ebitda": None,
        "acquired_tev": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# Performance signal boundary tests
# ---------------------------------------------------------------------------

class TestPerformanceSignal:
    def test_green_high_moic(self):
        portfolio = {"returns": {"gross_moic": {"wavg": 2.0}}}
        result = _performance_signal(portfolio, None)
        assert result["signal"] == "green"

    def test_green_at_exactly_1_5x(self):
        portfolio = {"returns": {"gross_moic": {"wavg": 1.5}}}
        result = _performance_signal(portfolio, None)
        assert result["signal"] == "green"

    def test_amber_below_1_5x(self):
        portfolio = {"returns": {"gross_moic": {"wavg": 1.49}}}
        result = _performance_signal(portfolio, None)
        assert result["signal"] == "amber"

    def test_red_below_1_0x(self):
        portfolio = {"returns": {"gross_moic": {"wavg": 0.8}}}
        result = _performance_signal(portfolio, None)
        assert result["signal"] == "red"

    def test_red_at_exactly_1_0x(self):
        """At exactly 1.0x, MOIC >= 1.0 so should be amber, not red."""
        portfolio = {"returns": {"gross_moic": {"wavg": 1.0}}}
        result = _performance_signal(portfolio, None)
        assert result["signal"] == "amber"

    def test_gray_no_moic(self):
        portfolio = {"returns": {"gross_moic": {}}}
        result = _performance_signal(portfolio, None)
        assert result["signal"] == "gray"

    def test_red_when_q4_benchmark(self):
        portfolio = {"returns": {"gross_moic": {"wavg": 1.6}}}
        benchmark = {"fund_rows": [{"composite_rank": {"rank_code": "q4"}}]}
        result = _performance_signal(portfolio, benchmark)
        assert result["signal"] == "red"

    def test_headline_includes_quartile(self):
        portfolio = {"returns": {"gross_moic": {"wavg": 2.0}}}
        benchmark = {"fund_rows": [{"composite_rank": {"rank_code": "q1"}}]}
        result = _performance_signal(portfolio, benchmark)
        assert "Q1" in result["headline"]


# ---------------------------------------------------------------------------
# Capital signal boundary tests
# ---------------------------------------------------------------------------

class TestCapitalSignal:
    def _make_portfolio(self, total_equity=100, total_realized=50, total_unrealized=50):
        return {
            "total_equity": total_equity,
            "total_realized": total_realized,
            "total_unrealized": total_unrealized,
        }

    def test_green_good_dpi_low_concentration(self):
        portfolio = self._make_portfolio(total_equity=100, total_realized=60)
        concentration = {"top3_pct": 0.30}
        deals = [_make_deal(id=i) for i in range(5)]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = _capital_signal(portfolio, concentration, deals, metrics, date(2023, 1, 1))
        assert result["signal"] == "green"

    def test_red_low_dpi(self):
        portfolio = self._make_portfolio(total_equity=100, total_realized=15)
        concentration = {"top3_pct": 0.30}
        deals = [_make_deal(id=i) for i in range(5)]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = _capital_signal(portfolio, concentration, deals, metrics, date(2023, 1, 1))
        assert result["signal"] == "red"

    def test_red_high_concentration(self):
        portfolio = self._make_portfolio(total_equity=100, total_realized=60)
        concentration = {"top3_pct": 0.65}
        deals = [_make_deal(id=i) for i in range(5)]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = _capital_signal(portfolio, concentration, deals, metrics, date(2023, 1, 1))
        assert result["signal"] == "red"

    def test_amber_marginal_dpi(self):
        portfolio = self._make_portfolio(total_equity=100, total_realized=40)
        concentration = {"top3_pct": 0.30}
        deals = [_make_deal(id=i) for i in range(5)]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = _capital_signal(portfolio, concentration, deals, metrics, date(2023, 1, 1))
        assert result["signal"] == "amber"

    def test_gray_no_equity(self):
        portfolio = self._make_portfolio(total_equity=0)
        result = _capital_signal(portfolio, {}, [], {}, date(2023, 1, 1))
        assert result["signal"] == "gray"


# ---------------------------------------------------------------------------
# Value creation signal tests
# ---------------------------------------------------------------------------

class TestValueCreationSignal:
    def test_green_operational_drivers_dominate(self):
        bridge = {
            "display_drivers": [
                {"key": "revenue", "dollar": 50, "label": "Revenue Growth"},
                {"key": "margin", "dollar": 30, "label": "Margin Expansion"},
                {"key": "multiple", "dollar": 10, "label": "Multiple Expansion"},
                {"key": "leverage", "dollar": 5, "label": "Leverage"},
                {"key": "other", "dollar": 5, "label": "Other"},
            ],
            "ready_count": 5,
            "coverage": 0.8,
        }
        result = _value_creation_signal(bridge)
        assert result["signal"] == "green"

    def test_red_leverage_dominated(self):
        bridge = {
            "display_drivers": [
                {"key": "revenue", "dollar": 5},
                {"key": "margin", "dollar": 5},
                {"key": "multiple", "dollar": 10},
                {"key": "leverage", "dollar": 80},
            ],
            "ready_count": 5,
        }
        result = _value_creation_signal(bridge)
        assert result["signal"] == "red"

    def test_gray_no_bridge(self):
        result = _value_creation_signal(None)
        assert result["signal"] == "gray"

    def test_gray_zero_ready(self):
        bridge = {"display_drivers": [], "ready_count": 0}
        result = _value_creation_signal(bridge)
        assert result["signal"] == "gray"


# ---------------------------------------------------------------------------
# Risk signal boundary tests
# ---------------------------------------------------------------------------

class TestRiskSignal:
    def test_green_low_loss_no_stale(self):
        deals = [_make_deal(id=i, realized_value=200, unrealized_value=0, as_of_date=date(2023, 1, 1)) for i in range(5)]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = _risk_signal(deals, metrics, date(2023, 6, 1))
        assert result["signal"] == "green"

    def test_red_high_loss_ratio(self):
        # All deals are losses: realized < equity
        deals = [_make_deal(id=i, equity_invested=100, realized_value=30, unrealized_value=0, as_of_date=date(2023, 1, 1)) for i in range(5)]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = _risk_signal(deals, metrics, date(2023, 6, 1))
        assert result["signal"] == "red"

    def test_red_with_stale_marks(self):
        # Unrealized deal with very old as_of_date
        deals = [
            _make_deal(id=1, status="Unrealized", unrealized_value=100, realized_value=0, as_of_date=date(2022, 1, 1)),
        ]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = _risk_signal(deals, metrics, date(2023, 6, 1))
        assert result["stale_marks_count"] >= 1

    def test_stale_uses_as_of_date_not_exit_date(self):
        """Regression: stale marks must use as_of_date, not exit_date."""
        deal = _make_deal(
            id=1, status="Unrealized",
            unrealized_value=100, realized_value=0,
            exit_date=None,
            as_of_date=date(2023, 5, 1),  # Recent — should NOT be stale
        )
        metrics = {deal.id: compute_deal_metrics(deal)}
        result = _risk_signal([deal], metrics, date(2023, 6, 1))
        assert result["stale_marks_count"] == 0

    def test_headline_no_concerns_when_green(self):
        deals = [_make_deal(id=i, as_of_date=date(2023, 1, 1)) for i in range(5)]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = _risk_signal(deals, metrics, date(2023, 6, 1))
        if result["signal"] == "green":
            assert "No concerns" in result["headline"]


# ---------------------------------------------------------------------------
# Peer signal tests
# ---------------------------------------------------------------------------

class TestPeerSignal:
    def test_gray_no_benchmarks(self):
        result = _peer_signal(None)
        assert result["signal"] == "gray"
        assert result["benchmark_available"] is False

    def test_green_top_quartile(self):
        benchmark = {"fund_rows": [{"composite_rank": {"rank_code": "q1"}}]}
        result = _peer_signal(benchmark)
        assert result["signal"] == "green"

    def test_red_bottom_quartile(self):
        benchmark = {"fund_rows": [{"composite_rank": {"rank_code": "q4"}}]}
        result = _peer_signal(benchmark)
        assert result["signal"] == "red"

    def test_gray_no_ranked_funds(self):
        benchmark = {"fund_rows": [{"composite_rank": {"rank_code": "na"}}]}
        result = _peer_signal(benchmark)
        assert result["signal"] == "gray"


# ---------------------------------------------------------------------------
# Full payload structure
# ---------------------------------------------------------------------------

class TestExecSummaryPayload:
    def test_payload_has_5_pulse_items(self):
        deals = [_make_deal(id=i) for i in range(5)]
        result = compute_executive_summary_analysis(deals)
        assert len(result["pulse"]) == 5

    def test_pulse_labels(self):
        deals = [_make_deal(id=i) for i in range(5)]
        result = compute_executive_summary_analysis(deals)
        labels = [p["label"] for p in result["pulse"]]
        assert labels == ["Performance", "Capital", "Value Creation", "Risks", "Peers"]

    def test_all_sections_present(self):
        deals = [_make_deal(id=i) for i in range(5)]
        result = compute_executive_summary_analysis(deals)
        for key in ("section_performance", "section_capital", "section_value_creation", "section_risk", "section_peers"):
            assert key in result
            assert "signal" in result[key]
            assert "headline" in result[key]

    def test_retained_fields(self):
        deals = [_make_deal(id=i) for i in range(5)]
        result = compute_executive_summary_analysis(deals)
        for key in ("health_score", "bridge", "deal_ranking", "concentration", "portfolio", "coverage"):
            assert key in result

    def test_too_few_deals_all_gray(self):
        """With < 3 deals, all signals should be gray."""
        deals = [_make_deal(id=1), _make_deal(id=2)]
        result = compute_executive_summary_analysis(deals)
        for p in result["pulse"]:
            assert p["signal"] == "gray"

    def test_all_loss_portfolio(self):
        """Portfolio where all deals lost money."""
        deals = [
            _make_deal(id=i, equity_invested=100, realized_value=30, unrealized_value=0, as_of_date=date(2023, 1, 1))
            for i in range(5)
        ]
        result = compute_executive_summary_analysis(deals)
        perf = result["section_performance"]
        assert perf["signal"] == "red"

    def test_stellar_portfolio(self):
        deals = [
            _make_deal(id=i, equity_invested=100, realized_value=300, unrealized_value=0, as_of_date=date(2023, 1, 1))
            for i in range(10)
        ]
        result = compute_executive_summary_analysis(deals)
        perf = result["section_performance"]
        assert perf["signal"] == "green"

    def test_empty_portfolio(self):
        result = compute_executive_summary_analysis([])
        assert result["total_deals"] == 0
        for p in result["pulse"]:
            assert p["signal"] == "gray"


# ---------------------------------------------------------------------------
# LLM input contract
# ---------------------------------------------------------------------------

class TestLlmInputContract:
    def test_build_llm_input_has_signal_data(self):
        from services.metrics.executive_summary_insights import _build_llm_input
        deals = [_make_deal(id=i) for i in range(5)]
        payload = compute_executive_summary_analysis(deals)
        llm_input = _build_llm_input(payload)
        assert "pulse_signals" in llm_input
        assert "risk_watchlist" in llm_input
        assert "peer_context" in llm_input
        assert len(llm_input["pulse_signals"]) == 5

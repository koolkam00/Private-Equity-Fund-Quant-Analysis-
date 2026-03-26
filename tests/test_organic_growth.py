"""Tests for organic vs acquired growth analysis.

Covers:
- Deal-level organic derivation (revenue and EBITDA)
- Organic CAGR computation
- Edge cases: no acquired data, negative organic, zero acquired, missing entry/exit
- Analysis payload structure and summary cards
- Bridge decomposition reconciliation
- Data completeness indicator
"""

from datetime import date
from types import SimpleNamespace

from services.metrics.deal import compute_deal_metrics
from services.metrics.organic_growth import compute_organic_growth_analysis


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
        "as_of_date": None,
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
# Deal-level organic derivation
# ---------------------------------------------------------------------------

class TestOrganicDerivation:
    def test_organic_revenue_with_acquired(self):
        deal = _make_deal(
            entry_revenue=50, exit_revenue=100,
            acquired_revenue=20,
        )
        m = compute_deal_metrics(deal)
        assert m["total_revenue_growth"] == 50  # 100 - 50
        assert m["acquired_revenue_contribution"] == 20
        assert m["organic_revenue_growth"] == 30  # 50 - 20
        assert abs(m["organic_revenue_pct"] - 0.6) < 1e-9  # 30/50
        assert abs(m["acquired_revenue_pct"] - 0.4) < 1e-9  # 20/50

    def test_organic_ebitda_with_acquired(self):
        deal = _make_deal(
            entry_ebitda=10, exit_ebitda=25,
            acquired_ebitda=5,
        )
        m = compute_deal_metrics(deal)
        assert m["total_ebitda_growth"] == 15  # 25 - 10
        assert m["acquired_ebitda_contribution"] == 5
        assert m["organic_ebitda_growth"] == 10  # 15 - 5

    def test_no_acquired_data_all_organic(self):
        """When acquired fields are None, all growth is organic."""
        deal = _make_deal(entry_revenue=50, exit_revenue=100)
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_growth"] == 50  # All organic
        assert m["acquired_revenue_contribution"] == 0
        assert m["acquired_data_status"] == "data_not_provided"

    def test_zero_acquired_treated_as_no_acquisitions(self):
        deal = _make_deal(
            entry_revenue=50, exit_revenue=100,
            acquired_revenue=0, acquired_ebitda=0, acquired_tev=0,
        )
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_growth"] == 50
        assert m["acquired_data_status"] == "data_not_provided"

    def test_negative_organic_when_acquired_exceeds_growth(self):
        """Acquired > total growth means organic declined — valid."""
        deal = _make_deal(
            entry_revenue=50, exit_revenue=80,
            acquired_revenue=40,
        )
        m = compute_deal_metrics(deal)
        assert m["total_revenue_growth"] == 30  # 80 - 50
        assert m["organic_revenue_growth"] == -10  # 30 - 40
        assert m["organic_revenue_pct"] is not None

    def test_missing_entry_exit_returns_none(self):
        deal = _make_deal(entry_revenue=None, exit_revenue=100, acquired_revenue=20)
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_growth"] is None
        assert m["total_revenue_growth"] is None

    def test_acquired_data_status_provided(self):
        deal = _make_deal(acquired_revenue=20)
        m = compute_deal_metrics(deal)
        assert m["acquired_data_status"] == "acquired_data_provided"


# ---------------------------------------------------------------------------
# Organic CAGR
# ---------------------------------------------------------------------------

class TestOrganicCagr:
    def test_organic_revenue_cagr(self):
        deal = _make_deal(
            entry_revenue=50, exit_revenue=100,
            acquired_revenue=20,
            investment_date=date(2020, 1, 1),
            exit_date=date(2023, 1, 1),
        )
        m = compute_deal_metrics(deal)
        # Organic exit = 100 - 20 = 80
        # Organic CAGR = (80/50)^(1/3) - 1 ≈ 16.96%
        assert m["organic_revenue_cagr"] is not None
        assert abs(m["organic_revenue_cagr"] - (((80 / 50) ** (1 / 3) - 1) * 100)) < 0.01

    def test_organic_ebitda_cagr(self):
        deal = _make_deal(
            entry_ebitda=10, exit_ebitda=25,
            acquired_ebitda=5,
            investment_date=date(2020, 1, 1),
            exit_date=date(2023, 1, 1),
        )
        m = compute_deal_metrics(deal)
        # Organic exit = 25 - 5 = 20
        # Organic CAGR = (20/10)^(1/3) - 1 ≈ 25.99%
        assert m["organic_ebitda_cagr"] is not None
        assert abs(m["organic_ebitda_cagr"] - (((20 / 10) ** (1 / 3) - 1) * 100)) < 0.01

    def test_no_acquired_cagr_equals_total_cagr(self):
        deal = _make_deal(entry_revenue=50, exit_revenue=100)
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_cagr"] == m["revenue_cagr"]

    def test_cagr_with_no_hold_period_returns_none(self):
        deal = _make_deal(investment_date=None, acquired_revenue=20)
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_cagr"] is None


# ---------------------------------------------------------------------------
# Analysis payload
# ---------------------------------------------------------------------------

class TestOrganicGrowthAnalysis:
    def test_payload_structure(self):
        deals = [_make_deal(id=1, acquired_revenue=20, acquired_ebitda=5)]
        result = compute_organic_growth_analysis(deals)
        assert "meta" in result
        assert "summary_cards" in result
        assert "deal_rows" in result
        assert "charts" in result
        assert "bridge_decomposition" in result
        assert "methodology_notes" in result

    def test_has_acquired_data_flag(self):
        deals = [_make_deal(id=1, acquired_revenue=20)]
        result = compute_organic_growth_analysis(deals)
        assert result["meta"]["has_acquired_data"] is True

    def test_no_acquired_data_flag(self):
        deals = [_make_deal(id=1)]
        result = compute_organic_growth_analysis(deals)
        assert result["meta"]["has_acquired_data"] is False

    def test_summary_cards_portfolio_totals(self):
        deals = [
            _make_deal(id=1, entry_revenue=50, exit_revenue=100, acquired_revenue=20,
                       entry_ebitda=10, exit_ebitda=25, acquired_ebitda=5),
            _make_deal(id=2, entry_revenue=30, exit_revenue=60, acquired_revenue=10,
                       entry_ebitda=8, exit_ebitda=18, acquired_ebitda=3),
        ]
        result = compute_organic_growth_analysis(deals)
        cards = result["summary_cards"]
        assert cards["deals_with_acquisitions"] == 2
        assert cards["deals_total"] == 2
        # Portfolio totals: organic = (30 + 20), acquired = (20 + 10)
        assert cards["portfolio_organic_revenue_growth"] == 50
        assert cards["portfolio_acquired_revenue_growth"] == 30

    def test_deals_without_acquired_show_all_organic(self):
        deals = [_make_deal(id=1, entry_revenue=50, exit_revenue=100)]
        result = compute_organic_growth_analysis(deals)
        row = result["deal_rows"][0]
        assert row["organic_revenue_growth"] == 50
        assert row["acquired_revenue_contribution"] == 0

    def test_chart_data_only_includes_acquired_deals(self):
        deals = [
            _make_deal(id=1, acquired_revenue=20),  # Has acquired
            _make_deal(id=2),  # No acquired
        ]
        result = compute_organic_growth_analysis(deals)
        chart = result["charts"]["organic_vs_acquired_revenue"]
        # Only the deal with acquired data should be in charts
        assert len(chart["labels"]) == 1

    def test_deal_rows_sorted_acquired_first(self):
        deals = [
            _make_deal(id=1),  # No acquired
            _make_deal(id=2, acquired_revenue=20),  # Has acquired
        ]
        result = compute_organic_growth_analysis(deals)
        rows = result["deal_rows"]
        assert rows[0]["acquired_data_status"] == "acquired_data_provided"
        assert rows[1]["acquired_data_status"] == "data_not_provided"


# ---------------------------------------------------------------------------
# Bridge decomposition
# ---------------------------------------------------------------------------

class TestBridgeDecomposition:
    def test_bridge_split_reconciles(self):
        """Organic + acquired bridge contributions should equal total revenue driver."""
        deal = _make_deal(
            id=1,
            equity_invested=100,
            entry_revenue=50, exit_revenue=100,
            entry_ebitda=10, exit_ebitda=25,
            entry_enterprise_value=200, exit_enterprise_value=400,
            entry_net_debt=100, exit_net_debt=50,
            acquired_revenue=20, acquired_ebitda=5, acquired_tev=80,
            realized_value=200, unrealized_value=0,
        )
        result = compute_organic_growth_analysis([deal])
        bridge_rows = result["bridge_decomposition"]
        if bridge_rows:
            bd = bridge_rows[0]
            total = bd["organic_revenue_contribution"] + bd["acquired_revenue_contribution"]
            assert abs(total - bd["total_revenue_driver"]) < 0.01, (
                f"Bridge doesn't reconcile: organic={bd['organic_revenue_contribution']}, "
                f"acquired={bd['acquired_revenue_contribution']}, total={bd['total_revenue_driver']}"
            )

    def test_bridge_only_for_ebitda_additive(self):
        """Bridge decomposition should only appear for deals with ebitda_additive method."""
        # Deal with negative EBITDA -> uses revenue_multiple_fallback, not ebitda_additive
        deal = _make_deal(
            id=1,
            entry_ebitda=-5, exit_ebitda=-2,
            acquired_revenue=20, acquired_ebitda=5,
        )
        result = compute_organic_growth_analysis([deal])
        # Bridge decomposition should be empty for non-ebitda_additive deals
        assert len(result["bridge_decomposition"]) == 0

    def test_no_bridge_without_acquired_data(self):
        deal = _make_deal(id=1)
        result = compute_organic_growth_analysis([deal])
        assert len(result["bridge_decomposition"]) == 0

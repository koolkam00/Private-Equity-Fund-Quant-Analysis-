"""Tests for organic vs acquired growth attribution analysis.

Covers:
- Deal-level organic derivation (revenue and EBITDA)
- Organic CAGR computation
- Cohort classification (pure organic, augmented, dependent)
- Growth quality score computation
- Acquisition efficiency
- Organic margin expansion
- Analysis payload structure
- Bridge decomposition reconciliation
- Fund-level aggregation
- Scatter and waterfall data
- Edge cases: negative growth, zero growth, missing data
"""

from datetime import date
from types import SimpleNamespace

from services.metrics.deal import compute_deal_metrics
from services.metrics.organic_growth import (
    compute_organic_growth_analysis,
    COHORT_PURE_ORGANIC,
    COHORT_AUGMENTED,
    COHORT_DEPENDENT,
)
from services.metrics.common import percentile_rank


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
# Deal-level organic derivation (unchanged from deal.py)
# ---------------------------------------------------------------------------

class TestOrganicDerivation:
    def test_organic_revenue_with_acquired(self):
        deal = _make_deal(entry_revenue=50, exit_revenue=100, acquired_revenue=20)
        m = compute_deal_metrics(deal)
        assert m["total_revenue_growth"] == 50
        assert m["acquired_revenue_contribution"] == 20
        assert m["organic_revenue_growth"] == 30
        assert abs(m["organic_revenue_pct"] - 0.6) < 1e-9

    def test_organic_ebitda_with_acquired(self):
        deal = _make_deal(entry_ebitda=10, exit_ebitda=25, acquired_ebitda=5)
        m = compute_deal_metrics(deal)
        assert m["total_ebitda_growth"] == 15
        assert m["organic_ebitda_growth"] == 10

    def test_no_acquired_data_all_organic(self):
        deal = _make_deal(entry_revenue=50, exit_revenue=100)
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_growth"] == 50
        assert m["acquired_data_status"] == "data_not_provided"

    def test_zero_acquired_treated_as_no_acquisitions(self):
        deal = _make_deal(acquired_revenue=0, acquired_ebitda=0, acquired_tev=0)
        m = compute_deal_metrics(deal)
        assert m["acquired_data_status"] == "data_not_provided"

    def test_negative_organic_when_acquired_exceeds_growth(self):
        deal = _make_deal(entry_revenue=50, exit_revenue=80, acquired_revenue=40)
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_growth"] == -10

    def test_missing_entry_exit_returns_none(self):
        deal = _make_deal(entry_revenue=None, exit_revenue=100, acquired_revenue=20)
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_growth"] is None

    def test_acquired_data_status_provided(self):
        deal = _make_deal(acquired_revenue=20)
        m = compute_deal_metrics(deal)
        assert m["acquired_data_status"] == "acquired_data_provided"


# ---------------------------------------------------------------------------
# Organic CAGR
# ---------------------------------------------------------------------------

class TestOrganicCagr:
    def test_organic_revenue_cagr(self):
        deal = _make_deal(entry_revenue=50, exit_revenue=100, acquired_revenue=20,
                          investment_date=date(2020, 1, 1), exit_date=date(2023, 1, 1))
        m = compute_deal_metrics(deal)
        expected = (((80 / 50) ** (1 / 3) - 1) * 100)
        assert abs(m["organic_revenue_cagr"] - expected) < 0.01

    def test_no_acquired_cagr_equals_total_cagr(self):
        deal = _make_deal(entry_revenue=50, exit_revenue=100)
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_cagr"] == m["revenue_cagr"]

    def test_cagr_with_no_hold_period_returns_none(self):
        deal = _make_deal(investment_date=None, acquired_revenue=20)
        m = compute_deal_metrics(deal)
        assert m["organic_revenue_cagr"] is None


# ---------------------------------------------------------------------------
# Analysis payload structure
# ---------------------------------------------------------------------------

class TestPayloadStructure:
    def test_payload_keys(self):
        deals = [_make_deal(id=1, acquired_revenue=20)]
        result = compute_organic_growth_analysis(deals)
        assert "meta" in result
        assert "deal_rows" in result
        assert "cohorts" in result
        assert "total" in result
        assert "fund_comparison" in result
        assert "scatter_data" in result
        assert "waterfall" in result
        assert "charts" in result
        assert "bridge_decomposition" in result
        assert "methodology_notes" in result

    def test_all_deals_included(self):
        """ALL deals should appear in deal_rows, not just acquisition deals."""
        deals = [
            _make_deal(id=1, acquired_revenue=20),  # Has acquired
            _make_deal(id=2),  # Pure organic
        ]
        result = compute_organic_growth_analysis(deals)
        assert len(result["deal_rows"]) == 2
        assert result["meta"]["deals_total"] == 2
        assert result["meta"]["deals_with_acquisitions"] == 1
        assert result["meta"]["deals_pure_organic"] == 1

    def test_deal_rows_sorted_by_moic(self):
        deals = [
            _make_deal(id=1, equity_invested=100, realized_value=150, unrealized_value=0),
            _make_deal(id=2, equity_invested=100, realized_value=300, unrealized_value=0),
        ]
        result = compute_organic_growth_analysis(deals)
        rows = result["deal_rows"]
        assert rows[0]["moic"] >= rows[1]["moic"]

    def test_empty_portfolio(self):
        result = compute_organic_growth_analysis([])
        assert result["meta"]["has_deals"] is False
        assert result["total"] is None


# ---------------------------------------------------------------------------
# Cohort classification
# ---------------------------------------------------------------------------

class TestCohortClassification:
    def test_pure_organic(self):
        """Deal without acquired data -> Pure Organic."""
        deals = [_make_deal(id=1)]
        result = compute_organic_growth_analysis(deals)
        assert result["deal_rows"][0]["cohort"] == COHORT_PURE_ORGANIC

    def test_augmented(self):
        """Deal with acquired < 50% of growth -> Augmented."""
        deals = [_make_deal(id=1, entry_revenue=50, exit_revenue=100, acquired_revenue=20)]
        result = compute_organic_growth_analysis(deals)
        row = result["deal_rows"][0]
        assert row["cohort"] == COHORT_AUGMENTED  # organic=30/50=60% > 50%

    def test_dependent(self):
        """Deal with acquired >= 50% of growth -> Dependent."""
        deals = [_make_deal(id=1, entry_revenue=50, exit_revenue=100, acquired_revenue=30)]
        result = compute_organic_growth_analysis(deals)
        row = result["deal_rows"][0]
        assert row["cohort"] == COHORT_DEPENDENT  # organic=20/50=40% < 50%

    def test_exactly_50_50_is_dependent(self):
        """Exactly 50/50 split -> Dependent (>= rule)."""
        deals = [_make_deal(id=1, entry_revenue=50, exit_revenue=100, acquired_revenue=25)]
        result = compute_organic_growth_analysis(deals)
        row = result["deal_rows"][0]
        assert row["cohort"] == COHORT_DEPENDENT  # organic=25/50=50%, not > 50%

    def test_declining_with_acquisitions_is_dependent(self):
        """Negative growth with acquisitions -> Dependent."""
        deals = [_make_deal(id=1, entry_revenue=100, exit_revenue=90, acquired_revenue=5)]
        result = compute_organic_growth_analysis(deals)
        assert result["deal_rows"][0]["cohort"] == COHORT_DEPENDENT

    def test_cohort_aggregates(self):
        """Cohort aggregation should produce metrics per cohort."""
        deals = [
            _make_deal(id=1, entry_revenue=50, exit_revenue=100),  # Pure organic
            _make_deal(id=2, entry_revenue=50, exit_revenue=100, acquired_revenue=10),  # Augmented
            _make_deal(id=3, entry_revenue=50, exit_revenue=100, acquired_revenue=30),  # Dependent
        ]
        result = compute_organic_growth_analysis(deals)
        assert result["cohorts"][COHORT_PURE_ORGANIC] is not None
        assert result["cohorts"][COHORT_PURE_ORGANIC]["deal_count"] == 1
        assert result["cohorts"][COHORT_AUGMENTED] is not None
        assert result["cohorts"][COHORT_AUGMENTED]["deal_count"] == 1
        assert result["cohorts"][COHORT_DEPENDENT] is not None
        assert result["cohorts"][COHORT_DEPENDENT]["deal_count"] == 1


# ---------------------------------------------------------------------------
# Growth quality score
# ---------------------------------------------------------------------------

class TestGrowthQualityScore:
    def test_scores_computed(self):
        deals = [_make_deal(id=1), _make_deal(id=2, acquired_revenue=20)]
        result = compute_organic_growth_analysis(deals)
        for row in result["deal_rows"]:
            assert "growth_quality_score" in row
            assert 0 <= row["growth_quality_score"] <= 100

    def test_single_deal(self):
        """Single deal should get a valid score (not crash)."""
        deals = [_make_deal(id=1)]
        result = compute_organic_growth_analysis(deals)
        score = result["deal_rows"][0]["growth_quality_score"]
        assert score is not None
        assert 0 <= score <= 100

    def test_pure_organic_gets_full_independence_points(self):
        """Pure organic deal gets 25 pts for acquisition independence."""
        deals = [_make_deal(id=1), _make_deal(id=2, acquired_revenue=20)]
        result = compute_organic_growth_analysis(deals)
        organic_row = next(r for r in result["deal_rows"] if r["cohort"] == COHORT_PURE_ORGANIC)
        # Pure organic gets c3 = 25.0 (full independence)
        assert organic_row["growth_quality_score"] >= 25.0


# ---------------------------------------------------------------------------
# Acquisition efficiency
# ---------------------------------------------------------------------------

class TestAcquisitionEfficiency:
    def test_efficiency_computed(self):
        """Deal with acquired_tev should get an efficiency score."""
        deals = [_make_deal(id=1, acquired_revenue=20, acquired_ebitda=5, acquired_tev=80)]
        result = compute_organic_growth_analysis(deals)
        row = result["deal_rows"][0]
        assert row["acquisition_efficiency"] is not None

    def test_pure_organic_no_efficiency(self):
        """Pure organic deal has no acquisition efficiency."""
        deals = [_make_deal(id=1)]
        result = compute_organic_growth_analysis(deals)
        assert result["deal_rows"][0]["acquisition_efficiency"] is None

    def test_organic_value_negative_returns_none(self):
        """When acquired_tev > total_value, efficiency is undefined."""
        deals = [_make_deal(id=1, equity_invested=100, realized_value=100, unrealized_value=0,
                            acquired_revenue=20, acquired_ebitda=5, acquired_tev=200)]
        result = compute_organic_growth_analysis(deals)
        assert result["deal_rows"][0]["acquisition_efficiency"] is None


# ---------------------------------------------------------------------------
# Organic margin expansion
# ---------------------------------------------------------------------------

class TestOrganicMargin:
    def test_margin_computed(self):
        deals = [_make_deal(id=1, entry_revenue=50, entry_ebitda=10,
                            exit_revenue=100, exit_ebitda=25)]
        result = compute_organic_growth_analysis(deals)
        row = result["deal_rows"][0]
        assert row["entry_ebitda_margin"] is not None
        assert abs(row["entry_ebitda_margin"] - 0.2) < 1e-6  # 10/50

    def test_organic_exit_margin_strips_acquired(self):
        """Organic exit margin should exclude acquired revenue/EBITDA."""
        deals = [_make_deal(id=1, entry_revenue=50, entry_ebitda=10,
                            exit_revenue=100, exit_ebitda=25,
                            acquired_revenue=20, acquired_ebitda=5)]
        result = compute_organic_growth_analysis(deals)
        row = result["deal_rows"][0]
        # Organic exit: (25-5)/(100-20) = 20/80 = 0.25
        assert row["organic_exit_ebitda_margin"] is not None
        assert abs(row["organic_exit_ebitda_margin"] - 0.25) < 1e-6

    def test_margin_div_zero(self):
        """When exit_revenue == acquired_revenue, organic exit margin is None."""
        deals = [_make_deal(id=1, exit_revenue=100, acquired_revenue=100, acquired_ebitda=25)]
        result = compute_organic_growth_analysis(deals)
        assert result["deal_rows"][0]["organic_exit_ebitda_margin"] is None


# ---------------------------------------------------------------------------
# Fund-level aggregation
# ---------------------------------------------------------------------------

class TestFundAggregation:
    def test_fund_comparison(self):
        deals = [
            _make_deal(id=1, fund_number="Fund II"),
            _make_deal(id=2, fund_number="Fund I"),
            _make_deal(id=3, fund_number="Fund II"),
        ]
        result = compute_organic_growth_analysis(deals)
        funds = result["fund_comparison"]
        assert len(funds) == 2
        # Should be sorted: Fund I first, Fund II second
        assert funds[0]["fund"] == "Fund I"
        assert funds[1]["fund"] == "Fund II"
        assert funds[1]["deal_count"] == 2


# ---------------------------------------------------------------------------
# Scatter and waterfall data
# ---------------------------------------------------------------------------

class TestChartData:
    def test_scatter_data(self):
        deals = [_make_deal(id=1), _make_deal(id=2, acquired_revenue=20)]
        result = compute_organic_growth_analysis(deals)
        points = result["scatter_data"]
        assert len(points) == 2
        assert "moic" in points[0]
        assert "cohort" in points[0]
        assert "equity" in points[0]

    def test_waterfall_data(self):
        deals = [_make_deal(id=1, entry_revenue=50, exit_revenue=100)]
        result = compute_organic_growth_analysis(deals)
        wf = result["waterfall"]
        assert "revenue" in wf
        assert "ebitda" in wf
        assert wf["revenue"]["entry"] == 50
        assert wf["revenue"]["exit"] == 100
        assert wf["revenue"]["deals"] == 1


# ---------------------------------------------------------------------------
# Bridge decomposition (kept from original)
# ---------------------------------------------------------------------------

class TestBridgeDecomposition:
    def test_bridge_split_reconciles(self):
        deal = _make_deal(
            id=1, equity_invested=100,
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
            assert abs(total - bd["total_revenue_driver"]) < 0.01

    def test_pure_organic_bridge_has_zero_acquired(self):
        """Pure organic deal may still get a bridge row, but acquired contribution = 0."""
        deal = _make_deal(id=1)
        result = compute_organic_growth_analysis([deal])
        for bd in result["bridge_decomposition"]:
            if bd["deal_id"] == 1:
                assert abs(bd["acquired_revenue_contribution"]) < 0.01


# ---------------------------------------------------------------------------
# Percentile rank helper
# ---------------------------------------------------------------------------

class TestPercentileRank:
    def test_basic(self):
        assert percentile_rank(3, [1, 2, 3, 4, 5]) == 0.6  # 3 values <= 3

    def test_none_value(self):
        assert percentile_rank(None, [1, 2, 3]) == 0.0

    def test_empty_values(self):
        assert percentile_rank(5, []) == 0.0

    def test_all_none_values(self):
        assert percentile_rank(5, [None, None]) == 0.0

    def test_single_value(self):
        assert percentile_rank(5, [5]) == 1.0

    def test_ties(self):
        # 4 values <= 3 out of 5
        assert percentile_rank(3, [1, 3, 3, 3, 5]) == 0.8

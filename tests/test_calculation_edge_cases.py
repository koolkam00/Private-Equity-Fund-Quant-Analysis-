"""Edge case and validation tests for PE fund calculation logic.

Tests cover:
- NaN/Inf/None input propagation
- Zero and negative denominators
- Extreme hold periods
- Negative EBITDA CAGR methodology
- Bridge reconciliation and fallback modes
- Portfolio aggregation edge cases
- Loss ratio boundary conditions
- Status normalization consistency
- Quarter end date construction (Feb 30 bug regression)
- Implied IRR bounds
"""

import math
from datetime import date
from types import SimpleNamespace

from services.metrics.common import (
    EPS,
    deal_hold_years,
    effective_exit_date,
    resolve_analysis_as_of_date,
    safe_divide,
    safe_log,
    safe_power,
)
from services.metrics.deal import (
    _cagr_pct,
    _growth_pct,
    _implied_irr,
    compute_deal_metrics,
)
from services.metrics.bridge import compute_additive_bridge
from services.metrics.portfolio import (
    _avg,
    _metric_aggregate,
    _normalize_track_status,
    _wavg,
    compute_portfolio_analytics,
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
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# safe_divide edge cases
# ---------------------------------------------------------------------------

class TestSafeDivide:
    def test_nan_numerator(self):
        assert safe_divide(float("nan"), 1.0) is None

    def test_nan_denominator(self):
        assert safe_divide(1.0, float("nan")) is None

    def test_inf_numerator(self):
        assert safe_divide(float("inf"), 1.0) is None

    def test_inf_denominator(self):
        assert safe_divide(1.0, float("inf")) is None

    def test_negative_inf(self):
        assert safe_divide(float("-inf"), 1.0) is None

    def test_zero_denominator(self):
        assert safe_divide(100, 0) is None

    def test_none_numerator(self):
        assert safe_divide(None, 5) is None

    def test_none_denominator(self):
        assert safe_divide(5, None) is None

    def test_normal_division(self):
        assert safe_divide(10, 2) == 5.0

    def test_custom_default(self):
        assert safe_divide(10, 0, default=0.0) == 0.0

    def test_negative_division(self):
        assert safe_divide(-10, 2) == -5.0

    def test_very_small_denominator(self):
        result = safe_divide(1e10, 1e-300)
        # Should return a large number or None if overflow
        assert result is None or isinstance(result, float)


class TestSafePower:
    def test_nan_base(self):
        assert safe_power(float("nan"), 2) is None

    def test_nan_exponent(self):
        assert safe_power(2, float("nan")) is None

    def test_inf_base(self):
        assert safe_power(float("inf"), 2) is None

    def test_negative_base_fractional_exp(self):
        # Should return None (complex number territory)
        assert safe_power(-4, 0.5) is None

    def test_zero_base_negative_exp(self):
        assert safe_power(0, -1) is None

    def test_normal_power(self):
        assert abs(safe_power(2, 3) - 8.0) < 1e-9


class TestSafeLog:
    def test_nan_input(self):
        assert safe_log(float("nan")) is None

    def test_inf_input(self):
        assert safe_log(float("inf")) is None

    def test_zero_input(self):
        assert safe_log(0) is None

    def test_negative_input(self):
        assert safe_log(-5) is None

    def test_normal_log(self):
        assert abs(safe_log(math.e) - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# IRR edge cases
# ---------------------------------------------------------------------------

class TestImpliedIrr:
    def test_zero_hold_period(self):
        assert _implied_irr(2.0, 0) is None

    def test_negative_hold_period(self):
        assert _implied_irr(2.0, -1.0) is None

    def test_very_short_hold_returns_none(self):
        """Hold periods under 30 days should return None to avoid extreme IRRs."""
        assert _implied_irr(2.0, 10 / 365.25) is None

    def test_normal_irr(self):
        # 2x in 3 years => 2^(1/3) - 1 ≈ 26%
        irr = _implied_irr(2.0, 3.0)
        assert irr is not None
        assert abs(irr - (2 ** (1 / 3) - 1)) < 1e-9

    def test_moic_below_one(self):
        """Loss scenario: 0.5x in 2 years => negative IRR."""
        irr = _implied_irr(0.5, 2.0)
        assert irr is not None
        assert irr < 0

    def test_none_moic(self):
        assert _implied_irr(None, 3.0) is None

    def test_zero_moic(self):
        assert _implied_irr(0, 3.0) is None

    def test_extreme_irr_capped(self):
        """Extremely high implied IRR (>10000%) should return None."""
        # 1000x in 0.1 years => astronomical IRR
        assert _implied_irr(1000, 0.1) is None


# ---------------------------------------------------------------------------
# Growth % edge cases
# ---------------------------------------------------------------------------

class TestGrowthPct:
    def test_zero_entry(self):
        assert _growth_pct(100, 0) is None

    def test_none_entry(self):
        assert _growth_pct(100, None) is None

    def test_none_exit(self):
        assert _growth_pct(None, 100) is None

    def test_negative_entry_improvement(self):
        """Negative to less-negative should be positive growth."""
        result = _growth_pct(-10, -100)
        assert result is not None
        assert result > 0  # Improving toward zero

    def test_negative_entry_worsening(self):
        """Negative to more-negative should be negative growth."""
        result = _growth_pct(-200, -100)
        assert result is not None
        assert result < 0

    def test_normal_growth(self):
        result = _growth_pct(150, 100)
        assert result is not None
        assert abs(result - 50.0) < 1e-9


# ---------------------------------------------------------------------------
# CAGR edge cases
# ---------------------------------------------------------------------------

class TestCagrPct:
    def test_zero_entry(self):
        assert _cagr_pct(100, 0, 3.0) is None

    def test_zero_exit(self):
        assert _cagr_pct(0, 100, 3.0) is None

    def test_sign_flip_positive_to_negative(self):
        assert _cagr_pct(-50, 100, 3.0) is None

    def test_sign_flip_negative_to_positive(self):
        assert _cagr_pct(50, -100, 3.0) is None

    def test_negative_to_negative_improving(self):
        """EBITDA from -50 to -10 over 3 years: loss magnitude decreasing."""
        result = _cagr_pct(-10, -50, 3.0)
        assert result is not None
        assert result > 0  # Improving → positive CAGR

    def test_negative_to_negative_worsening(self):
        """EBITDA from -10 to -50 over 3 years: loss magnitude increasing."""
        result = _cagr_pct(-50, -10, 3.0)
        assert result is not None
        assert result < 0  # Worsening → negative CAGR

    def test_zero_hold_period(self):
        assert _cagr_pct(200, 100, 0) is None

    def test_negative_hold_period(self):
        assert _cagr_pct(200, 100, -1.0) is None

    def test_normal_cagr(self):
        # 100 to 200 in 3 years => (200/100)^(1/3) - 1
        result = _cagr_pct(200, 100, 3.0)
        expected = ((200 / 100) ** (1 / 3) - 1) * 100
        assert result is not None
        assert abs(result - expected) < 1e-6


# ---------------------------------------------------------------------------
# Hold period edge cases
# ---------------------------------------------------------------------------

class TestHoldPeriod:
    def test_none_investment_date(self):
        deal = _make_deal(investment_date=None)
        assert deal_hold_years(deal) is None

    def test_exit_before_investment(self):
        deal = _make_deal(
            investment_date=date(2023, 1, 1),
            exit_date=date(2020, 1, 1),
        )
        assert deal_hold_years(deal) is None

    def test_same_day(self):
        deal = _make_deal(
            investment_date=date(2023, 1, 1),
            exit_date=date(2023, 1, 1),
        )
        assert deal_hold_years(deal) is None

    def test_normal_period(self):
        deal = _make_deal(
            investment_date=date(2020, 1, 1),
            exit_date=date(2023, 1, 1),
        )
        result = deal_hold_years(deal)
        assert result is not None
        assert abs(result - 3.0) < 0.01

    def test_effective_exit_date_fallback(self):
        deal = _make_deal(exit_date=None)
        edate = effective_exit_date(deal, as_of_date=date(2024, 6, 15))
        assert edate == date(2024, 6, 15)

    def test_effective_exit_date_uses_exit(self):
        deal = _make_deal(exit_date=date(2023, 6, 1))
        edate = effective_exit_date(deal, as_of_date=date(2024, 6, 15))
        assert edate == date(2023, 6, 1)


# ---------------------------------------------------------------------------
# Deal metrics validation
# ---------------------------------------------------------------------------

class TestComputeDealMetrics:
    def test_negative_equity_warning(self):
        deal = _make_deal(equity_invested=-50)
        m = compute_deal_metrics(deal)
        assert any("Negative equity" in w for w in m["_warnings"])

    def test_negative_realized_warning(self):
        deal = _make_deal(realized_value=-10)
        m = compute_deal_metrics(deal)
        assert any("Negative realized" in w for w in m["_warnings"])

    def test_negative_unrealized_warning(self):
        deal = _make_deal(unrealized_value=-10)
        m = compute_deal_metrics(deal)
        assert any("Negative unrealized" in w for w in m["_warnings"])

    def test_moic_plausibility_warning(self):
        deal = _make_deal(equity_invested=1, realized_value=200, unrealized_value=0)
        m = compute_deal_metrics(deal)
        assert any("implausible" in w for w in m["_warnings"])

    def test_long_hold_period_warning(self):
        deal = _make_deal(
            investment_date=date(2000, 1, 1),
            exit_date=date(2025, 1, 1),
        )
        m = compute_deal_metrics(deal)
        assert any("unusually long" in w for w in m["_warnings"])

    def test_zero_equity_moic_is_none(self):
        deal = _make_deal(equity_invested=0)
        m = compute_deal_metrics(deal)
        assert m["moic"] is None

    def test_normal_deal_no_warnings(self):
        deal = _make_deal()
        m = compute_deal_metrics(deal)
        # Standard deal should have no critical warnings
        critical = [w for w in m["_warnings"] if "Negative" in w or "implausible" in w]
        assert len(critical) == 0

    def test_margin_percentage_scaling(self):
        """EBITDA margins should be expressed as percentages (×100)."""
        deal = _make_deal(entry_ebitda=20, entry_revenue=100, exit_ebitda=30, exit_revenue=100)
        m = compute_deal_metrics(deal)
        assert m["entry_ebitda_margin"] == 20.0  # 20/100 * 100
        assert m["exit_ebitda_margin"] == 30.0


# ---------------------------------------------------------------------------
# Bridge edge cases
# ---------------------------------------------------------------------------

class TestBridge:
    def test_bridge_with_zero_equity(self):
        deal = _make_deal(equity_invested=0)
        warnings = []
        bridge = compute_additive_bridge(deal, warnings)
        assert not bridge["ready"]

    def test_bridge_missing_entry_ev(self):
        deal = _make_deal(entry_enterprise_value=None)
        warnings = []
        bridge = compute_additive_bridge(deal, warnings)
        assert not bridge["ready"]
        assert any("Insufficient" in w for w in warnings)

    def test_bridge_negative_ebitda_fallback(self):
        """When EBITDA is negative, should fall back to revenue-multiple method."""
        deal = _make_deal(entry_ebitda=-5, exit_ebitda=-2)
        warnings = []
        bridge = compute_additive_bridge(deal, warnings)
        if bridge["ready"]:
            assert bridge["calculation_method"] == "revenue_multiple_fallback"
            assert bridge["fallback_reason"] == "negative_ebitda"

    def test_bridge_missing_revenue_fallback(self):
        """When revenue is missing, should fall back to EBITDA-multiple method."""
        deal = _make_deal(entry_revenue=None, exit_revenue=None)
        warnings = []
        bridge = compute_additive_bridge(deal, warnings)
        if bridge["ready"]:
            assert bridge["calculation_method"] == "ebitda_multiple_fallback"
            assert bridge["fallback_reason"] == "missing_revenue"

    def test_bridge_reconciliation(self):
        """Bridge drivers + other should sum to value created."""
        deal = _make_deal()
        warnings = []
        bridge = compute_additive_bridge(deal, warnings, basis="fund")
        if bridge["ready"]:
            drivers = bridge["drivers_dollar"]
            total_from_drivers = sum(v for v in drivers.values() if v is not None)
            value_created = bridge["value_created"]
            if value_created is not None:
                assert abs(total_from_drivers - value_created) < 0.01, (
                    f"Bridge doesn't reconcile: drivers={total_from_drivers}, "
                    f"value_created={value_created}"
                )

    def test_bridge_partial_revenue_rejected(self):
        """Having only entry OR exit revenue should prevent bridge."""
        deal = _make_deal(entry_revenue=50, exit_revenue=None)
        warnings = []
        bridge = compute_additive_bridge(deal, warnings)
        assert not bridge["ready"]
        assert any("Partial revenue" in w for w in warnings)

    def test_bridge_negative_tev_ebitda_rejected(self):
        """Negative TEV/EBITDA multiples should prevent EBITDA additive bridge."""
        deal = _make_deal(
            entry_enterprise_value=-100,
            entry_ebitda=10,
        )
        warnings = []
        bridge = compute_additive_bridge(deal, warnings)
        # Should either not be ready or use a fallback
        if bridge["ready"]:
            assert bridge["calculation_method"] != "ebitda_additive"


# ---------------------------------------------------------------------------
# Portfolio aggregation edge cases
# ---------------------------------------------------------------------------

class TestPortfolioAggregation:
    def test_avg_empty(self):
        assert _avg([]) is None

    def test_avg_single(self):
        assert _avg([5.0]) == 5.0

    def test_wavg_empty(self):
        assert _wavg([]) is None

    def test_wavg_zero_weights(self):
        assert _wavg([(5.0, 0), (10.0, 0)]) is None

    def test_wavg_negative_weights(self):
        """Negative weights should return None (equity shouldn't be negative)."""
        assert _wavg([(5.0, -10)]) is None

    def test_wavg_normal(self):
        result = _wavg([(2.0, 100), (3.0, 200)])
        expected = (2.0 * 100 + 3.0 * 200) / 300
        assert abs(result - expected) < 1e-9

    def test_metric_aggregate_all_none(self):
        metrics = [{"moic": None, "equity": 100}, {"moic": None, "equity": 200}]
        result = _metric_aggregate(metrics, "moic")
        assert result["avg"] is None
        assert result["wavg"] is None

    def test_metric_aggregate_with_zero_equity(self):
        """Deals with zero equity should still contribute to simple avg."""
        metrics = [
            {"moic": 2.0, "equity": 0},
            {"moic": 3.0, "equity": 0},
        ]
        result = _metric_aggregate(metrics, "moic")
        assert result["avg"] == 2.5
        # wavg falls back to avg when no positive-equity pairs exist
        assert result["wavg"] == 2.5

    def test_single_deal_portfolio(self):
        deal = _make_deal()
        m = compute_deal_metrics(deal)
        metrics_by_id = {deal.id: m}
        port = compute_portfolio_analytics([deal], metrics_by_id)
        assert port["total_equity"] == 100
        assert port["total_value"] == 200

    def test_portfolio_all_losses(self):
        """Portfolio where all deals are losses should still compute cleanly."""
        deals = [
            _make_deal(id=1, equity_invested=100, realized_value=30, unrealized_value=0),
            _make_deal(id=2, equity_invested=200, realized_value=50, unrealized_value=0),
        ]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        port = compute_portfolio_analytics(deals, metrics)
        assert port["total_equity"] == 300
        assert port["total_value"] == 80
        moic = port["returns"]["gross_moic"]["wavg"]
        assert moic is not None
        assert moic < 1.0


# ---------------------------------------------------------------------------
# Status normalization consistency
# ---------------------------------------------------------------------------

class TestStatusNormalization:
    def test_fully_realized(self):
        assert _normalize_track_status("Fully Realized") == "Fully Realized"

    def test_realized_plain(self):
        assert _normalize_track_status("Realized") == "Fully Realized"

    def test_partially_realized(self):
        assert _normalize_track_status("Partially Realized") == "Partially Realized"

    def test_unrealized(self):
        assert _normalize_track_status("Unrealized") == "Unrealized"

    def test_empty_string(self):
        assert _normalize_track_status("") == "Unrealized"

    def test_none(self):
        assert _normalize_track_status(None) == "Unrealized"

    def test_whitespace(self):
        assert _normalize_track_status("  Fully Realized  ") == "Fully Realized"

    def test_case_insensitive(self):
        assert _normalize_track_status("FULLY REALIZED") == "Fully Realized"

    def test_unknown_status(self):
        assert _normalize_track_status("Written Off") == "Other"


# ---------------------------------------------------------------------------
# Quarter end date regression test
# ---------------------------------------------------------------------------

class TestQuarterEndDate:
    def test_q1_march_31(self):
        from services.metrics.lp import _quarter_end_from_date
        result = _quarter_end_from_date(date(2024, 2, 15))
        assert result == date(2024, 3, 31)

    def test_q2_june_30(self):
        from services.metrics.lp import _quarter_end_from_date
        result = _quarter_end_from_date(date(2024, 5, 20))
        assert result == date(2024, 6, 30)

    def test_q3_september_30(self):
        from services.metrics.lp import _quarter_end_from_date
        result = _quarter_end_from_date(date(2024, 8, 1))
        assert result == date(2024, 9, 30)

    def test_q4_december_31(self):
        from services.metrics.lp import _quarter_end_from_date
        result = _quarter_end_from_date(date(2024, 11, 30))
        assert result == date(2024, 12, 31)

    def test_none_input(self):
        from services.metrics.lp import _quarter_end_from_date
        assert _quarter_end_from_date(None) is None

    def test_all_months_produce_valid_dates(self):
        """Regression: ensure no month produces an invalid date (e.g., Feb 30)."""
        from services.metrics.lp import _quarter_end_from_date
        for month in range(1, 13):
            result = _quarter_end_from_date(date(2024, month, 15))
            assert result is not None
            assert isinstance(result.day, int)
            assert result.day in (30, 31)


# ---------------------------------------------------------------------------
# Resolve analysis as-of date
# ---------------------------------------------------------------------------

class TestResolveAsOfDate:
    def test_empty_deals(self):
        result = resolve_analysis_as_of_date([])
        assert result == date.today()

    def test_prefers_explicit_as_of(self):
        deal = _make_deal(as_of_date=date(2024, 3, 31))
        result = resolve_analysis_as_of_date([deal])
        assert result == date(2024, 3, 31)

    def test_falls_back_to_exit_date(self):
        deal = _make_deal(as_of_date=None, exit_date=date(2023, 12, 31))
        result = resolve_analysis_as_of_date([deal])
        assert result == date(2023, 12, 31)


# ---------------------------------------------------------------------------
# Executive summary realized status helper
# ---------------------------------------------------------------------------

class TestRealizedStatusHelper:
    def test_fully_realized(self):
        from services.metrics.executive_summary import _is_realized_status
        assert _is_realized_status("Fully Realized") is True

    def test_realized(self):
        from services.metrics.executive_summary import _is_realized_status
        assert _is_realized_status("Realized") is True

    def test_unrealized(self):
        from services.metrics.executive_summary import _is_realized_status
        assert _is_realized_status("Unrealized") is False

    def test_none(self):
        from services.metrics.executive_summary import _is_realized_status
        assert _is_realized_status(None) is False

    def test_full_realization(self):
        from services.metrics.executive_summary import _is_realized_status
        assert _is_realized_status("Full Realization") is True

    def test_case_insensitive(self):
        from services.metrics.executive_summary import _is_realized_status
        assert _is_realized_status("FULLY REALIZED") is True

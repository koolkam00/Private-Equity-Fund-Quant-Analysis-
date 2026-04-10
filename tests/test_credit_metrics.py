"""Tests for private credit metrics engine."""

import pytest
from datetime import date
from unittest.mock import MagicMock

from types import SimpleNamespace

from services.metrics.credit import (
    credit_data_cuts_available_dimension_keys,
    compute_credit_benchmarking_analysis,
    compute_credit_data_cuts,
    compute_credit_fundamentals,
    compute_credit_loan_metrics,
    compute_credit_migration_matrix,
    compute_credit_portfolio_analytics,
    compute_credit_pricing_trends,
    compute_credit_risk_metrics,
    compute_credit_underwrite_outcome,
    compute_credit_track_record,
    compute_credit_watchlist,
    compute_credit_yield_attribution,
    compute_credit_stress_scenarios,
    compute_credit_concentration,
    compute_credit_vintage_comparison,
    compute_credit_maturity_profile,
    compute_snapshot_coverage,
    compute_traffic_lights,
    compute_top_concerns,
    WATCHLIST_ATTENTION_THRESHOLD,
    WATCHLIST_MONITOR_THRESHOLD,
    WATCHLIST_SCORE_CAP,
    WATCHLIST_URGENT_THRESHOLD,
    WATCHLIST_WEIGHT_COVENANT,
    WATCHLIST_WEIGHT_EBITDA_TREND,
    WATCHLIST_WEIGHT_LTV_TREND,
    WATCHLIST_WEIGHT_SPONSOR_HISTORY,
    WATCHLIST_WEIGHT_STATUS_AUTO,
    _build_sponsor_history,
    _latest_snapshot_value,
    _loan_traffic_light,
    _parse_loan_term,
    _score_loan,
    _snapshot_trend,
)


def _make_loan(**kwargs):
    """Create a mock CreditLoan with sensible defaults."""
    defaults = {
        "id": 1,
        "company_name": "Acme Corp",
        "fund_name": "PCOF I",
        "vintage_year": 2022,
        "close_date": date(2022, 6, 1),
        "exit_date": None,
        "status": "Unrealized",
        "as_of_date": date(2024, 6, 1),
        "instrument": "Term Loan B",
        "tranche": "First Lien",
        "security_type": "Senior Secured",
        "issue_size": 100.0,
        "hold_size": 25.0,
        "coupon_rate": 0.08,
        "spread_bps": 400,
        "floor_rate": 0.01,
        "fee_oid": 0.02,
        "fee_upfront": 0.5,
        "fee_exit": None,
        "maturity_date": date(2027, 6, 1),
        "fixed_or_floating": "Floating",
        "reference_rate": "SOFR",
        "pik_toggle": False,
        "pik_rate": None,
        "call_protection_months": 12,
        "make_whole_premium": None,
        "amortization_type": "Bullet",
        "payment_frequency": "Quarterly",
        "entry_ltv": 0.55,
        "current_ltv": 0.60,
        "entry_revenue": 50.0,
        "entry_ebitda": 15.0,
        "current_revenue": 55.0,
        "current_ebitda": 17.0,
        "interest_coverage_ratio": 2.5,
        "dscr": 1.8,
        "internal_credit_rating": 2,
        "default_status": "Performing",
        "covenant_type": "Maintenance",
        "covenant_compliant": True,
        "gross_irr": 0.10,
        "moic": 1.15,
        "realized_value": None,
        "unrealized_value": 26.0,
        "cumulative_interest_income": 4.0,
        "cumulative_fee_income": 0.5,
        "fair_value": 25.5,
        "yield_to_maturity": 0.09,
        "recovery_rate": None,
        "original_par": 25.0,
        "current_outstanding": 25.0,
        "accrued_interest": 0.2,
        "sector": "Software",
        "geography": "North America",
        "sponsor": "Apollo",
        "currency": "USD",
        "fx_rate_to_usd": 1.0,
        # --- NEW LP fields ---
        "investment_count": None,
        "business_description": None,
        "is_public": None,
        "sourcing_channel": None,
        "location": None,
        "committed_amount": None,
        "entry_loan_amount": None,
        "current_invested_capital": None,
        "realized_proceeds": None,
        "unrealized_loan_value": None,
        "unrealized_warrant_equity_value": None,
        "total_value": None,
        "estimated_irr_at_entry": None,
        "cash_margin": None,
        "pik_margin": None,
        "closing_fee": None,
        "prepayment_protection": None,
        "loan_term": None,
        "term_years": None,
        "equity_investment": None,
        "warrants_at_entry": None,
        "warrant_strike_entry": None,
        "warrants_current": None,
        "warrant_strike_current": None,
        "warrant_term": None,
        "ttm_revenue_entry": None,
        "ttm_revenue_current": None,
        "entry_coverage_ratio": None,
        "current_coverage_ratio": None,
        "entry_equity_cushion": None,
        "current_equity_cushion": None,
    }
    defaults.update(kwargs)
    loan = MagicMock()
    for k, v in defaults.items():
        setattr(loan, k, v)
    return loan


def _make_lp_loan(**kwargs):
    """Create a mock CreditLoan with new LP fields populated (real credit manager data)."""
    defaults = {
        "id": 100,
        "company_name": "TechCo Inc",
        "fund_name": "Credit Fund I",
        "vintage_year": None,
        "close_date": date(2022, 3, 15),
        "exit_date": None,
        "status": "Unrealized",
        "as_of_date": None,
        "instrument": None,
        "tranche": None,
        "security_type": "Senior Secured",
        "issue_size": None,
        "hold_size": None,
        "coupon_rate": None,
        "spread_bps": None,
        "floor_rate": 0.01,
        "fee_oid": None,
        "fee_upfront": None,
        "fee_exit": None,
        "maturity_date": None,
        "fixed_or_floating": None,
        "reference_rate": None,
        "pik_toggle": False,
        "pik_rate": None,
        "call_protection_months": None,
        "make_whole_premium": None,
        "amortization_type": None,
        "payment_frequency": None,
        "entry_ltv": None,
        "current_ltv": None,
        "entry_revenue": None,
        "entry_ebitda": None,
        "current_revenue": None,
        "current_ebitda": None,
        "interest_coverage_ratio": None,
        "dscr": None,
        "internal_credit_rating": None,
        "default_status": "Performing",
        "covenant_type": None,
        "covenant_compliant": None,
        "gross_irr": 0.12,
        "moic": 1.20,
        "realized_value": None,
        "unrealized_value": None,
        "cumulative_interest_income": None,
        "cumulative_fee_income": None,
        "fair_value": None,
        "yield_to_maturity": None,
        "recovery_rate": None,
        "original_par": None,
        "current_outstanding": None,
        "accrued_interest": None,
        "sector": "Technology",
        "geography": None,
        "sponsor": None,
        "currency": "USD",
        "fx_rate_to_usd": 1.0,
        # LP-specific fields
        "investment_count": 1,
        "business_description": "Enterprise SaaS platform",
        "is_public": False,
        "sourcing_channel": "Direct",
        "location": "North America",
        "committed_amount": 30.0,
        "entry_loan_amount": 25.0,
        "current_invested_capital": 24.5,
        "realized_proceeds": None,
        "unrealized_loan_value": 26.0,
        "unrealized_warrant_equity_value": 1.5,
        "total_value": 27.5,
        "estimated_irr_at_entry": 0.15,
        "cash_margin": 0.085,
        "pik_margin": 0.02,
        "closing_fee": 0.02,
        "prepayment_protection": "12-month no-call",
        "loan_term": "5 years",
        "equity_investment": 0.5,
        "warrants_at_entry": 50000,
        "warrant_strike_entry": 10.0,
        "warrants_current": 50000,
        "warrant_strike_current": 12.5,
        "warrant_term": "10 years",
        "ttm_revenue_entry": 50.0,
        "ttm_revenue_current": 55.0,
        "entry_coverage_ratio": 1.6,
        "current_coverage_ratio": 1.8,
        "entry_equity_cushion": 0.45,
        "current_equity_cushion": 0.40,
    }
    defaults.update(kwargs)
    loan = MagicMock()
    for k, v in defaults.items():
        setattr(loan, k, v)
    return loan


# ---------------------------------------------------------------------------
# Per-loan metrics
# ---------------------------------------------------------------------------


class TestCreditLoanMetrics:
    def test_loan_metrics_normal(self):
        loan = _make_loan()
        m = compute_credit_loan_metrics(loan)
        assert m["income_return"] is not None
        assert m["price_return"] is not None
        assert m["total_return"] is not None
        assert m["hold_months"] is not None
        assert m["hold_months"] > 0
        # cost_basis = 25.0 * (1 - 0.02) = 24.5
        assert abs(m["cost_basis"] - 24.5) < 0.01
        assert m["ltv_delta"] == pytest.approx(0.05, abs=0.001)
        assert len(m["_warnings"]) == 0

    def test_loan_metrics_zero_hold(self):
        loan = _make_loan(hold_size=0)
        m = compute_credit_loan_metrics(loan)
        assert m["income_return"] is None
        assert m["cost_basis"] is None
        assert "hold_size is zero" in m["_warnings"][0]

    def test_loan_metrics_pik_only(self):
        loan = _make_loan(
            coupon_rate=0,
            cumulative_interest_income=0,
            cumulative_fee_income=0,
            pik_toggle=True,
            pik_rate=0.06,
        )
        m = compute_credit_loan_metrics(loan)
        assert m["pik_accrual"] > 0
        assert m["income_return"] is not None
        assert m["income_return"] > 0

    def test_loan_metrics_null_ltv(self):
        loan = _make_loan(entry_ltv=None, current_ltv=None)
        m = compute_credit_loan_metrics(loan)
        assert m["ltv_delta"] is None
        # Should not crash
        assert m["income_return"] is not None

    def test_loan_metrics_lp_fields(self):
        """New LP data: total_return_mtm, warrant_upside, revenue_growth, deployment_pct."""
        loan = _make_lp_loan()
        m = compute_credit_loan_metrics(loan)
        assert m["total_return_mtm"] is not None
        assert m["total_return_mtm"] == pytest.approx(27.5 / 24.5 - 1.0, abs=0.01)
        assert m["warrant_upside"] is not None
        assert m["warrant_upside"] == pytest.approx(1.5 / 24.5, abs=0.01)
        assert m["revenue_growth"] is not None
        assert m["revenue_growth"] == pytest.approx(55.0 / 50.0 - 1.0, abs=0.01)
        assert m["deployment_pct"] is not None
        assert m["deployment_pct"] == pytest.approx(24.5 / 30.0, abs=0.01)

    def test_loan_metrics_all_null_optional(self):
        """Sparse data: only company, fund, status. Everything else None."""
        loan = _make_loan(
            hold_size=None, coupon_rate=None, spread_bps=None, floor_rate=None,
            fee_oid=None, fee_upfront=None, fair_value=None, entry_ltv=None,
            current_ltv=None, interest_coverage_ratio=None, dscr=None,
            internal_credit_rating=None, cumulative_interest_income=None,
            cumulative_fee_income=None, close_date=None, maturity_date=None,
            gross_irr=None, moic=None,
        )
        m = compute_credit_loan_metrics(loan)
        assert m["income_return"] is None
        assert m["cost_basis"] is None
        assert m["total_return_mtm"] is None
        # Should NOT crash


# ---------------------------------------------------------------------------
# Traffic lights
# ---------------------------------------------------------------------------


class TestTrafficLights:
    def test_traffic_light_null_ltv(self):
        # No LTV/coverage AND no LP signals -> gray (truly insufficient data).
        # _make_loan defaults moic=1.15 and gross_irr=0.10, so both must be
        # explicitly nulled to exercise the gray fallback.
        loan = _make_loan(
            current_ltv=None,
            interest_coverage_ratio=None,
            default_status="Performing",
            moic=None,
            gross_irr=None,
            total_value=None,
            entry_loan_amount=None,
            hold_size=None,
        )
        signal = _loan_traffic_light(loan)
        assert signal == "gray"

    def test_traffic_light_all_performing(self):
        loans = [_make_loan(id=i) for i in range(5)]
        result = compute_traffic_lights(loans)
        assert result["portfolio_signal"] == "green"
        assert result["pcts"]["green"] > 0.9

    def test_traffic_light_red_default(self):
        loan = _make_loan(default_status="Default")
        assert _loan_traffic_light(loan) == "red"

    def test_traffic_light_yellow_watch(self):
        loan = _make_loan(default_status="Watch List", current_ltv=0.70, interest_coverage_ratio=2.0)
        assert _loan_traffic_light(loan) == "yellow"

    def test_traffic_light_red_high_ltv(self):
        loan = _make_loan(current_ltv=0.95, default_status="Performing")
        assert _loan_traffic_light(loan) == "red"

    def test_traffic_light_red_underwater_moic(self):
        """LP data: MOIC < 0.8 -> red."""
        loan = _make_lp_loan(moic=0.7, total_value=17.5, entry_loan_amount=25.0)
        assert _loan_traffic_light(loan) == "red"

    def test_traffic_light_green_good_irr(self):
        """LP data: positive IRR -> green (when no LTV/coverage data)."""
        loan = _make_lp_loan(gross_irr=0.12, moic=1.2)
        signal = _loan_traffic_light(loan)
        assert signal == "green"

    def test_traffic_light_yellow_underperforming_moic(self):
        """LP data: MOIC 0.8-1.0 -> yellow."""
        loan = _make_lp_loan(moic=0.95, total_value=23.75, gross_irr=None)
        signal = _loan_traffic_light(loan)
        assert signal == "yellow"


# ---------------------------------------------------------------------------
# Top concerns
# ---------------------------------------------------------------------------


class TestTopConcerns:
    def test_top_concerns_severity_order(self):
        loans = [
            _make_loan(id=1, company_name="Default Co", default_status="Default"),
            _make_loan(id=2, company_name="Covenant Co", covenant_compliant=False, default_status="Performing"),
            _make_loan(id=3, company_name="Watch LTV Co", default_status="Watch List", current_ltv=0.85),
        ]
        concerns = compute_top_concerns(loans)
        assert len(concerns) >= 2
        assert concerns[0]["severity"] <= concerns[-1]["severity"]
        assert concerns[0]["company"] == "Default Co"

    def test_top_concerns_empty_portfolio(self):
        concerns = compute_top_concerns([])
        assert concerns == []

    def test_top_concerns_underwater(self):
        """LP data: total_value < entry_loan_amount triggers underwater concern."""
        loan = _make_lp_loan(id=10, total_value=20.0, entry_loan_amount=25.0, moic=0.8)
        concerns = compute_top_concerns([loan])
        underwater = [c for c in concerns if c["reason"] == "Underwater"]
        assert len(underwater) >= 1

    def test_top_concerns_declining_revenue(self):
        """LP data: declining revenue triggers concern."""
        loan = _make_lp_loan(id=11, ttm_revenue_entry=100.0, ttm_revenue_current=70.0)
        concerns = compute_top_concerns([loan])
        rev_concerns = [c for c in concerns if "Revenue" in c["reason"]]
        assert len(rev_concerns) >= 1


# ---------------------------------------------------------------------------
# Portfolio analytics
# ---------------------------------------------------------------------------


class TestPortfolioAnalytics:
    def test_portfolio_analytics(self):
        loans = [_make_loan(id=i, hold_size=10 + i) for i in range(5)]
        result = compute_credit_portfolio_analytics(loans)
        assert result["loan_count"] == 5
        assert result["total_deployed"] > 0
        assert result["wavg_yield"] is not None
        assert result["pct_performing"] > 0
        assert "traffic_lights" in result
        assert "top_concerns" in result

    def test_portfolio_fx_aggregation(self):
        usd_loan = _make_loan(id=1, hold_size=10, currency="USD", fx_rate_to_usd=1.0)
        eur_loan = _make_loan(id=2, hold_size=10, currency="EUR", fx_rate_to_usd=1.1)
        result = compute_credit_portfolio_analytics([usd_loan, eur_loan])
        # EUR loan should be converted: 10 * 1.1 = 11
        assert result["total_deployed"] == pytest.approx(21.0, abs=0.1)

    def test_portfolio_all_default(self):
        loans = [_make_loan(id=i, default_status="Default") for i in range(3)]
        result = compute_credit_portfolio_analytics(loans)
        assert result["pct_performing"] == 0.0
        assert result["traffic_lights"]["portfolio_signal"] == "red"

    def test_portfolio_analytics_lp_fields(self):
        """LP data: aggregation weights off current invested capital."""
        loans = [
            _make_lp_loan(
                id=100,
                committed_amount=30.0,
                entry_loan_amount=25.0,
                current_invested_capital=24.5,
                gross_irr=0.12,
                moic=1.2,
            ),
            _make_lp_loan(
                id=101,
                committed_amount=20.0,
                entry_loan_amount=15.0,
                current_invested_capital=14.5,
                gross_irr=0.10,
                moic=1.1,
            ),
        ]
        result = compute_credit_portfolio_analytics(loans)
        assert result["has_new_fields"] is True
        assert result["total_committed"] == pytest.approx(50.0, abs=0.1)
        assert result["total_entry_loan"] == pytest.approx(39.0, abs=0.1)
        assert result["total_current_invested"] == pytest.approx(39.0, abs=0.1)
        assert result["weighted_avg_irr"] is not None
        assert result["weighted_avg_moic"] is not None
        assert result["total_warrant_value"] is not None

    def test_portfolio_analytics_empty(self):
        """Empty portfolio should not crash."""
        result = compute_credit_portfolio_analytics([])
        assert result["loan_count"] == 0
        assert result["total_deployed"] == 0.0


# ---------------------------------------------------------------------------
# Yield attribution
# ---------------------------------------------------------------------------


class TestYieldAttribution:
    def test_yield_attribution_decomposition(self):
        loan = _make_loan()
        result = compute_credit_yield_attribution([loan])
        assert result["coupon_income"] == 4.0
        assert result["fee_income"] == 0.5
        total = result["coupon_income"] + result["fee_income"] + result["pik_accrual"] + result["price_appreciation"]
        assert result["total_return_dollars"] == pytest.approx(total, abs=0.01)
        assert "by_fund" in result
        assert "PCOF I" in result["by_fund"]

    def test_floor_coverage_no_floating(self):
        """Portfolio with only fixed-rate loans -> floor coverage block all zero.

        We never want to display "0% floor coverage" as a problem when there's
        no floating book to cover. The template gates the section on
        floating_loans_count > 0.
        """
        fixed_loan = _make_loan(id=1, fixed_or_floating="Fixed", floor_rate=None)
        result = compute_credit_yield_attribution([fixed_loan])
        assert result["floating_loans_count"] == 0
        assert result["floating_loans_with_floor_count"] == 0
        assert result["floor_coverage_pct"] is None
        assert result["wavg_floor_rate"] is None
        assert result["floating_hold_total"] == 0.0
        assert result["floating_hold_with_floor"] == 0.0

    def test_floor_coverage_weighted_avg(self):
        """Two floating loans with different sizes and floors -> hold-weighted floor.

        SmallLoan: hold $10M, floor 1.0%
        BigLoan:   hold $90M, floor 2.0%
        Wtd avg floor: (0.01 * 10 + 0.02 * 90) / 100 = 0.019 = 1.9%

        A third loan is fixed (no floor) and must NOT show up in any of the
        floating-side counts.
        """
        small = _make_loan(
            id=1, company_name="SmallLoan", fixed_or_floating="Floating",
            hold_size=10.0, floor_rate=0.01,
        )
        big = _make_loan(
            id=2, company_name="BigLoan", fixed_or_floating="Floating",
            hold_size=90.0, floor_rate=0.02,
        )
        fixed = _make_loan(
            id=3, company_name="FixedLoan", fixed_or_floating="Fixed",
            hold_size=50.0, floor_rate=None,
        )
        result = compute_credit_yield_attribution([small, big, fixed])

        assert result["floating_loans_count"] == 2
        assert result["floating_loans_with_floor_count"] == 2
        assert result["floor_coverage_pct"] == pytest.approx(1.0, abs=0.001)
        # Hold-weighted: (0.01 * 10 + 0.02 * 90) / 100 = 0.019
        assert result["wavg_floor_rate"] == pytest.approx(0.019, abs=0.0001)
        assert result["floating_hold_total"] == pytest.approx(100.0, abs=0.01)
        assert result["floating_hold_with_floor"] == pytest.approx(100.0, abs=0.01)


# ---------------------------------------------------------------------------
# Stress scenarios
# ---------------------------------------------------------------------------


class TestStressScenarios:
    def test_stress_rate_shock_floating(self):
        loan = _make_loan(fixed_or_floating="Floating")
        scenario = {"default_rate_shock": 0, "recovery_rate_shock": 0.40, "rate_shock_bps": 200}
        result = compute_credit_stress_scenarios([loan], scenario)
        assert result["base_nav"] > 0
        # +200bps with 1.5% per 100bps = 3% haircut on floating NAV
        assert result["stressed_nav"] < result["base_nav"]
        expected_haircut = result["base_nav"] * (200 / 100.0) * 0.015
        assert (result["base_nav"] - result["stressed_nav"]) == pytest.approx(expected_haircut, abs=0.01)

    def test_stress_fixed_unaffected(self):
        loan = _make_loan(fixed_or_floating="Fixed")
        scenario = {"default_rate_shock": 0, "recovery_rate_shock": 0.40, "rate_shock_bps": 200}
        result = compute_credit_stress_scenarios([loan], scenario)
        assert result["base_nav"] == result["stressed_nav"]

    def test_stress_default_by_rating(self):
        good = _make_loan(id=1, internal_credit_rating=1, hold_size=10, fair_value=10)
        bad = _make_loan(id=2, internal_credit_rating=5, hold_size=10, fair_value=10)
        scenario = {"default_rate_shock": 0.50, "recovery_rate_shock": 0.40, "rate_shock_bps": 0}
        result = compute_credit_stress_scenarios([good, bad], scenario)
        # Worst-rated loan should default first
        assert result["defaults_triggered"] >= 1
        impacted = [l["company"] for l in result["impacted_loans"]]
        assert bad.company_name in impacted

    def test_stress_moic_fallback_sort(self):
        """LP data: no credit rating, sort by MOIC (lowest first)."""
        good = _make_lp_loan(id=100, moic=1.5, entry_loan_amount=10, total_value=15)
        bad = _make_lp_loan(id=101, moic=0.8, entry_loan_amount=10, total_value=8)
        scenario = {"default_rate_shock": 0.50, "recovery_rate_shock": 0.40, "rate_shock_bps": 0}
        result = compute_credit_stress_scenarios([good, bad], scenario)
        assert result["defaults_triggered"] >= 1
        impacted = [l["company"] for l in result["impacted_loans"]]
        assert bad.company_name in impacted


# ---------------------------------------------------------------------------
# Risk metrics (Bug 6 — credit-risk page aggregates)
# ---------------------------------------------------------------------------


class TestCreditRiskMetrics:
    """compute_credit_risk_metrics returns the existing template fields PLUS
    ICR / DSCR / covenant / recovery aggregates that the parser ingests but
    no other metric used to surface."""

    def test_risk_metrics_basic_aggregates(self):
        loans = [_make_loan(id=i, hold_size=10 + i) for i in range(5)]
        result = compute_credit_risk_metrics(loans)
        # Existing fields the template still consumes
        assert result["pct_performing"] == 1.0
        assert result["wavg_ltv"] is not None
        assert "Performing" in result["status_distribution"]
        # Bug 6 additions
        assert result["wavg_icr"] is not None
        assert result["wavg_dscr"] is not None
        assert result["loans_with_icr_count"] == 5
        assert result["loans_with_dscr_count"] == 5

    def test_risk_metrics_icr_hold_weighted(self):
        """ICR weight follows entry_amt > 0 else hold_size convention."""
        small = _make_loan(id=1, hold_size=10, interest_coverage_ratio=1.0)
        big = _make_loan(id=2, hold_size=90, interest_coverage_ratio=3.0)
        result = compute_credit_risk_metrics([small, big])
        # Hold-weighted: (1.0*10 + 3.0*90) / 100 = 2.8
        assert result["wavg_icr"] == pytest.approx(2.8, abs=0.01)

    def test_risk_metrics_dscr_hold_weighted(self):
        small = _make_loan(id=1, hold_size=20, dscr=1.0)
        big = _make_loan(id=2, hold_size=80, dscr=2.0)
        result = compute_credit_risk_metrics([small, big])
        # (1.0*20 + 2.0*80) / 100 = 1.8
        assert result["wavg_dscr"] == pytest.approx(1.8, abs=0.01)

    def test_risk_metrics_covenant_breach_count(self):
        loans = [
            _make_loan(id=1, covenant_compliant=True, hold_size=10),
            _make_loan(id=2, covenant_compliant=False, hold_size=20),
            _make_loan(id=3, covenant_compliant=False, hold_size=30),
            _make_loan(id=4, covenant_compliant=None, hold_size=40),
        ]
        result = compute_credit_risk_metrics(loans)
        assert result["covenant_breach_count"] == 2
        # 3 loans have covenant data (None excluded)
        assert result["loans_with_covenant_data"] == 3
        # Breach hold = 20 + 30 = 50; total hold = 100; pct = 0.5
        assert result["covenant_breach_pct"] == pytest.approx(0.5, abs=0.01)

    def test_risk_metrics_recovery_only_defaulted(self):
        """recovery_rate aggregate considers ONLY default/restructured loans."""
        performing = _make_loan(id=1, default_status="Performing", recovery_rate=0.9, hold_size=50)
        defaulted = _make_loan(id=2, default_status="Default", recovery_rate=0.4, hold_size=50)
        restructured = _make_loan(id=3, default_status="Restructured", recovery_rate=0.6, hold_size=50)
        result = compute_credit_risk_metrics([performing, defaulted, restructured])
        # Performing 0.9 must be excluded; only 0.4 and 0.6 averaged equally weighted by hold
        assert result["wavg_recovery_rate"] == pytest.approx(0.5, abs=0.01)

    def test_risk_metrics_no_icr_data_returns_none(self):
        loans = [_make_loan(id=i, interest_coverage_ratio=None, dscr=None) for i in range(3)]
        result = compute_credit_risk_metrics(loans)
        assert result["wavg_icr"] is None
        assert result["wavg_dscr"] is None
        assert result["loans_with_icr_count"] == 0
        assert result["loans_with_dscr_count"] == 0

    def test_risk_metrics_no_covenant_data_returns_none_pct(self):
        loans = [_make_loan(id=i, covenant_compliant=None, hold_size=10) for i in range(3)]
        result = compute_credit_risk_metrics(loans)
        assert result["covenant_breach_count"] == 0
        assert result["loans_with_covenant_data"] == 0

    def test_risk_metrics_lp_fields_use_current_invested_weight(self):
        """Current invested capital should drive risk-page weighting."""
        loans = [
            _make_lp_loan(
                id=100,
                entry_loan_amount=10.0,
                current_invested_capital=50.0,
                interest_coverage_ratio=1.0,
            ),
            _make_lp_loan(
                id=101,
                entry_loan_amount=90.0,
                current_invested_capital=50.0,
                interest_coverage_ratio=3.0,
            ),
        ]
        result = compute_credit_risk_metrics(loans)
        # Equal weights: (1.0 + 3.0) / 2 = 2.0
        assert result["wavg_icr"] == pytest.approx(2.0, abs=0.01)
        assert result["has_new_fields"] is True
        assert result["total_entry_loan"] == pytest.approx(100.0, abs=0.1)

    def test_risk_metrics_status_distribution(self):
        loans = [
            _make_loan(id=1, default_status="Performing"),
            _make_loan(id=2, default_status="Performing"),
            _make_loan(id=3, default_status="Watch List"),
            _make_loan(id=4, default_status="Default"),
        ]
        result = compute_credit_risk_metrics(loans)
        assert result["status_distribution"]["Performing"] == 2
        assert result["status_distribution"]["Watch List"] == 1
        assert result["status_distribution"]["Default"] == 1
        assert result["pct_performing"] == 0.5

    def test_risk_metrics_empty_portfolio(self):
        result = compute_credit_risk_metrics([])
        assert result["pct_performing"] == 0.0
        assert result["wavg_ltv"] is None
        assert result["wavg_icr"] is None
        assert result["covenant_breach_count"] == 0


# ---------------------------------------------------------------------------
# Concentration
# ---------------------------------------------------------------------------


class TestConcentration:
    def test_concentration_hhi(self):
        loans = [
            _make_loan(id=1, sector="Software", total_value=50.0),
            _make_loan(id=2, sector="Healthcare", total_value=30.0),
            _make_loan(id=3, sector="Software", total_value=20.0),
        ]
        result = compute_credit_concentration(loans)
        assert result["hhi_sector"] > 0
        assert result["by_sector"][0]["name"] == "Software"
        assert result["total_unrealized_value"] == pytest.approx(100.0)
        assert result["total_value"] == pytest.approx(100.0)
        assert len(result["top_5"]) == 3
        assert len(result["top_10"]) == 3

    def test_concentration_lp_fields(self):
        """LP data: location, sourcing, public breakdowns."""
        loans = [
            _make_lp_loan(id=100, location="North America", sourcing_channel="Direct", is_public=False),
            _make_lp_loan(id=101, location="Europe", sourcing_channel="Broker", is_public=True),
        ]
        result = compute_credit_concentration(loans)
        assert result["has_sourcing_data"] is True
        assert result["has_public_data"] is True
        assert len(result["by_sourcing"]) == 2
        assert len(result["by_public"]) == 2

    def test_concentration_uses_total_unrealized_value(self):
        """LP data: concentration should size the book off unrealized exposure."""
        loans = [
            _make_lp_loan(
                id=100,
                hold_size=None,
                entry_loan_amount=100.0,
                current_invested_capital=25.0,
                total_value=40.0,
                realized_proceeds=15.0,
                unrealized_loan_value=20.0,
                unrealized_warrant_equity_value=5.0,
            ),
            _make_lp_loan(
                id=101,
                hold_size=None,
                entry_loan_amount=5.0,
                current_invested_capital=15.0,
                total_value=10.0,
                realized_proceeds=0.0,
                unrealized_loan_value=10.0,
                unrealized_warrant_equity_value=0.0,
            ),
        ]
        result = compute_credit_concentration(loans)
        assert result["total_unrealized_value"] == pytest.approx(35.0, abs=0.1)
        assert result["total_invested"] == pytest.approx(40.0, abs=0.1)
        assert result["total_realized_value"] == pytest.approx(15.0, abs=0.1)
        assert result["total_unrealized_loan_value"] == pytest.approx(30.0, abs=0.1)
        assert result["total_unrealized_equity_value"] == pytest.approx(5.0, abs=0.1)
        assert result["total_value"] == pytest.approx(50.0, abs=0.1)
        assert result["total_hold"] == pytest.approx(35.0, abs=0.1)
        assert result["top_10"][0]["value"] == pytest.approx(25.0, abs=0.1)
        assert result["top_10"][0]["pct"] == pytest.approx(25.0 / 35.0, abs=0.001)

    def test_concentration_detail_uses_track_record_sort_and_rollups(self):
        loans = [
            _make_lp_loan(
                id=100,
                fund_name="Fund A",
                company_name="Unrealized Co",
                status="Unrealized",
                close_date=date(2024, 6, 1),
                current_invested_capital=40.0,
                total_value=44.0,
                unrealized_loan_value=44.0,
                realized_proceeds=0.0,
            ),
            _make_lp_loan(
                id=101,
                fund_name="Fund A",
                company_name="Realized Co",
                status="Realized",
                close_date=date(2024, 1, 1),
                current_invested_capital=60.0,
                total_value=72.0,
                realized_proceeds=72.0,
                unrealized_loan_value=0.0,
            ),
        ]

        result = compute_credit_concentration(loans)

        assert result["loan_count"] == 2
        assert result["fund_count"] == 1
        fund = result["detail_funds"][0]
        assert fund["fund_name"] == "Fund A"
        assert fund["rows"][0]["company_name"] == "Realized Co"
        assert fund["rows"][1]["company_name"] == "Unrealized Co"
        assert fund["status_rollups"][0]["status"] == "Fully Realized"
        assert fund["summary_rollups"][-1]["totals"]["total_value"] == pytest.approx(116.0, abs=0.001)
        assert fund["summary_rollups"][-1]["totals"]["pct_portfolio_value"] == pytest.approx(1.0, abs=0.001)
        assert result["detail_overall"]["summary_rollups"][-1]["totals"]["total_value"] == pytest.approx(116.0, abs=0.001)


# ---------------------------------------------------------------------------
# Vintage comparison
# ---------------------------------------------------------------------------


class TestVintageComparison:
    def test_vintage_comparison(self):
        loans = [
            _make_loan(id=1, fund_name="PCOF I", vintage_year=2020),
            _make_loan(id=2, fund_name="PCOF I", vintage_year=2020),
            _make_loan(id=3, fund_name="PCOF II", vintage_year=2022),
        ]
        result = compute_credit_vintage_comparison(loans)
        assert len(result["funds"]) == 2
        fund_names = [f["fund_name"] for f in result["funds"]]
        assert "PCOF I" in fund_names
        assert "PCOF II" in fund_names


# ---------------------------------------------------------------------------
# Maturity profile
# ---------------------------------------------------------------------------


class TestMaturityProfile:
    def test_maturity_profile(self):
        loans = [
            _make_loan(id=1, maturity_date=date(2026, 6, 1), hold_size=10),
            _make_loan(id=2, maturity_date=date(2027, 6, 1), hold_size=20),
            _make_loan(id=3, maturity_date=None, hold_size=5),
        ]
        result = compute_credit_maturity_profile(loans)
        assert result["weighted_average_life"] is not None
        assert len(result["maturity_wall"]) == 2
        assert result["no_maturity_date"] == 1

    def test_maturity_loan_term_fallback(self):
        """LP data: derive maturity from loan_term when maturity_date is None."""
        loan = _make_lp_loan(
            id=100,
            maturity_date=None,
            close_date=date(2022, 3, 15),
            loan_term="5 years",
            entry_loan_amount=25.0,
        )
        result = compute_credit_maturity_profile([loan])
        assert result["weighted_average_life"] is not None
        assert len(result["maturity_wall"]) == 1
        assert result["no_maturity_date"] == 0


# ---------------------------------------------------------------------------
# Loan term parsing
# ---------------------------------------------------------------------------


class TestLoanTermParsing:
    def test_parse_5_years(self):
        result = _parse_loan_term("5 years", date(2022, 1, 1))
        assert result is not None
        assert result.year == 2027

    def test_parse_60_months(self):
        result = _parse_loan_term("60 months", date(2022, 1, 1))
        assert result is not None
        assert result.year == 2027

    def test_parse_5y_shorthand(self):
        result = _parse_loan_term("5Y", date(2022, 1, 1))
        assert result is not None
        assert result.year == 2027

    def test_parse_range(self):
        result = _parse_loan_term("3-5 years", date(2022, 1, 1))
        assert result is not None
        # Midpoint = 4 years
        assert result.year == 2026

    def test_parse_18m(self):
        result = _parse_loan_term("18M", date(2022, 1, 1))
        assert result is not None
        # 18 months from Jan 2022 = roughly July 2023
        assert result.year == 2023

    def test_parse_none(self):
        assert _parse_loan_term(None, date(2022, 1, 1)) is None
        assert _parse_loan_term("5 years", None) is None
        assert _parse_loan_term("", date(2022, 1, 1)) is None

    def test_parse_tbd(self):
        """Unparseable strings return None."""
        assert _parse_loan_term("TBD", date(2022, 1, 1)) is None
        assert _parse_loan_term("N/A", date(2022, 1, 1)) is None


# ---------------------------------------------------------------------------
# Snapshot coverage helper
# ---------------------------------------------------------------------------


def _make_snapshot(loan_id=1, snapshot_date=None, **kwargs):
    """Build a fake CreditLoanSnapshot for coverage tests.

    Uses SimpleNamespace instead of MagicMock so missing attributes raise
    AttributeError (a real bug surface) instead of silently returning a Mock
    that compares truthy.
    """
    defaults = {
        "credit_loan_id": loan_id,
        "snapshot_date": snapshot_date or date(2024, 6, 1),
        "current_ltv": None,
        "fair_value": None,
        "internal_credit_rating": None,
        "current_ebitda": None,
        "current_revenue": None,
        "interest_coverage_ratio": None,
        "dscr": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestSnapshotCoverage:
    def test_coverage_helper_no_snapshots(self):
        """Empty snapshots_by_loan -> 0% coverage, total reflects loan count."""
        loans = [_make_loan(id=1), _make_loan(id=2), _make_loan(id=3)]
        result = compute_snapshot_coverage(loans, snapshots_by_loan={})

        assert result["coverage_pct"] == 0.0
        assert result["loans_covered"] == 0
        assert result["loans_total"] == 3
        assert result["latest_snapshot_date"] is None
        assert result["per_field_coverage"] == {}

    def test_coverage_helper_full_coverage(self):
        """Every loan has at least one snapshot -> 100% coverage, latest date is correct."""
        loans = [_make_loan(id=1), _make_loan(id=2)]
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1)),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 6, 1)),
            ],
            2: [_make_snapshot(loan_id=2, snapshot_date=date(2024, 3, 1))],
        }
        result = compute_snapshot_coverage(loans, snapshots_by_loan)

        assert result["coverage_pct"] == 1.0
        assert result["loans_covered"] == 2
        assert result["loans_total"] == 2
        assert result["latest_snapshot_date"] == date(2024, 6, 1)
        assert result["per_field_coverage"] == {}

    def test_coverage_helper_required_field(self):
        """required_field excludes loans whose snapshots are all null for that field.

        Loan 1: has internal_credit_rating in one snapshot -> covered
        Loan 2: has snapshots but ALL null for internal_credit_rating -> NOT covered
        Loan 3: no snapshots at all -> NOT covered
        Expected coverage_pct = 1/3.
        """
        loans = [_make_loan(id=1), _make_loan(id=2), _make_loan(id=3)]
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), internal_credit_rating=None),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 6, 1), internal_credit_rating=3),
            ],
            2: [
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 3, 1), internal_credit_rating=None),
            ],
        }
        result = compute_snapshot_coverage(
            loans, snapshots_by_loan, required_field="internal_credit_rating"
        )

        assert result["coverage_pct"] == pytest.approx(1 / 3, abs=0.001)
        assert result["loans_covered"] == 1
        assert result["loans_total"] == 3
        assert result["latest_snapshot_date"] == date(2024, 6, 1)
        assert result["per_field_coverage"] == {
            "internal_credit_rating": pytest.approx(1 / 3, abs=0.001)
        }

    def test_coverage_helper_per_field_only_when_requested(self):
        """per_field_coverage stays empty when required_field is None, populated otherwise."""
        loans = [_make_loan(id=1)]
        snapshots_by_loan = {
            1: [_make_snapshot(loan_id=1, snapshot_date=date(2024, 6, 1), current_ltv=0.65)],
        }

        # No required_field -> per_field_coverage is empty
        no_field = compute_snapshot_coverage(loans, snapshots_by_loan)
        assert no_field["per_field_coverage"] == {}
        assert no_field["coverage_pct"] == 1.0

        # With required_field -> per_field_coverage has exactly one entry
        with_field = compute_snapshot_coverage(
            loans, snapshots_by_loan, required_field="current_ltv"
        )
        assert with_field["per_field_coverage"] == {"current_ltv": 1.0}
        assert with_field["coverage_pct"] == 1.0

        # Empty loans list -> all zeros, per_field still {}
        empty = compute_snapshot_coverage([], {}, required_field="current_ltv")
        assert empty["coverage_pct"] == 0.0
        assert empty["loans_total"] == 0
        assert empty["per_field_coverage"] == {}


class TestSnapshotHelpers:
    def test_latest_snapshot_value_returns_most_recent_non_null(self):
        """Walks snapshots in reverse, returning the first non-null hit.

        Setup: 3 snapshots, latest is null for current_ltv, middle has 0.65,
        earliest has 0.55. Expected: returns 0.65 (the most recent NON-null).
        """
        snapshots_by_loan = {
            42: [
                _make_snapshot(loan_id=42, snapshot_date=date(2024, 1, 1), current_ltv=0.55),
                _make_snapshot(loan_id=42, snapshot_date=date(2024, 4, 1), current_ltv=0.65),
                _make_snapshot(loan_id=42, snapshot_date=date(2024, 7, 1), current_ltv=None),
            ],
        }
        assert _latest_snapshot_value(snapshots_by_loan, 42, "current_ltv") == 0.65

        # Loan with no snapshots -> None
        assert _latest_snapshot_value(snapshots_by_loan, 999, "current_ltv") is None

        # Field that's null in every snapshot -> None
        assert _latest_snapshot_value(snapshots_by_loan, 42, "internal_credit_rating") is None

        # Empty / None safety
        assert _latest_snapshot_value(None, 42, "current_ltv") is None
        assert _latest_snapshot_value({}, 42, "current_ltv") is None

    def test_snapshot_trend_returns_none_with_insufficient_data(self):
        """Trend requires >= 2 non-null observations. One sample is not a trend."""
        snapshots_by_loan = {
            1: [_make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), current_ltv=0.55)],
            2: [
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 1, 1), current_ltv=None),
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 4, 1), current_ltv=0.60),
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 7, 1), current_ltv=None),
            ],
            3: [],
        }
        # Single non-null sample -> None
        assert _snapshot_trend(snapshots_by_loan, 1, "current_ltv") is None
        # Two snapshots but only one non-null -> None
        assert _snapshot_trend(snapshots_by_loan, 2, "current_ltv") is None
        # Empty list -> None
        assert _snapshot_trend(snapshots_by_loan, 3, "current_ltv") is None
        # Loan id not in dict -> None
        assert _snapshot_trend(snapshots_by_loan, 999, "current_ltv") is None
        # None safety
        assert _snapshot_trend(None, 1, "current_ltv") is None

    def test_snapshot_trend_computes_delta_and_direction(self):
        """Two+ non-null points -> dict with delta + direction.

        Up case: 0.55 -> 0.72 (LTV deteriorating)
        Down case: 2.5 -> 1.4 (ICR deteriorating, delta is negative)
        Flat case: 3 -> 3 (no change)
        Mid-window nulls are skipped, earliest and latest are the boundary
        non-null observations.
        """
        snapshots_by_loan = {
            10: [
                _make_snapshot(loan_id=10, snapshot_date=date(2024, 1, 1), current_ltv=0.55),
                _make_snapshot(loan_id=10, snapshot_date=date(2024, 4, 1), current_ltv=None),
                _make_snapshot(loan_id=10, snapshot_date=date(2024, 7, 1), current_ltv=0.72),
            ],
            20: [
                _make_snapshot(loan_id=20, snapshot_date=date(2024, 1, 1), interest_coverage_ratio=2.5),
                _make_snapshot(loan_id=20, snapshot_date=date(2024, 7, 1), interest_coverage_ratio=1.4),
            ],
            30: [
                _make_snapshot(loan_id=30, snapshot_date=date(2024, 1, 1), internal_credit_rating=3),
                _make_snapshot(loan_id=30, snapshot_date=date(2024, 7, 1), internal_credit_rating=3),
            ],
        }

        up = _snapshot_trend(snapshots_by_loan, 10, "current_ltv")
        assert up is not None
        assert up["earliest"] == 0.55
        assert up["latest"] == 0.72
        assert up["delta"] == pytest.approx(0.17, abs=0.001)
        assert up["direction"] == "up"
        assert up["earliest_date"] == date(2024, 1, 1)
        assert up["latest_date"] == date(2024, 7, 1)

        down = _snapshot_trend(snapshots_by_loan, 20, "interest_coverage_ratio")
        assert down is not None
        assert down["earliest"] == 2.5
        assert down["latest"] == 1.4
        assert down["delta"] == pytest.approx(-1.1, abs=0.001)
        assert down["direction"] == "down"

        flat = _snapshot_trend(snapshots_by_loan, 30, "internal_credit_rating")
        assert flat is not None
        assert flat["delta"] == 0
        assert flat["direction"] == "flat"


# ---------------------------------------------------------------------------
# Migration matrix
# ---------------------------------------------------------------------------


class TestCreditMigrationMatrix:
    def test_migration_matrix_no_snapshots(self):
        """No snapshots at all -> empty matrix, all counts zero, default 1-5 rating universe."""
        loans = [_make_loan(id=1), _make_loan(id=2)]
        result = compute_credit_migration_matrix(loans, snapshots_by_loan={})

        assert result["upgrades_count"] == 0
        assert result["stable_count"] == 0
        assert result["downgrades_count"] == 0
        assert result["loans_with_migration_history"] == 0
        # Loans have a current rating (default 2) but no snapshot history
        assert result["loans_without_history"] == 2
        # Default 1-5 rating universe is always present
        assert result["rating_order"] == [1, 2, 3, 4, 5]
        assert result["rating_labels"] == ["AAA", "AA", "A", "BBB", "BB+/Below"]
        # Matrix is 5x5 of zeros
        assert len(result["matrix"]) == 5
        assert all(len(row) == 5 for row in result["matrix"])
        assert all(cell == 0 for row in result["matrix"] for cell in row)
        assert result["at_risk_loans"] == []

    def test_migration_matrix_counts_upgrades_stable_downgrades(self):
        """Mix of upgrades, stable, and downgrades land in the right buckets and matrix cells."""
        loans = [
            _make_loan(id=1, company_name="UpCo"),       # upgrade 3 -> 2
            _make_loan(id=2, company_name="StableCo"),    # stable 2 -> 2
            _make_loan(id=3, company_name="DownCo"),      # downgrade 2 -> 4 (2 notches)
            _make_loan(id=4, company_name="BadCo"),       # downgrade 3 -> 5 (2 notches)
        ]
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), internal_credit_rating=3),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), internal_credit_rating=2),
            ],
            2: [
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 1, 1), internal_credit_rating=2),
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 7, 1), internal_credit_rating=2),
            ],
            3: [
                _make_snapshot(loan_id=3, snapshot_date=date(2024, 1, 1), internal_credit_rating=2),
                _make_snapshot(loan_id=3, snapshot_date=date(2024, 7, 1), internal_credit_rating=4),
            ],
            4: [
                _make_snapshot(loan_id=4, snapshot_date=date(2024, 1, 1), internal_credit_rating=3),
                _make_snapshot(loan_id=4, snapshot_date=date(2024, 7, 1), internal_credit_rating=5),
            ],
        }
        result = compute_credit_migration_matrix(loans, snapshots_by_loan=snapshots_by_loan)

        assert result["upgrades_count"] == 1
        assert result["stable_count"] == 1
        assert result["downgrades_count"] == 2
        assert result["loans_with_migration_history"] == 4

        # Rating universe is the standard 1-5 since all observed ratings are in range
        idx = {r: i for i, r in enumerate(result["rating_order"])}

        # UpCo: 3 -> 2 lands at matrix[idx[3]][idx[2]]
        assert result["matrix"][idx[3]][idx[2]] == 1
        # StableCo: 2 -> 2 lands on the diagonal
        assert result["matrix"][idx[2]][idx[2]] == 1
        # DownCo: 2 -> 4
        assert result["matrix"][idx[2]][idx[4]] == 1
        # BadCo: 3 -> 5
        assert result["matrix"][idx[3]][idx[5]] == 1

        # Row totals (totals_from): how many loans started at each rating
        assert result["totals_from"][idx[2]] == 2  # StableCo + DownCo
        assert result["totals_from"][idx[3]] == 2  # UpCo + BadCo

        # Column totals (totals_to): how many loans ended at each rating
        assert result["totals_to"][idx[2]] == 2  # UpCo + StableCo
        assert result["totals_to"][idx[4]] == 1  # DownCo
        assert result["totals_to"][idx[5]] == 1  # BadCo

    def test_migration_matrix_at_risk_sorted_by_severity(self):
        """at_risk_loans only includes downgraded loans, sorted by notches_down desc."""
        loans = [
            _make_loan(id=1, company_name="Alpha"),  # 1 notch down
            _make_loan(id=2, company_name="Beta"),   # 3 notches down (worst)
            _make_loan(id=3, company_name="Gamma"),  # 2 notches down
            _make_loan(id=4, company_name="Delta"),  # upgrade — should NOT appear
        ]
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), internal_credit_rating=2),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), internal_credit_rating=3),
            ],
            2: [
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 1, 1), internal_credit_rating=1),
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 7, 1), internal_credit_rating=4),
            ],
            3: [
                _make_snapshot(loan_id=3, snapshot_date=date(2024, 1, 1), internal_credit_rating=2),
                _make_snapshot(loan_id=3, snapshot_date=date(2024, 7, 1), internal_credit_rating=4),
            ],
            4: [
                _make_snapshot(loan_id=4, snapshot_date=date(2024, 1, 1), internal_credit_rating=4),
                _make_snapshot(loan_id=4, snapshot_date=date(2024, 7, 1), internal_credit_rating=2),
            ],
        }
        result = compute_credit_migration_matrix(loans, snapshots_by_loan=snapshots_by_loan)

        assert len(result["at_risk_loans"]) == 3  # Delta is upgrade, excluded
        # Sorted by severity: Beta (3) -> Gamma (2) -> Alpha (1)
        assert result["at_risk_loans"][0]["company"] == "Beta"
        assert result["at_risk_loans"][0]["notches_down"] == 3
        assert result["at_risk_loans"][0]["from_label"] == "AAA"
        assert result["at_risk_loans"][0]["to_label"] == "BBB"
        assert result["at_risk_loans"][1]["company"] == "Gamma"
        assert result["at_risk_loans"][1]["notches_down"] == 2
        assert result["at_risk_loans"][2]["company"] == "Alpha"
        assert result["at_risk_loans"][2]["notches_down"] == 1

    def test_migration_matrix_loans_without_history_counted_separately(self):
        """A loan with only ONE rating observation goes into loans_without_history,
        not into the matrix. The coverage block reflects the field-level signal.
        """
        loans = [
            _make_loan(id=1),  # has history
            _make_loan(id=2),  # only one snapshot — no history
            _make_loan(id=3),  # has current rating on loan but no snapshots at all
        ]
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), internal_credit_rating=2),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), internal_credit_rating=3),
            ],
            2: [
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 1, 1), internal_credit_rating=4),
            ],
        }
        result = compute_credit_migration_matrix(loans, snapshots_by_loan=snapshots_by_loan)

        assert result["loans_with_migration_history"] == 1  # only loan 1
        assert result["loans_without_history"] == 2  # loans 2 and 3
        assert result["loans_with_rating_data"] == 3  # all three have rating signals

        # Coverage block from compute_snapshot_coverage uses required_field
        assert "internal_credit_rating" in result["coverage"]["per_field_coverage"]
        # Loans 1 and 2 have at least one snapshot with internal_credit_rating; loan 3 has none
        assert result["coverage"]["per_field_coverage"]["internal_credit_rating"] == pytest.approx(2 / 3, abs=0.001)


# ---------------------------------------------------------------------------
# Borrower fundamentals
# ---------------------------------------------------------------------------


class TestCreditFundamentals:
    def test_fundamentals_no_snapshots(self):
        """No snapshot history -> all aggregates None, empty rows, coverage shows 0%."""
        loans = [_make_loan(id=1), _make_loan(id=2)]
        result = compute_credit_fundamentals(loans, snapshots_by_loan={})

        assert result["wavg_revenue_growth"] is None
        assert result["wavg_ebitda_growth"] is None
        assert result["loans_with_revenue_trend"] == 0
        assert result["loans_with_ebitda_trend"] == 0
        assert result["rows"] == []
        assert result["revenue_decliners"] == []
        assert result["ebitda_decliners"] == []
        assert result["coverage"]["per_field_coverage"]["current_revenue"] == 0.0
        assert result["coverage"]["per_field_coverage"]["current_ebitda"] == 0.0

    def test_fundamentals_weighted_growth_aggregation(self):
        """Weighted growth uses current invested capital only.

        The weighting basis should ignore hold_size noise and use
        current_invested_capital for the weighted growth outputs.

        Loan A: current invested $100M, EBITDA 10 -> 12 (+20%)
        Loan B: current invested $400M, EBITDA 20 -> 19 (-5%)
        Weighted: (0.20 * 100 + -0.05 * 400) / (100 + 400) = (20 - 20) / 500 = 0.0
        """
        loans = [
            _make_loan(
                id=1,
                company_name="A",
                hold_size=400.0,
                entry_loan_amount=400.0,
                current_invested_capital=100.0,
            ),
            _make_loan(
                id=2,
                company_name="B",
                hold_size=100.0,
                entry_loan_amount=100.0,
                current_invested_capital=400.0,
            ),
        ]
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), current_ebitda=10.0, current_revenue=80.0),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), current_ebitda=12.0, current_revenue=88.0),
            ],
            2: [
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 1, 1), current_ebitda=20.0, current_revenue=200.0),
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 7, 1), current_ebitda=19.0, current_revenue=180.0),
            ],
        }
        result = compute_credit_fundamentals(loans, snapshots_by_loan=snapshots_by_loan)

        assert result["loans_with_ebitda_trend"] == 2
        assert result["loans_with_revenue_trend"] == 2

        # EBITDA: (0.20 * 100 + -0.05 * 400) / 500 = 0
        assert result["wavg_ebitda_growth"] == pytest.approx(0.0, abs=0.001)
        # Revenue: A grew (88-80)/80 = 0.10, B fell (180-200)/200 = -0.10
        # Weighted: (0.10 * 100 + -0.10 * 400) / 500 = (10 - 40) / 500 = -0.06
        assert result["wavg_revenue_growth"] == pytest.approx(-0.06, abs=0.001)

        # Both loans show in rows; the decliner (B) gets sorted to the top
        assert len(result["rows"]) == 2
        assert result["rows"][0]["company"] == "B"  # decliner first
        assert result["rows"][1]["company"] == "A"

    def test_fundamentals_entry_vs_exit_current_summary_and_snapshot_fallback(self):
        loans = [
            _make_loan(
                id=1,
                company_name="Alpha",
                fund_name="Fund A",
                status="Unrealized",
                current_invested_capital=20.0,
                entry_revenue=100.0,
                current_revenue=110.0,
                entry_ltv=0.50,
                current_ltv=None,
                entry_coverage_ratio=1.50,
                current_coverage_ratio=1.80,
                entry_equity_cushion=0.45,
                current_equity_cushion=0.40,
            ),
            _make_loan(
                id=2,
                company_name="Beta",
                fund_name="Fund B",
                status="Realized",
                exit_date=date(2024, 12, 31),
                current_invested_capital=80.0,
                entry_revenue=200.0,
                current_revenue=180.0,
                entry_ltv=0.60,
                current_ltv=0.55,
                entry_coverage_ratio=2.00,
                current_coverage_ratio=1.60,
                entry_equity_cushion=0.40,
                current_equity_cushion=0.45,
            ),
        ]
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), current_ltv=0.52),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), current_ltv=0.58),
            ]
        }

        result = compute_credit_fundamentals(loans, snapshots_by_loan=snapshots_by_loan)

        assert result["loan_count"] == 2
        assert result["fund_count"] == 2
        assert result["weighted_loan_count"] == 2
        assert result["total_current_invested_capital"] == pytest.approx(100.0, abs=0.001)

        revenue_summary = result["summary_by_key"]["revenue"]
        assert revenue_summary["weighted_average_entry"] == pytest.approx(180.0, abs=0.001)
        assert revenue_summary["weighted_average_exit_current"] == pytest.approx(166.0, abs=0.001)
        assert revenue_summary["weighted_average_delta"] == pytest.approx(-14.0, abs=0.001)
        assert revenue_summary["average_delta"] == pytest.approx(-5.0, abs=0.001)

        ltv_summary = result["summary_by_key"]["ltv"]
        assert ltv_summary["weighted_average_exit_current"] == pytest.approx(0.556, abs=0.001)

        fund_a = next(row for row in result["fund_rows"] if row["fund_name"] == "Fund A")
        assert fund_a["metrics"]["revenue"]["weighted_average_entry"] == pytest.approx(100.0, abs=0.001)
        assert fund_a["metrics"]["ltv"]["weighted_average_exit_current"] == pytest.approx(0.58, abs=0.001)

        alpha_row = next(row for row in result["deal_rows"] if row["company_name"] == "Alpha")
        beta_row = next(row for row in result["deal_rows"] if row["company_name"] == "Beta")
        assert alpha_row["ltv_exit_current"] == pytest.approx(0.58, abs=0.001)
        assert alpha_row["coverage_ratio_delta"] == pytest.approx(0.30, abs=0.001)
        assert beta_row["exit_current_label"] == "Exit"
        assert [group["fund_name"] for group in result["deal_groups"]] == ["Fund A", "Fund B"]
        assert [row["company_name"] for row in result["deal_groups"][0]["rows"]] == ["Alpha"]

    def test_fundamentals_zero_only_summaries_render_as_na(self):
        loans = [
            _make_loan(
                id=1,
                fund_name="Zero Fund",
                company_name="Zero Co 1",
                current_invested_capital=25.0,
                entry_revenue=0.0,
                current_revenue=0.0,
                entry_ltv=0.0,
                current_ltv=0.0,
                entry_coverage_ratio=0.0,
                current_coverage_ratio=0.0,
                entry_equity_cushion=0.0,
                current_equity_cushion=0.0,
            ),
            _make_loan(
                id=2,
                fund_name="Zero Fund",
                company_name="Zero Co 2",
                current_invested_capital=75.0,
                entry_revenue=0.0,
                current_revenue=0.0,
                entry_ltv=0.0,
                current_ltv=0.0,
                entry_coverage_ratio=0.0,
                current_coverage_ratio=0.0,
                entry_equity_cushion=0.0,
                current_equity_cushion=0.0,
            ),
        ]

        result = compute_credit_fundamentals(loans, snapshots_by_loan={})

        revenue_summary = result["summary_by_key"]["revenue"]
        assert revenue_summary["weighted_average_entry"] is None
        assert revenue_summary["weighted_average_exit_current"] is None
        assert revenue_summary["weighted_average_delta"] is None
        assert revenue_summary["average_entry"] is None
        assert revenue_summary["average_exit_current"] is None
        assert revenue_summary["average_delta"] is None

        fund_revenue_summary = result["fund_rows"][0]["metrics"]["revenue"]
        assert fund_revenue_summary["weighted_average_entry"] is None
        assert fund_revenue_summary["average_delta"] is None

    def test_fundamentals_zero_values_are_excluded_and_delta_becomes_na(self):
        loans = [
            _make_loan(
                id=1,
                fund_name="Mixed Fund",
                company_name="Healthy Co",
                current_invested_capital=40.0,
                entry_revenue=100.0,
                current_revenue=120.0,
                entry_ltv=0.50,
                current_ltv=0.45,
                entry_coverage_ratio=2.00,
                current_coverage_ratio=2.20,
                entry_equity_cushion=0.40,
                current_equity_cushion=0.42,
            ),
            _make_loan(
                id=2,
                fund_name="Mixed Fund",
                company_name="Missing Current Co",
                current_invested_capital=60.0,
                entry_revenue=150.0,
                current_revenue=0.0,
                entry_ltv=0.55,
                current_ltv=0.0,
                entry_coverage_ratio=1.75,
                current_coverage_ratio=0.0,
                entry_equity_cushion=0.35,
                current_equity_cushion=0.0,
            ),
        ]

        result = compute_credit_fundamentals(loans, snapshots_by_loan={})

        revenue_summary = result["summary_by_key"]["revenue"]
        assert revenue_summary["weighted_average_entry"] == pytest.approx(130.0, abs=0.001)
        assert revenue_summary["weighted_average_exit_current"] == pytest.approx(120.0, abs=0.001)
        assert revenue_summary["weighted_average_delta"] == pytest.approx(20.0, abs=0.001)
        assert revenue_summary["average_entry"] == pytest.approx(125.0, abs=0.001)
        assert revenue_summary["average_exit_current"] == pytest.approx(120.0, abs=0.001)
        assert revenue_summary["average_delta"] == pytest.approx(20.0, abs=0.001)
        assert revenue_summary["paired_count"] == 1

        ltv_summary = result["summary_by_key"]["ltv"]
        assert ltv_summary["weighted_average_entry"] == pytest.approx(0.53, abs=0.001)
        assert ltv_summary["weighted_average_exit_current"] == pytest.approx(0.45, abs=0.001)
        assert ltv_summary["weighted_average_delta"] == pytest.approx(-0.05, abs=0.001)
        assert ltv_summary["paired_count"] == 1

        missing_current_row = next(
            row for row in result["deal_rows"] if row["company_name"] == "Missing Current Co"
        )
        assert missing_current_row["revenue_exit_current"] is None
        assert missing_current_row["revenue_delta"] is None
        assert missing_current_row["revenue_delta_pct"] is None
        assert missing_current_row["ltv_exit_current"] is None
        assert missing_current_row["ltv_delta"] is None
        assert missing_current_row["coverage_ratio_exit_current"] is None
        assert missing_current_row["coverage_ratio_delta"] is None
        assert missing_current_row["equity_cushion_exit_current"] is None
        assert missing_current_row["equity_cushion_delta"] is None

    def test_fundamentals_zero_current_uses_latest_non_zero_snapshot_fallback(self):
        loans = [
            _make_loan(
                id=1,
                fund_name="Snapshot Fund",
                company_name="Snapshot Co",
                current_invested_capital=50.0,
                entry_revenue=100.0,
                current_revenue=0.0,
                entry_ltv=0.60,
                current_ltv=0.0,
            ),
        ]
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), current_revenue=140.0, current_ltv=0.58),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), current_revenue=0.0, current_ltv=0.0),
            ]
        }

        result = compute_credit_fundamentals(loans, snapshots_by_loan=snapshots_by_loan)
        row = result["deal_rows"][0]

        assert row["revenue_exit_current"] == pytest.approx(140.0, abs=0.001)
        assert row["revenue_delta"] == pytest.approx(40.0, abs=0.001)
        assert row["ltv_exit_current"] == pytest.approx(0.58, abs=0.001)
        assert row["ltv_delta"] == pytest.approx(-0.02, abs=0.001)

    def test_fundamentals_term_by_fund_and_group_subtotals_use_current_invested_capital(self):
        loans = [
            _make_loan(
                id=1,
                fund_name="Fund A",
                company_name="Alpha",
                current_invested_capital=25.0,
                hold_size=400.0,
                entry_loan_amount=400.0,
                term_years=3.0,
                entry_revenue=100.0,
                current_revenue=120.0,
                entry_ltv=0.50,
                current_ltv=0.45,
                entry_coverage_ratio=2.00,
                current_coverage_ratio=2.20,
                entry_equity_cushion=0.40,
                current_equity_cushion=0.42,
            ),
            _make_loan(
                id=2,
                fund_name="Fund A",
                company_name="Beta",
                current_invested_capital=75.0,
                hold_size=100.0,
                entry_loan_amount=100.0,
                term_years=5.0,
                entry_revenue=200.0,
                current_revenue=180.0,
                entry_ltv=0.60,
                current_ltv=0.55,
                entry_coverage_ratio=1.50,
                current_coverage_ratio=1.40,
                entry_equity_cushion=0.30,
                current_equity_cushion=0.28,
            ),
        ]

        result = compute_credit_fundamentals(loans, snapshots_by_loan={})

        fund_a = result["fund_rows"][0]
        assert fund_a["term_summary"]["weighted_average"] == pytest.approx(4.5, abs=0.001)
        assert fund_a["term_summary"]["average"] == pytest.approx(4.0, abs=0.001)

        term_table_row = result["term_by_fund_rows"][0]
        assert term_table_row["weighted_average_term_years"] == pytest.approx(4.5, abs=0.001)
        assert term_table_row["average_term_years"] == pytest.approx(4.0, abs=0.001)

        group = result["deal_groups"][0]
        assert group["weighted_subtotal"]["term_years"] == pytest.approx(4.5, abs=0.001)
        assert group["weighted_subtotal"]["revenue_entry"] == pytest.approx(175.0, abs=0.001)
        assert group["weighted_subtotal"]["revenue_exit_current"] == pytest.approx(165.0, abs=0.001)
        assert group["weighted_subtotal"]["revenue_delta"] == pytest.approx(-10.0, abs=0.001)
        assert group["weighted_subtotal"]["revenue_delta_pct"] == pytest.approx(-0.025, abs=0.001)

    def test_fundamentals_zero_term_years_are_excluded_from_term_averages(self):
        loans = [
            _make_loan(
                id=1,
                fund_name="Fund A",
                company_name="Known Term",
                current_invested_capital=60.0,
                term_years=4.0,
            ),
            _make_loan(
                id=2,
                fund_name="Fund A",
                company_name="Missing Term",
                current_invested_capital=40.0,
                term_years=0.0,
            ),
        ]

        result = compute_credit_fundamentals(loans, snapshots_by_loan={})
        fund_a = result["fund_rows"][0]

        assert fund_a["term_summary"]["count"] == 1
        assert fund_a["term_summary"]["weighted_average"] == pytest.approx(4.0, abs=0.001)
        assert fund_a["term_summary"]["average"] == pytest.approx(4.0, abs=0.001)
        assert result["deal_groups"][0]["weighted_subtotal"]["term_years"] == pytest.approx(4.0, abs=0.001)

    def test_fundamentals_decliners_sorted_by_absolute_drop(self):
        """ebitda_decliners only contains loans where EBITDA fell, sorted worst first.

        SmallDrop:  EBITDA 10 -> 9 (delta = -1)
        BigDrop:    EBITDA 50 -> 30 (delta = -20)
        Grower:     EBITDA 10 -> 15 (delta = +5, NOT a decliner)
        """
        loans = [
            _make_loan(id=1, company_name="SmallDrop"),
            _make_loan(id=2, company_name="BigDrop"),
            _make_loan(id=3, company_name="Grower"),
        ]
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), current_ebitda=10.0),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), current_ebitda=9.0),
            ],
            2: [
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 1, 1), current_ebitda=50.0),
                _make_snapshot(loan_id=2, snapshot_date=date(2024, 7, 1), current_ebitda=30.0),
            ],
            3: [
                _make_snapshot(loan_id=3, snapshot_date=date(2024, 1, 1), current_ebitda=10.0),
                _make_snapshot(loan_id=3, snapshot_date=date(2024, 7, 1), current_ebitda=15.0),
            ],
        }
        result = compute_credit_fundamentals(loans, snapshots_by_loan=snapshots_by_loan)

        assert len(result["ebitda_decliners"]) == 2  # Grower excluded
        assert result["ebitda_decliners"][0]["company"] == "BigDrop"
        assert result["ebitda_decliners"][0]["delta"] == pytest.approx(-20.0, abs=0.001)
        assert result["ebitda_decliners"][0]["growth_pct"] == pytest.approx(-0.40, abs=0.001)
        assert result["ebitda_decliners"][1]["company"] == "SmallDrop"
        assert result["ebitda_decliners"][1]["delta"] == pytest.approx(-1.0, abs=0.001)


class TestCreditUnderwriteOutcome:
    def test_underwrite_outcome_weights_by_current_invested_capital_and_sorts_worst_first(self):
        loans = [
            _make_loan(
                id=1,
                company_name="Miss Co",
                fund_name="Fund A",
                sector="Software",
                sponsor="Apollo",
                business_description="Mission-critical software vendor",
                sourcing_channel="Direct",
                current_invested_capital=80.0,
                estimated_irr_at_entry=0.20,
                gross_irr=0.05,
                realized_proceeds=10.0,
                unrealized_loan_value=65.0,
                unrealized_warrant_equity_value=0.0,
                total_value=75.0,
                coupon_rate=0.09,
                cash_margin=0.085,
                current_revenue=120.0,
                current_ebitda=30.0,
                entry_ltv=0.55,
                current_ltv=0.60,
                entry_coverage_ratio=1.80,
                current_coverage_ratio=1.60,
                entry_equity_cushion=0.45,
                current_equity_cushion=0.40,
            ),
            _make_loan(
                id=2,
                company_name="Beat Co",
                fund_name="Fund B",
                sector="Healthcare",
                sponsor="TPG",
                business_description="Healthcare platform",
                sourcing_channel="Sponsor",
                current_invested_capital=20.0,
                estimated_irr_at_entry=0.15,
                gross_irr=0.25,
                realized_proceeds=5.0,
                unrealized_loan_value=20.0,
                unrealized_warrant_equity_value=0.0,
                total_value=25.0,
                coupon_rate=0.08,
                cash_margin=0.075,
                current_revenue=140.0,
                current_ebitda=28.0,
                entry_ltv=0.50,
                current_ltv=0.45,
                entry_coverage_ratio=1.90,
                current_coverage_ratio=2.10,
                entry_equity_cushion=0.42,
                current_equity_cushion=0.48,
            ),
        ]

        result = compute_credit_underwrite_outcome(loans, snapshots_by_loan={})

        summary = result["summary"]
        assert summary["loan_count"] == 2
        assert summary["fund_count"] == 2
        assert summary["weighted_loan_count"] == 2
        assert summary["total_current_invested_capital"] == pytest.approx(100.0, abs=0.001)
        assert summary["weighted_estimated_irr"] == pytest.approx(0.19, abs=0.001)
        assert summary["weighted_actual_gross_irr"] == pytest.approx(0.09, abs=0.001)
        assert summary["weighted_delta_irr"] == pytest.approx(-0.10, abs=0.001)
        assert summary["hit_rate"] == pytest.approx(0.5, abs=0.001)
        assert summary["miss_count"] == 1
        assert summary["worst_delta_irr"] == pytest.approx(-0.15, abs=0.001)
        assert summary["best_delta_irr"] == pytest.approx(0.10, abs=0.001)

        worst_row = result["worst_rows"][0]
        assert worst_row["company_name"] == "Miss Co"
        assert worst_row["delta_irr"] == pytest.approx(-0.15, abs=0.001)
        assert worst_row["gross_moic"] == pytest.approx(75.0 / 80.0, abs=0.001)
        assert worst_row["sector"] == "Software"
        assert worst_row["sponsor"] == "Apollo"
        assert worst_row["business_description"] == "Mission-critical software vendor"
        assert worst_row["sourcing_channel"] == "Direct"

    def test_underwrite_outcome_uses_snapshot_gross_irr_fallback_and_tracks_missing_counts(self):
        loans = [
            _make_loan(
                id=1,
                company_name="Snapshot Actual Co",
                fund_name="Fund A",
                current_invested_capital=50.0,
                estimated_irr_at_entry=0.18,
                gross_irr=None,
            ),
            _make_loan(
                id=2,
                company_name="Missing Estimate Co",
                fund_name="Fund A",
                current_invested_capital=30.0,
                estimated_irr_at_entry=None,
                gross_irr=0.12,
            ),
            _make_loan(
                id=3,
                company_name="Missing Actual Co",
                fund_name="Fund B",
                current_invested_capital=20.0,
                estimated_irr_at_entry=0.14,
                gross_irr=None,
            ),
        ]
        snapshots_by_loan = {
            1: [_make_snapshot(loan_id=1, snapshot_date=date(2024, 6, 1), gross_irr=0.11)],
        }

        result = compute_credit_underwrite_outcome(loans, snapshots_by_loan=snapshots_by_loan)

        assert result["summary"]["loan_count"] == 1
        assert result["summary"]["missing_estimate_count"] == 1
        assert result["summary"]["missing_actual_count"] == 1
        row = result["rows"][0]
        assert row["company_name"] == "Snapshot Actual Co"
        assert row["actual_gross_irr"] == pytest.approx(0.11, abs=0.001)
        assert row["actual_gross_irr_source"] == "Latest Snapshot"
        assert row["delta_irr"] == pytest.approx(-0.07, abs=0.001)

    def test_underwrite_outcome_fund_rows_sort_by_roman_numeral_sequence(self):
        loans = [
            _make_loan(
                id=1,
                company_name="Fund III Deal",
                fund_name="Fund III",
                current_invested_capital=30.0,
                estimated_irr_at_entry=0.14,
                gross_irr=0.10,
            ),
            _make_loan(
                id=2,
                company_name="Fund I Deal",
                fund_name="Fund I",
                current_invested_capital=20.0,
                estimated_irr_at_entry=0.12,
                gross_irr=0.09,
            ),
            _make_loan(
                id=3,
                company_name="Fund II Deal",
                fund_name="Fund II",
                current_invested_capital=25.0,
                estimated_irr_at_entry=0.13,
                gross_irr=0.11,
            ),
        ]

        result = compute_credit_underwrite_outcome(loans, snapshots_by_loan={})

        assert [row["fund_name"] for row in result["fund_rows"]] == ["Fund I", "Fund II", "Fund III"]


class TestCreditPricingTrends:
    def test_weighted_coupon_and_floor_use_current_invested_capital(self):
        loans = [
            _make_loan(
                id=1,
                fund_name="Fund A",
                company_name="Large Position",
                close_date=date(2024, 1, 15),
                current_invested_capital=80.0,
                hold_size=10.0,
                entry_loan_amount=10.0,
                coupon_rate=0.10,
                floor_rate=0.01,
                fee_upfront=0.01,
                sector="Software",
            ),
            _make_loan(
                id=2,
                fund_name="Fund A",
                company_name="Small Position",
                close_date=date(2024, 2, 1),
                current_invested_capital=20.0,
                hold_size=90.0,
                entry_loan_amount=90.0,
                coupon_rate=0.30,
                floor_rate=0.05,
                fee_upfront=0.03,
                sector="Software",
            ),
        ]

        result = compute_credit_pricing_trends(loans, primary_dim="sector", time_group="quarter")

        assert result["summary"]["weighted_average_coupon_rate"] == pytest.approx(0.14, abs=0.001)
        assert result["summary"]["weighted_average_floor_rate"] == pytest.approx(0.018, abs=0.001)
        assert result["summary"]["average_coupon_rate"] == pytest.approx(0.20, abs=0.001)
        assert result["summary"]["weighted_average_upfront_fee"] == pytest.approx(0.014, abs=0.001)
        assert result["summary"]["average_upfront_fee"] == pytest.approx(0.02, abs=0.001)

    def test_time_rows_group_by_selected_entry_date_granularity(self):
        loans = [
            _make_loan(
                id=1,
                company_name="Q1 Deal",
                fund_name="Fund A",
                close_date=date(2024, 2, 5),
                current_invested_capital=40.0,
                coupon_rate=0.08,
                floor_rate=0.01,
                fee_upfront=0.005,
            ),
            _make_loan(
                id=2,
                company_name="Q2 Deal",
                fund_name="Fund A",
                close_date=date(2024, 5, 20),
                current_invested_capital=60.0,
                coupon_rate=0.09,
                floor_rate=0.015,
                fee_upfront=0.007,
            ),
            _make_loan(
                id=3,
                company_name="Unknown Date Deal",
                fund_name="Fund B",
                close_date=None,
                current_invested_capital=10.0,
                coupon_rate=0.11,
                floor_rate=0.02,
                fee_upfront=0.002,
            ),
        ]

        result = compute_credit_pricing_trends(loans, time_group="quarter")

        labels = [row["period_label"] for row in result["time_rows"]]
        assert labels == ["2024 Q1", "2024 Q2", "Unknown Date"]
        assert result["time_rows"][0]["loan_count"] == 1
        assert result["time_rows"][1]["total_current_invested_capital"] == pytest.approx(60.0, abs=0.001)
        assert result["time_rows"][2]["average_upfront_fee"] == pytest.approx(0.002, abs=0.001)
        assert result["time_series_charts"][0]["labels"] == labels
        assert result["time_series_charts"][0]["datasets"][0]["label"] == "Weighted Avg Coupon"
        assert result["time_series_charts"][0]["datasets"][1]["label"] == "Avg Coupon"
        assert result["time_series_charts"][2]["metric_kind"] == "percent"

    def test_dimension_rows_follow_selected_dimension(self):
        loans = [
            _make_loan(
                id=1,
                company_name="Alpha",
                fund_name="Fund A",
                close_date=date(2024, 1, 10),
                current_invested_capital=25.0,
                coupon_rate=0.08,
                floor_rate=0.01,
                fee_upfront=0.005,
                sponsor="Apollo",
            ),
            _make_loan(
                id=2,
                company_name="Beta",
                fund_name="Fund B",
                close_date=date(2024, 3, 12),
                current_invested_capital=35.0,
                coupon_rate=0.09,
                floor_rate=0.015,
                fee_upfront=0.008,
                sponsor=None,
            ),
        ]

        result = compute_credit_pricing_trends(loans, primary_dim="sponsor", time_group="year")

        assert result["primary_dim"] == "sponsor"
        assert result["primary_dim_label"] == "Sponsor"
        dimension_labels = [row["dimension_value"] for row in result["dimension_rows"]]
        assert "Apollo" in dimension_labels
        assert "Unknown" in dimension_labels

    def test_fund_rows_sort_by_roman_numeral_sequence(self):
        loans = [
            _make_loan(id=1, fund_name="Fund III", current_invested_capital=40.0, fee_upfront=0.0075),
            _make_loan(id=2, fund_name="Fund I", current_invested_capital=20.0, fee_upfront=0.0050),
            _make_loan(id=3, fund_name="Fund II", current_invested_capital=30.0, fee_upfront=0.0060),
        ]

        result = compute_credit_pricing_trends(loans, time_group="year")

        assert [row["fund_name"] for row in result["fund_rows"]] == ["Fund I", "Fund II", "Fund III"]

    def test_upfront_fee_stays_decimal_percentage_not_currency(self):
        loans = [
            _make_loan(
                id=1,
                fund_name="Fund I",
                current_invested_capital=50.0,
                fx_rate_to_usd=1.5,
                fee_upfront=0.0075,
            )
        ]

        result = compute_credit_pricing_trends(loans, time_group="year")

        assert result["summary"]["average_upfront_fee"] == pytest.approx(0.0075, abs=0.001)
        assert result["summary"]["weighted_average_upfront_fee"] == pytest.approx(0.0075, abs=0.001)


class TestCreditWatchlist:
    """Watchlist scoring is the audit trail for "why is this loan on the list."

    Every test here pins a single rubric line so when an analyst asks "why did
    that loan get +20 points," we have a test that proves the answer.
    """

    def test_watchlist_no_loans(self):
        """Empty portfolio -> empty rows, all bucket counts zero."""
        result = compute_credit_watchlist([], snapshots_by_loan={})
        assert result["rows"] == []
        assert result["urgent_count"] == 0
        assert result["attention_count"] == 0
        assert result["monitor_count"] == 0
        assert result["clear_count"] == 0
        assert result["total_loans"] == 0
        # Weights and thresholds always exposed so the template can render the rubric
        assert "ltv_trend" in result["weights"]
        assert "urgent" in result["thresholds"]

    def test_watchlist_status_auto_triggers_score_100(self):
        """A loan with default_status != Performing must score >= STATUS_AUTO weight.

        This is the contractual hard trigger: any non-Performing loan lands on
        the urgent bucket regardless of trend signals.
        """
        loan = _make_loan(id=1, default_status="Default", covenant_compliant=True)
        result = compute_credit_watchlist([loan], snapshots_by_loan={})

        row = result["rows"][0]
        assert row["score"] >= WATCHLIST_WEIGHT_STATUS_AUTO or row["score"] == WATCHLIST_SCORE_CAP
        assert row["bucket"] == "urgent"
        # Breakdown must contain the status factor with the full weight
        status_entries = [b for b in row["breakdown"] if b["factor"] == "status"]
        assert len(status_entries) == 1
        assert status_entries[0]["points"] == WATCHLIST_WEIGHT_STATUS_AUTO

    def test_watchlist_covenant_breach_auto_triggers_score_100(self):
        """covenant_compliant=False is a hard trigger at COVENANT weight."""
        loan = _make_loan(id=1, default_status="Performing", covenant_compliant=False)
        result = compute_credit_watchlist([loan], snapshots_by_loan={})

        row = result["rows"][0]
        assert row["score"] >= WATCHLIST_WEIGHT_COVENANT or row["score"] == WATCHLIST_SCORE_CAP
        assert row["bucket"] == "urgent"
        cov_entries = [b for b in row["breakdown"] if b["factor"] == "covenant"]
        assert len(cov_entries) == 1
        assert cov_entries[0]["points"] == WATCHLIST_WEIGHT_COVENANT

    def test_watchlist_ltv_trend_adds_points(self):
        """Climbing LTV across snapshots adds proportional points up to LTV_TREND weight.

        We construct a +10pp LTV climb (well above the 5pp full-points threshold)
        on an otherwise clean Performing loan with no covenant breach. The only
        active signal must be ltv_trend, and it must hit the full weight.
        """
        loan = _make_loan(id=1, default_status="Performing", covenant_compliant=True)
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), current_ltv=0.50),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), current_ltv=0.60),
            ],
        }
        result = compute_credit_watchlist([loan], snapshots_by_loan=snapshots_by_loan)
        row = result["rows"][0]

        ltv_entries = [b for b in row["breakdown"] if b["factor"] == "ltv_trend"]
        assert len(ltv_entries) == 1
        # +10pp climb >> +5pp threshold, so the LTV trend signal should hit max
        assert ltv_entries[0]["points"] == WATCHLIST_WEIGHT_LTV_TREND
        assert row["score"] >= WATCHLIST_WEIGHT_LTV_TREND

    def test_watchlist_ebitda_trend_adds_points(self):
        """Falling EBITDA across snapshots adds the full EBITDA_TREND weight."""
        loan = _make_loan(id=1, default_status="Performing", covenant_compliant=True)
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), current_ebitda=20.0),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), current_ebitda=15.0),
            ],
        }
        result = compute_credit_watchlist([loan], snapshots_by_loan=snapshots_by_loan)
        row = result["rows"][0]

        ebitda_entries = [b for b in row["breakdown"] if b["factor"] == "ebitda_trend"]
        assert len(ebitda_entries) == 1
        assert ebitda_entries[0]["points"] == WATCHLIST_WEIGHT_EBITDA_TREND

    def test_watchlist_sponsor_history_adds_points(self):
        """A sponsor with a prior default in the same portfolio adds SPONSOR_HISTORY points.

        Two loans, same sponsor 'BadSponsor'. Loan 1 defaulted. Loan 2 is
        performing but should still get the +SPONSOR_HISTORY adder because of
        loan 1's status.
        """
        defaulted = _make_loan(id=1, company_name="LoanA", sponsor="BadSponsor", default_status="Default")
        performing = _make_loan(
            id=2, company_name="LoanB", sponsor="BadSponsor",
            default_status="Performing", covenant_compliant=True,
        )
        result = compute_credit_watchlist([defaulted, performing], snapshots_by_loan={})

        # Find the performing loan in the result
        performing_row = next(r for r in result["rows"] if r["company"] == "LoanB")
        sponsor_entries = [b for b in performing_row["breakdown"] if b["factor"] == "sponsor_history"]
        assert len(sponsor_entries) == 1
        assert sponsor_entries[0]["points"] == WATCHLIST_WEIGHT_SPONSOR_HISTORY

    def test_watchlist_score_capped_at_100(self):
        """Stacking multiple signals must not produce a score above the cap."""
        # Worst-case loan: defaulted + covenant breach + bad LTV + bad EBITDA + sponsor
        loan = _make_loan(
            id=1, default_status="Default", covenant_compliant=False, sponsor="ProblemSponsor",
        )
        # Make sponsor history non-trivial so we get the +10
        other_default = _make_loan(id=2, default_status="Default", sponsor="ProblemSponsor")
        snapshots_by_loan = {
            1: [
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 1, 1), current_ltv=0.50, current_ebitda=20.0),
                _make_snapshot(loan_id=1, snapshot_date=date(2024, 7, 1), current_ltv=0.95, current_ebitda=5.0),
            ],
        }
        result = compute_credit_watchlist([loan, other_default], snapshots_by_loan=snapshots_by_loan)

        focal_row = next(r for r in result["rows"] if r["loan_id"] == 1)
        # raw_score (pre-cap) should be way above 100
        # final score must equal the cap
        assert focal_row["score"] == WATCHLIST_SCORE_CAP

    def test_watchlist_score_breakdown_contract(self):
        """Every breakdown entry must have factor, points, reason — the audit trail.

        This is the central contract for the page: an analyst must be able to
        defend "this loan scored 65 because" with a per-factor receipt.
        """
        loan = _make_loan(id=1, default_status="Performing", covenant_compliant=False)
        result = compute_credit_watchlist([loan], snapshots_by_loan={})

        row = result["rows"][0]
        assert isinstance(row["breakdown"], list)
        assert len(row["breakdown"]) > 0
        for entry in row["breakdown"]:
            assert "factor" in entry
            assert "points" in entry
            assert "reason" in entry
            assert isinstance(entry["points"], int)
            assert entry["points"] > 0
            assert isinstance(entry["reason"], str) and entry["reason"]

    def test_watchlist_buckets_urgent_attention_monitor(self):
        """Bucket assignment must respect URGENT/ATTENTION/MONITOR thresholds.

        Build four loans whose scores land in each bucket and verify the
        bucket_counts and individual bucket field assignments.
        """
        # urgent: any non-Performing status fires the auto trigger
        urgent_loan = _make_loan(id=1, company_name="UrgentCo", default_status="Default")
        # attention: stack ltv_trend (20) + ebitda_trend (15) + ICR drop (20) = 55 -> attention (>=50)
        attention_loan = _make_loan(
            id=2, company_name="AttentionCo",
            default_status="Performing", covenant_compliant=True,
        )
        attention_snapshots = [
            _make_snapshot(
                loan_id=2, snapshot_date=date(2024, 1, 1),
                current_ltv=0.50, current_ebitda=20.0, interest_coverage_ratio=3.0,
            ),
            _make_snapshot(
                loan_id=2, snapshot_date=date(2024, 7, 1),
                current_ltv=0.60, current_ebitda=15.0, interest_coverage_ratio=2.0,
            ),
        ]
        # monitor: just ebitda decline (15) + small icr drop (~8) -> monitor (>=20)
        monitor_loan = _make_loan(
            id=3, company_name="MonitorCo",
            default_status="Performing", covenant_compliant=True,
        )
        monitor_snapshots = [
            _make_snapshot(
                loan_id=3, snapshot_date=date(2024, 1, 1),
                current_ebitda=20.0, interest_coverage_ratio=3.0,
            ),
            _make_snapshot(
                loan_id=3, snapshot_date=date(2024, 7, 1),
                current_ebitda=15.0, interest_coverage_ratio=2.8,
            ),
        ]
        # clear: performing, no signals
        clear_loan = _make_loan(
            id=4, company_name="ClearCo",
            default_status="Performing", covenant_compliant=True,
            floor_rate=0.05,  # above the 0.02 floor rolloff threshold
        )

        snapshots_by_loan = {2: attention_snapshots, 3: monitor_snapshots}
        result = compute_credit_watchlist(
            [urgent_loan, attention_loan, monitor_loan, clear_loan],
            snapshots_by_loan=snapshots_by_loan,
        )

        bucket_by_company = {r["company"]: r["bucket"] for r in result["rows"]}
        assert bucket_by_company["UrgentCo"] == "urgent"
        assert bucket_by_company["AttentionCo"] == "attention"
        assert bucket_by_company["MonitorCo"] == "monitor"
        assert bucket_by_company["ClearCo"] == "clear"

        assert result["urgent_count"] == 1
        assert result["attention_count"] == 1
        assert result["monitor_count"] == 1
        assert result["clear_count"] == 1


# ---------------------------------------------------------------------------
# Credit Benchmarking
# ---------------------------------------------------------------------------


class TestCreditBenchmarking:
    def test_credit_benchmarking_uses_credit_fund_performance(self):
        loans = [
            _make_lp_loan(
                id=1,
                fund_name="Credit Fund I",
                vintage_year=2021,
                close_date=date(2021, 4, 1),
                status="Unrealized",
            )
        ]
        fund_perf = {
            "Credit Fund I": SimpleNamespace(
                fund_name="Credit Fund I",
                vintage_year=2021,
                fund_size=500.0,
                net_irr=0.14,
                net_moic=None,
                net_tvpi=1.45,
                net_dpi=0.60,
                report_date=date(2024, 12, 31),
            )
        }
        thresholds = {
            2021: {
                "net_irr": {
                    "lower_quartile": 0.08,
                    "median": 0.11,
                    "upper_quartile": 0.13,
                    "top_5": 0.18,
                },
                "net_moic": {
                    "lower_quartile": 1.20,
                    "median": 1.35,
                    "upper_quartile": 1.50,
                    "top_5": 1.90,
                },
                "net_dpi": {
                    "lower_quartile": 0.30,
                    "median": 0.50,
                    "upper_quartile": 0.70,
                    "top_5": 1.00,
                },
            }
        }

        result = compute_credit_benchmarking_analysis(
            loans,
            fund_performance=fund_perf,
            benchmark_thresholds=thresholds,
            benchmark_asset_class="Private Credit",
        )

        assert result["meta"]["benchmark_asset_class"] == "Private Credit"
        assert result["kpis"]["fund_count"] == 1
        row = result["fund_rows"][0]
        assert row["fund_name"] == "Credit Fund I"
        assert row["net_irr"] == pytest.approx(0.14)
        assert row["net_tvpi"] == pytest.approx(1.45)
        assert row["net_moic"] == pytest.approx(1.45)
        assert row["benchmark_net_irr"]["rank_code"] == "q1"
        assert row["benchmark_net_moic"]["rank_code"] == "q2"
        assert row["benchmark_net_dpi"]["rank_code"] == "q2"
        assert row["full_coverage"] is True
        assert result["threshold_rows"][0]["vintage_year"] == 2021


# ---------------------------------------------------------------------------
# Credit Track Record
# ---------------------------------------------------------------------------


class TestCreditTrackRecord:
    """Tests for compute_credit_track_record — mirrors PE track record shape."""

    def _make_track_loan(self, **overrides):
        """Shorthand for a loan geared toward track-record testing."""
        defaults = dict(
            entry_loan_amount=25.0,
            realized_proceeds=5.0,
            unrealized_loan_value=22.0,
            unrealized_warrant_equity_value=1.5,
            total_value=28.5,
        )
        defaults.update(overrides)
        return _make_loan(**defaults)

    def test_empty_input(self):
        result = compute_credit_track_record([])
        assert result["funds"] == []
        assert result["overall"]["totals"]["deal_count"] == 0

    def test_single_loan_single_fund(self):
        loan = self._make_track_loan(id=1, fund_name="Fund A", status="Unrealized")
        result = compute_credit_track_record([loan])

        assert len(result["funds"]) == 1
        fund = result["funds"][0]
        assert fund["fund_name"] == "Fund A"
        assert len(fund["rows"]) == 1
        row = fund["rows"][0]
        assert row["company_name"] == "Acme Corp"
        assert row["status"] == "Unrealized"
        assert row["invested_equity"] == 25.0
        assert row["realized_value"] == 5.0
        assert row["unrealized_value"] == 22.0
        assert row["unrealized_warrant_equity_value"] == 1.5
        assert row["total_value"] == 28.5
        assert row["row_num"] == 1
        assert row["pct_total_invested"] == pytest.approx(1.0)

    def test_groups_by_fund(self):
        loan_a = self._make_track_loan(id=1, fund_name="Fund A", company_name="Co A")
        loan_b = self._make_track_loan(id=2, fund_name="Fund B", company_name="Co B")
        loan_a2 = self._make_track_loan(id=3, fund_name="Fund A", company_name="Co C")

        result = compute_credit_track_record([loan_a, loan_b, loan_a2])
        fund_names = [f["fund_name"] for f in result["funds"]]
        assert "Fund A" in fund_names
        assert "Fund B" in fund_names

        fund_a = next(f for f in result["funds"] if f["fund_name"] == "Fund A")
        assert len(fund_a["rows"]) == 2

    def test_status_ordering(self):
        """Fully Realized loans sort before Unrealized within a fund."""
        realized = self._make_track_loan(
            id=1, fund_name="Fund A", company_name="Realized Co",
            status="Realized", close_date=date(2020, 1, 1),
        )
        unrealized = self._make_track_loan(
            id=2, fund_name="Fund A", company_name="Unrealized Co",
            status="Unrealized", close_date=date(2021, 1, 1),
        )
        result = compute_credit_track_record([unrealized, realized])
        rows = result["funds"][0]["rows"]
        assert rows[0]["status"] == "Fully Realized"
        assert rows[1]["status"] == "Unrealized"

    def test_status_rollups_and_summary(self):
        realized = self._make_track_loan(
            id=1, fund_name="F1", status="Realized",
            entry_loan_amount=10.0, total_value=15.0,
        )
        unrealized = self._make_track_loan(
            id=2, fund_name="F1", status="Unrealized",
            entry_loan_amount=20.0, total_value=22.0,
        )
        result = compute_credit_track_record([realized, unrealized])
        fund = result["funds"][0]

        # Should have Fully Realized and Unrealized status rollups
        status_labels = [r["status"] for r in fund["status_rollups"]]
        assert "Fully Realized" in status_labels
        assert "Unrealized" in status_labels

        # Summary rollups: realized bundle + unrealized + all
        assert len(fund["summary_rollups"]) == 3

        # Overall should aggregate across all funds
        assert result["overall"]["totals"]["deal_count"] == 2

    def test_gross_moic_calculation(self):
        loan = self._make_track_loan(
            id=1, fund_name="F1",
            entry_loan_amount=20.0,
            realized_proceeds=5.0,
            unrealized_loan_value=23.5,
            unrealized_warrant_equity_value=1.5,
            total_value=30.0,
        )
        result = compute_credit_track_record([loan])
        row = result["funds"][0]["rows"][0]
        assert row["gross_moic"] == pytest.approx(1.5)
        assert row["realized_gross_moic"] == pytest.approx(5.0 / 20.0)
        assert row["unrealized_gross_moic"] == pytest.approx((23.5 + 1.5) / 20.0)

    def test_facility_pct(self):
        loan = self._make_track_loan(id=1, fund_name="F1", hold_size=25.0, issue_size=100.0)
        result = compute_credit_track_record([loan])
        row = result["funds"][0]["rows"][0]
        assert row["facility_pct"] == pytest.approx(0.25)
        # ownership_pct alias should match
        assert row["ownership_pct"] == row["facility_pct"]

    def test_hold_period(self):
        loan = self._make_track_loan(
            id=1, fund_name="F1",
            close_date=date(2022, 1, 1),
            exit_date=date(2024, 1, 1),
        )
        result = compute_credit_track_record([loan])
        row = result["funds"][0]["rows"][0]
        assert row["hold_period"] == pytest.approx(2.0, abs=0.05)

    def test_net_performance_from_fund_performance(self):
        loan = self._make_track_loan(id=1, fund_name="PCOF III")

        fund_perf = SimpleNamespace(
            fund_name="PCOF III",
            fund_size=500.0,
            vintage_year=2021,
            net_irr=0.12,
            net_moic=1.35,
            net_dpi=0.45,
            net_tvpi=1.35,
            net_rvpi=0.90,
            called_capital=425.0,
            distributed_capital=190.0,
            nav=380.0,
        )
        result = compute_credit_track_record(
            [loan], fund_performance={"PCOF III": fund_perf}
        )

        fund = result["funds"][0]
        assert fund["fund_size"] == 500.0
        assert fund["vintage_year"] == 2021
        np = fund["net_performance"]
        assert np["net_irr"] == pytest.approx(0.12)
        assert np["net_moic"] == pytest.approx(1.35)
        assert np["net_dpi"] == pytest.approx(0.45)
        assert np["net_tvpi"] == pytest.approx(1.35)
        assert np["has_data"] is True
        assert np["conflicts"]["net_irr"] is False

    def test_net_performance_without_fund_performance(self):
        loan = self._make_track_loan(id=1, fund_name="F1")
        result = compute_credit_track_record([loan])

        np = result["funds"][0]["net_performance"]
        assert np["net_irr"] is None
        assert np["net_moic"] is None
        assert np["has_data"] is False

    def test_pct_fund_size(self):
        loan = self._make_track_loan(
            id=1, fund_name="F1", entry_loan_amount=50.0,
        )
        fund_perf = SimpleNamespace(
            fund_name="F1", fund_size=500.0, vintage_year=None,
            net_irr=None, net_moic=None, net_dpi=None,
            net_tvpi=None, net_rvpi=None,
            called_capital=None, distributed_capital=None, nav=None,
        )
        result = compute_credit_track_record(
            [loan], fund_performance={"F1": fund_perf}
        )
        row = result["funds"][0]["rows"][0]
        assert row["pct_fund_size"] == pytest.approx(0.10)

    def test_current_invested_capital_drives_track_record_weighting(self):
        loan = self._make_track_loan(
            id=1,
            fund_name="F1",
            fund_size=200.0,
            hold_size=0.0,
            entry_loan_amount=25.0,
            current_invested_capital=40.0,
            realized_proceeds=10.0,
            unrealized_loan_value=50.0,
            unrealized_warrant_equity_value=0.0,
            total_value=60.0,
        )

        result = compute_credit_track_record([loan])
        fund = result["funds"][0]
        row = fund["rows"][0]

        assert fund["fund_size"] == pytest.approx(200.0)
        assert row["invested_equity"] == pytest.approx(40.0)
        assert row["current_invested_capital"] == pytest.approx(40.0)
        assert row["pct_total_invested"] == pytest.approx(1.0)
        assert row["pct_fund_size"] == pytest.approx(0.20)
        assert row["realized_value"] == pytest.approx(10.0)
        assert row["unrealized_value"] == pytest.approx(50.0)
        assert row["unrealized_warrant_equity_value"] == pytest.approx(0.0)
        assert row["total_value"] == pytest.approx(60.0)
        assert row["gross_moic"] == pytest.approx(1.5)
        assert row["realized_gross_moic"] == pytest.approx(0.25)
        assert row["unrealized_gross_moic"] == pytest.approx(1.25)

    def test_legacy_value_fields_flow_into_track_record(self):
        loan = self._make_track_loan(
            id=1,
            fund_name="F1",
            entry_loan_amount=20.0,
            current_invested_capital=20.0,
            realized_proceeds=None,
            realized_value=8.0,
            unrealized_loan_value=None,
            unrealized_value=16.0,
            unrealized_warrant_equity_value=2.0,
            total_value=None,
        )

        result = compute_credit_track_record([loan])
        row = result["funds"][0]["rows"][0]

        assert row["realized_value"] == pytest.approx(8.0)
        assert row["unrealized_value"] == pytest.approx(16.0)
        assert row["unrealized_warrant_equity_value"] == pytest.approx(2.0)
        assert row["unrealized_total_value"] == pytest.approx(18.0)
        assert row["total_value"] == pytest.approx(26.0)
        assert row["gross_moic"] == pytest.approx(1.3)

    def test_net_tvpi_falls_back_to_net_moic_when_missing(self):
        loan = self._make_track_loan(id=1, fund_name="F1")
        fund_perf = SimpleNamespace(
            fund_name="F1",
            fund_size=500.0,
            vintage_year=2020,
            net_irr=0.12,
            net_moic=1.41,
            net_dpi=0.52,
            net_tvpi=None,
            net_rvpi=None,
            called_capital=None,
            distributed_capital=None,
            nav=None,
        )

        result = compute_credit_track_record([loan], fund_performance={"F1": fund_perf})
        np = result["funds"][0]["net_performance"]

        assert np["net_moic"] == pytest.approx(1.41)
        assert np["net_tvpi"] == pytest.approx(1.41)

    def test_overall_pct_fund_size_uses_all_fund_sizes(self):
        loans = [
            self._make_track_loan(
                id=1,
                fund_name="F1",
                fund_size=200.0,
                current_invested_capital=40.0,
                hold_size=0.0,
                entry_loan_amount=10.0,
            ),
            self._make_track_loan(
                id=2,
                fund_name="F2",
                fund_size=300.0,
                current_invested_capital=60.0,
                hold_size=0.0,
                entry_loan_amount=15.0,
            ),
        ]

        result = compute_credit_track_record(loans)

        assert result["overall"]["totals"]["pct_fund_size"] == pytest.approx(0.20)
        assert result["overall"]["summary_rollups"][-1]["totals"]["pct_fund_size"] == pytest.approx(0.20)

    def test_summary_rollup_hold_period_weighted_by_current_invested_capital(self):
        loan_short = self._make_track_loan(
            id=1,
            fund_name="F1",
            current_invested_capital=80.0,
            hold_size=0.0,
            entry_loan_amount=10.0,
            close_date=date(2023, 1, 1),
            exit_date=date(2024, 1, 1),
        )
        loan_long = self._make_track_loan(
            id=2,
            fund_name="F1",
            current_invested_capital=20.0,
            hold_size=0.0,
            entry_loan_amount=10.0,
            close_date=date(2020, 1, 1),
            exit_date=date(2024, 1, 1),
        )

        result = compute_credit_track_record([loan_short, loan_long])
        overall_fund_rollup = result["funds"][0]["summary_rollups"][-1]["totals"]

        assert overall_fund_rollup["hold_period"] == pytest.approx(1.6, abs=0.1)

    def test_gross_irr_rollups_weighted_by_current_invested_capital(self):
        low_irr_large_position = self._make_track_loan(
            id=1,
            fund_name="F1",
            status="Unrealized",
            hold_size=10.0,
            entry_loan_amount=10.0,
            current_invested_capital=80.0,
            gross_irr=0.10,
        )
        high_irr_small_position = self._make_track_loan(
            id=2,
            fund_name="F1",
            status="Unrealized",
            hold_size=90.0,
            entry_loan_amount=90.0,
            current_invested_capital=20.0,
            gross_irr=0.30,
        )

        result = compute_credit_track_record([low_irr_large_position, high_irr_small_position])
        status_rollup = result["funds"][0]["status_rollups"][0]["totals"]
        summary_rollup = result["funds"][0]["summary_rollups"][-1]["totals"]
        overall_rollup = result["overall"]["summary_rollups"][-1]["totals"]

        # Weighted by current invested capital: (0.10 * 80 + 0.30 * 20) / 100 = 0.14
        assert status_rollup["gross_irr"] == pytest.approx(0.14, abs=0.001)
        assert summary_rollup["gross_irr"] == pytest.approx(0.14, abs=0.001)
        assert overall_rollup["gross_irr"] == pytest.approx(0.14, abs=0.001)

    def test_fx_conversion(self):
        loan = self._make_track_loan(
            id=1, fund_name="F1",
            entry_loan_amount=20.0,
            realized_proceeds=5.0,
            unrealized_loan_value=18.5,
            unrealized_warrant_equity_value=1.5,
            total_value=25.0,
            currency="EUR",
            fx_rate_to_usd=1.1,
        )
        result = compute_credit_track_record([loan])
        row = result["funds"][0]["rows"][0]
        assert row["invested_equity"] == pytest.approx(22.0)  # 20 * 1.1
        assert row["total_value"] == pytest.approx(27.5)  # 25 * 1.1

    def test_overall_rollups(self):
        loans = [
            self._make_track_loan(id=1, fund_name="F1", status="Realized"),
            self._make_track_loan(id=2, fund_name="F1", status="Unrealized"),
            self._make_track_loan(id=3, fund_name="F2", status="Unrealized"),
        ]
        result = compute_credit_track_record(loans)

        overall = result["overall"]
        assert overall["totals"]["deal_count"] == 3
        # Should have status_rollups and summary_rollups
        assert len(overall["status_rollups"]) >= 1
        assert len(overall["summary_rollups"]) >= 1
        # status_groups is a back-compat alias
        assert overall["status_groups"] == overall["status_rollups"]

    def test_default_status_mapped_to_fully_realized(self):
        """Defaulted/restructured loans count as Fully Realized in track record."""
        loan = self._make_track_loan(
            id=1, fund_name="F1", status="Default", default_status="Default",
        )
        result = compute_credit_track_record([loan])
        row = result["funds"][0]["rows"][0]
        assert row["status"] == "Fully Realized"

    def test_return_shape(self):
        loan = self._make_track_loan(id=1, fund_name="F1")
        result = compute_credit_track_record([loan])

        assert "funds" in result
        assert "overall" in result
        assert "loan_count" in result
        assert "fund_count" in result
        fund = result["funds"][0]
        required_fund_keys = {
            "fund_name", "fund_size", "fund_size_conflict", "rows",
            "status_rollups", "summary_rollups", "net_performance", "totals",
        }
        assert required_fund_keys.issubset(set(fund.keys()))


# ---------------------------------------------------------------------------
# Credit data cuts
# ---------------------------------------------------------------------------


class TestCreditDataCuts:
    """Tests for compute_credit_data_cuts — dimension slicing for credit."""

    def test_basic_grouping_by_sector(self):
        loans = [
            _make_loan(id=1, sector="Software", hold_size=20.0, moic=1.3, gross_irr=0.12),
            _make_loan(id=2, sector="Software", hold_size=30.0, moic=1.2, gross_irr=0.10),
            _make_loan(id=3, sector="Healthcare", hold_size=50.0, moic=1.5, gross_irr=0.15),
        ]
        result = compute_credit_data_cuts(loans, primary_dim="sector")
        assert result["primary_dim"] == "sector"
        assert result["primary_dim_label"] == "Sector"
        assert result["loan_count"] == 3
        labels = [g["label"] for g in result["groups"]]
        assert "Software" in labels
        assert "Healthcare" in labels
        sw = next(g for g in result["groups"] if g["label"] == "Software")
        assert sw["loan_count"] == 2

    def test_grouping_by_fund_name(self):
        loans = [
            _make_loan(id=1, fund_name="Fund A", hold_size=10.0),
            _make_loan(id=2, fund_name="Fund A", hold_size=20.0),
            _make_loan(id=3, fund_name="Fund B", hold_size=30.0),
        ]
        result = compute_credit_data_cuts(loans, primary_dim="fund_name")
        labels = [g["label"] for g in result["groups"]]
        assert "Fund A" in labels
        assert "Fund B" in labels

    def test_unknown_fallback(self):
        loans = [
            _make_loan(id=1, sector=None, hold_size=10.0),
            _make_loan(id=2, sector="Tech", hold_size=10.0),
        ]
        result = compute_credit_data_cuts(loans, primary_dim="sector")
        labels = [g["label"] for g in result["groups"]]
        assert "Unknown" in labels

    def test_invalid_dim_falls_back_to_sector(self):
        loans = [_make_loan(id=1, sector="Tech")]
        result = compute_credit_data_cuts(loans, primary_dim="nonexistent_dim")
        assert result["primary_dim"] == "sector"

    def test_cross_tab(self):
        loans = [
            _make_loan(id=1, sector="Tech", fund_name="Fund A", hold_size=10.0),
            _make_loan(id=2, sector="Tech", fund_name="Fund B", hold_size=20.0),
            _make_loan(id=3, sector="Health", fund_name="Fund A", hold_size=30.0),
        ]
        result = compute_credit_data_cuts(loans, primary_dim="sector", secondary_dim="fund_name")
        assert result["secondary_dim"] == "fund_name"
        assert result["cross_tab"] is not None
        assert "Fund A" in result["secondary_labels"]
        assert "Fund B" in result["secondary_labels"]
        tech_row = result["cross_tab"]["Tech"]
        assert tech_row["Fund A"] is not None
        assert tech_row["Fund B"] is not None

    def test_cross_tab_same_dim_ignored(self):
        loans = [_make_loan(id=1, sector="Tech")]
        result = compute_credit_data_cuts(loans, primary_dim="sector", secondary_dim="sector")
        assert result["secondary_dim"] is None
        assert result["cross_tab"] is None

    def test_totals_row(self):
        loans = [
            _make_loan(id=1, hold_size=10.0),
            _make_loan(id=2, hold_size=20.0),
        ]
        result = compute_credit_data_cuts(loans)
        assert result["totals"]["loan_count"] == 2
        assert result["totals"]["invested"] == pytest.approx(30.0, abs=0.01)

    def test_chart_datasets_present(self):
        loans = [_make_loan(id=1, sector="Tech", hold_size=10.0)]
        result = compute_credit_data_cuts(loans)
        assert "chart_labels" in result
        assert "chart_datasets" in result
        assert "weighted_moic" in result["chart_datasets"]
        assert "weighted_irr" in result["chart_datasets"]
        assert "invested" in result["chart_datasets"]
        assert "weighted_entry_coverage_ratio" in result["chart_datasets"]
        assert "weighted_entry_equity_cushion" in result["chart_datasets"]

    def test_data_quality_warning_when_many_unknown(self):
        loans = [
            *[_make_loan(id=i, sector=None, hold_size=10.0) for i in range(5)],
            _make_loan(id=99, sector="Tech", hold_size=10.0),
        ]
        result = compute_credit_data_cuts(loans, primary_dim="sector")
        assert result["data_quality_warning"] is not None
        assert result["data_quality_warning"]["pct"] == pytest.approx(5 / 6)

    def test_no_data_quality_warning_when_few_unknown(self):
        loans = [
            _make_loan(id=1, sector=None, hold_size=10.0),
            *[_make_loan(id=i, sector="Tech", hold_size=10.0) for i in range(2, 12)],
        ]
        result = compute_credit_data_cuts(loans, primary_dim="sector")
        assert result["data_quality_warning"] is None

    def test_fx_conversion(self):
        loans = [
            _make_loan(id=1, sector="Tech", hold_size=10.0, fx_rate_to_usd=2.0),
        ]
        result = compute_credit_data_cuts(loans, primary_dim="sector")
        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        assert tech["invested"] == pytest.approx(20.0, abs=0.01)

    def test_entry_underwriting_metrics_use_entry_fields_not_current_fields(self):
        loans = [
            _make_loan(
                id=1,
                sector="Software",
                hold_size=0.0,
                entry_loan_amount=10.0,
                current_invested_capital=80.0,
                entry_ltv=0.40,
                current_ltv=0.90,
                entry_coverage_ratio=2.0,
                current_coverage_ratio=0.5,
                entry_equity_cushion=0.35,
                current_equity_cushion=0.05,
            ),
            _make_loan(
                id=2,
                sector="Software",
                hold_size=0.0,
                entry_loan_amount=10.0,
                current_invested_capital=20.0,
                entry_ltv=0.60,
                current_ltv=0.10,
                entry_coverage_ratio=1.0,
                current_coverage_ratio=4.0,
                entry_equity_cushion=0.15,
                current_equity_cushion=0.80,
            ),
        ]

        result = compute_credit_data_cuts(loans, primary_dim="sector")
        group = next(g for g in result["groups"] if g["label"] == "Software")
        loan_row = group["loans"][0]

        assert group["weighted_ltv"] == pytest.approx(0.44, abs=0.001)
        assert group["weighted_entry_coverage_ratio"] == pytest.approx(1.8, abs=0.001)
        assert group["weighted_entry_equity_cushion"] == pytest.approx(0.31, abs=0.001)
        assert loan_row["entry_ltv"] in (0.40, 0.60)
        assert "entry_coverage_ratio" in loan_row
        assert "entry_equity_cushion" in loan_row

    def test_vintage_year_dimension(self):
        loans = [
            _make_loan(id=1, vintage_year=2020, hold_size=10.0),
            _make_loan(id=2, vintage_year=2021, hold_size=10.0),
            _make_loan(id=3, vintage_year=2020, hold_size=10.0),
        ]
        result = compute_credit_data_cuts(loans, primary_dim="vintage_year")
        labels = [g["label"] for g in result["groups"]]
        assert "2020" in labels
        assert "2021" in labels
        y2020 = next(g for g in result["groups"] if g["label"] == "2020")
        assert y2020["loan_count"] == 2

    def test_term_bucket_dimension_uses_exact_year_values_and_numeric_sort(self):
        loans = [
            _make_loan(id=1, term_years=10.0, hold_size=10.0),
            _make_loan(id=2, term_years=1.0, hold_size=10.0),
            _make_loan(id=3, term_years=2.5, hold_size=10.0),
            _make_loan(id=4, term_years=2.0, hold_size=10.0),
            _make_loan(id=5, term_years=0.0, hold_size=10.0),
        ]

        result = compute_credit_data_cuts(loans, primary_dim="term_bucket")

        assert result["primary_dim_label"] == "Loan Term (Years)"
        assert [g["label"] for g in result["groups"]] == [
            "1 Year",
            "2 Years",
            "2.5 Years",
            "10 Years",
            "Unknown",
        ]

    def test_empty_input(self):
        result = compute_credit_data_cuts([])
        assert result["loan_count"] == 0
        assert result["groups"] == []
        assert result["totals"]["loan_count"] == 0

    def test_all_dimensions_valid(self):
        """Each dimension key in the registry resolves without error."""
        loan = _make_loan(
            id=1,
            hold_size=10.0,
            fund_name="Fund I",
            sector="Software",
            geography="North America",
            sponsor="Apollo",
            instrument="Unitranche",
            tranche="First Lien",
            security_type="Senior Secured",
            status="Unrealized",
            vintage_year=2023,
            sourcing_channel="Direct",
            fixed_or_floating="Floating",
            term_years=5.0,
        )
        from services.metrics.credit import CREDIT_DIMENSIONS
        for dim_key in CREDIT_DIMENSIONS:
            result = compute_credit_data_cuts([loan], primary_dim=dim_key)
            assert result["primary_dim"] == dim_key
            assert result["loan_count"] == 1

    def test_default_status_removed_from_active_data_cuts_dimensions(self):
        from services.metrics.credit import CREDIT_DIMENSIONS
        assert "default_status" not in CREDIT_DIMENSIONS

    def test_allowed_metrics_in_return(self):
        loans = [_make_loan(id=1, hold_size=10.0)]
        result = compute_credit_data_cuts(loans)
        from services.metrics.credit import CREDIT_ALLOWED_METRICS
        for mk in CREDIT_ALLOWED_METRICS:
            assert mk in result["chart_datasets"]

    def test_dimension_dropdown_excludes_unknown_only_fields_and_primary_falls_back(self):
        loans = [
            _make_loan(
                id=1,
                sector="Software",
                sponsor=None,
                geography=None,
                hold_size=10.0,
            )
        ]

        result = compute_credit_data_cuts(loans, primary_dim="sponsor", secondary_dim="geography")

        assert result["primary_dim"] == "sector"
        assert result["secondary_dim"] is None
        assert "sector" in result["dimension_labels"]
        assert "sponsor" not in result["dimension_labels"]
        assert "geography" not in result["dimension_labels"]

    def test_available_dimension_helper_only_returns_dimensions_with_real_data(self):
        loans = [
            _make_loan(
                id=1,
                fund_name="Fund I",
                sector="Software",
                sponsor=None,
                geography=None,
                hold_size=10.0,
            )
        ]

        keys = credit_data_cuts_available_dimension_keys(loans)

        assert "fund_name" in keys
        assert "sector" in keys
        assert "sponsor" not in keys
        assert "geography" not in keys

    def test_credit_data_cuts_pdf_payload_skips_unknown_only_dimensions(self):
        from legacy_app import _credit_pdf_payload_for_page

        loans = [
            _make_loan(
                id=1,
                fund_name="Fund I",
                sector="Software",
                sponsor=None,
                geography=None,
                hold_size=10.0,
            )
        ]

        payload = _credit_pdf_payload_for_page(
            "credit-data-cuts",
            {
                "ctx": {
                    "loans": loans,
                    "metrics_by_id": {},
                    "snapshots_by_loan": {},
                    "fund_performance": {},
                },
                "membership": SimpleNamespace(team_id=1),
                "benchmark_asset_class": "",
            },
        )

        assert "fund_name" in payload["all_cuts"]
        assert "sector" in payload["all_cuts"]
        assert "sponsor" not in payload["all_cuts"]
        assert "geography" not in payload["all_cuts"]

    def test_pct_of_invested(self):
        loans = [
            _make_loan(id=1, sector="A", hold_size=25.0),
            _make_loan(id=2, sector="B", hold_size=75.0),
        ]
        result = compute_credit_data_cuts(loans, primary_dim="sector")
        a = next(g for g in result["groups"] if g["label"] == "A")
        b = next(g for g in result["groups"] if g["label"] == "B")
        assert a["pct_of_invested"] == pytest.approx(0.25, abs=0.01)
        assert b["pct_of_invested"] == pytest.approx(0.75, abs=0.01)

    def test_legacy_value_fields_flow_into_data_cuts(self):
        loan = _make_loan(
            id=1,
            sector="Software",
            hold_size=0.0,
            entry_loan_amount=20.0,
            current_invested_capital=20.0,
            realized_proceeds=None,
            realized_value=8.0,
            unrealized_loan_value=None,
            unrealized_value=16.0,
            unrealized_warrant_equity_value=2.0,
            total_value=26.0,
        )

        result = compute_credit_data_cuts([loan], primary_dim="sector")
        group = result["groups"][0]

        assert group["realized_value"] == pytest.approx(8.0)
        assert group["unrealized_value"] == pytest.approx(18.0)
        assert group["total_value"] == pytest.approx(26.0)
        assert group["weighted_moic"] == pytest.approx(1.3)

    def test_data_cuts_totals_reconcile_to_track_record_totals(self):
        loans = [
            _make_loan(
                id=1,
                sector="Software",
                hold_size=0.0,
                entry_loan_amount=20.0,
                current_invested_capital=20.0,
                realized_proceeds=5.0,
                realized_value=None,
                unrealized_loan_value=18.0,
                unrealized_value=None,
                unrealized_warrant_equity_value=1.0,
                total_value=24.0,
            ),
            _make_loan(
                id=2,
                sector="Healthcare",
                hold_size=0.0,
                entry_loan_amount=10.0,
                current_invested_capital=10.0,
                realized_proceeds=None,
                realized_value=3.0,
                unrealized_loan_value=None,
                unrealized_value=12.0,
                unrealized_warrant_equity_value=0.0,
                total_value=None,
            ),
        ]

        data_cuts = compute_credit_data_cuts(loans, primary_dim="sector")
        track_record = compute_credit_track_record(loans)
        track_totals = track_record["overall"]["totals"]

        assert data_cuts["totals"]["realized_value"] == pytest.approx(track_totals["realized_value"])
        assert data_cuts["totals"]["unrealized_value"] == pytest.approx(track_totals["unrealized_total_value"])
        assert data_cuts["totals"]["total_value"] == pytest.approx(track_totals["total_value"])

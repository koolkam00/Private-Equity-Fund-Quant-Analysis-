"""Tests for private credit metrics engine."""

import pytest
from datetime import date
from unittest.mock import MagicMock

from services.metrics.credit import (
    compute_credit_loan_metrics,
    compute_credit_portfolio_analytics,
    compute_credit_yield_attribution,
    compute_credit_stress_scenarios,
    compute_credit_concentration,
    compute_credit_vintage_comparison,
    compute_credit_maturity_profile,
    compute_traffic_lights,
    compute_top_concerns,
    _loan_traffic_light,
    _parse_loan_term,
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
        "equity_investment": None,
        "warrants_at_entry": None,
        "warrant_strike_entry": None,
        "warrants_current": None,
        "warrant_strike_current": None,
        "warrant_term": None,
        "ttm_revenue_entry": None,
        "ttm_revenue_current": None,
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
        assert m["total_return_mtm"] == pytest.approx(27.5 / 25.0 - 1.0, abs=0.01)
        assert m["warrant_upside"] is not None
        assert m["warrant_upside"] == pytest.approx(1.5 / 25.0, abs=0.01)
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
        loan = _make_loan(current_ltv=None, interest_coverage_ratio=None, default_status="Performing")
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
        """LP data: aggregation includes committed, IRR, MOIC, warrants."""
        loans = [
            _make_lp_loan(id=100, committed_amount=30.0, entry_loan_amount=25.0, gross_irr=0.12, moic=1.2),
            _make_lp_loan(id=101, committed_amount=20.0, entry_loan_amount=15.0, gross_irr=0.10, moic=1.1),
        ]
        result = compute_credit_portfolio_analytics(loans)
        assert result["has_new_fields"] is True
        assert result["total_committed"] == pytest.approx(50.0, abs=0.1)
        assert result["total_entry_loan"] == pytest.approx(40.0, abs=0.1)
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


# ---------------------------------------------------------------------------
# Stress scenarios
# ---------------------------------------------------------------------------


class TestStressScenarios:
    def test_stress_rate_shock_floating(self):
        loan = _make_loan(fixed_or_floating="Floating")
        scenario = {"default_rate_shock": 0, "recovery_rate_shock": 0.40, "rate_shock_bps": 200}
        result = compute_credit_stress_scenarios([loan], scenario)
        assert result["base_nav"] > 0

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
# Concentration
# ---------------------------------------------------------------------------


class TestConcentration:
    def test_concentration_hhi(self):
        loans = [
            _make_loan(id=1, sector="Software", hold_size=50),
            _make_loan(id=2, sector="Healthcare", hold_size=30),
            _make_loan(id=3, sector="Software", hold_size=20),
        ]
        result = compute_credit_concentration(loans)
        assert result["hhi_sector"] > 0
        assert result["by_sector"][0]["name"] == "Software"
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

    def test_concentration_entry_loan_fallback(self):
        """LP data: uses entry_loan_amount when hold_size is None."""
        loans = [
            _make_lp_loan(id=100, hold_size=None, entry_loan_amount=25.0),
            _make_lp_loan(id=101, hold_size=None, entry_loan_amount=15.0),
        ]
        result = compute_credit_concentration(loans)
        assert result["total_hold"] == pytest.approx(40.0, abs=0.1)


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

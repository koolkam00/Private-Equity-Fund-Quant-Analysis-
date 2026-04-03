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

"""Unit tests for data cuts analytics — math correctness and edge cases."""

from datetime import date
from types import SimpleNamespace

from services.metrics.deal import compute_deal_metrics
from services.metrics.data_cuts import compute_data_cuts_analytics, _validate_dim


def _make_deal(**kwargs):
    defaults = {
        "id": 1,
        "company_name": "Test Co",
        "fund_number": "Fund I",
        "sector": "Tech",
        "geography": "US",
        "status": "Fully Realized",
        "lead_partner": "Partner A",
        "deal_type": "Platform",
        "exit_type": "Trade Sale",
        "entry_channel": "Proprietary",
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
        "irr": 0.25,
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


class TestWeightedMoic:
    def test_single_deal_group(self):
        deal = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=300, unrealized_value=0)
        m = compute_deal_metrics(deal)
        result = compute_data_cuts_analytics([deal], {deal.id: m}, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        assert tech["deal_count"] == 1
        assert abs(tech["weighted_moic"] - 3.0) < 1e-6
        assert tech["small_n"] is True  # N < 3

    def test_weighted_moic_two_deals(self):
        """Weighted MOIC = total_value / invested_equity across deals."""
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=300, unrealized_value=0)
        d2 = _make_deal(id=2, sector="Tech", equity_invested=200, realized_value=400, unrealized_value=0)
        m1 = compute_deal_metrics(d1)
        m2 = compute_deal_metrics(d2)
        result = compute_data_cuts_analytics([d1, d2], {1: m1, 2: m2}, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        # Weighted MOIC = (300 + 400) / (100 + 200) = 700 / 300 = 2.3333...
        assert abs(tech["weighted_moic"] - 700 / 300) < 1e-4
        assert tech["deal_count"] == 2
        assert tech["invested_equity"] == 300

    def test_weighted_moic_multiple_sectors(self):
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=250, unrealized_value=0)
        d2 = _make_deal(id=2, sector="Healthcare", equity_invested=200, realized_value=300, unrealized_value=0)
        d3 = _make_deal(id=3, sector="Tech", equity_invested=50, realized_value=200, unrealized_value=0)
        deals = [d1, d2, d3]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        hc = next(g for g in result["groups"] if g["label"] == "Healthcare")

        # Tech: (250 + 200) / (100 + 50) = 450 / 150 = 3.0
        assert abs(tech["weighted_moic"] - 3.0) < 1e-4
        # Healthcare: 300 / 200 = 1.5
        assert abs(hc["weighted_moic"] - 1.5) < 1e-4

    def test_totals_match(self):
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=300, unrealized_value=0)
        d2 = _make_deal(id=2, sector="Healthcare", equity_invested=200, realized_value=500, unrealized_value=0)
        deals = [d1, d2]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        assert result["totals"]["deal_count"] == 2
        assert result["totals"]["invested_equity"] == 300
        assert abs(result["totals"]["weighted_moic"] - 800 / 300) < 1e-4


class TestWeightedIrr:
    def test_weighted_irr_equity_weighted(self):
        """Weighted IRR = sum(irr * equity) / sum(equity) for deals with IRR."""
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, irr=0.20)
        d2 = _make_deal(id=2, sector="Tech", equity_invested=300, irr=0.10)
        deals = [d1, d2]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        # Weighted IRR = (0.20*100 + 0.10*300) / (100+300) = (20+30)/400 = 0.125
        assert abs(tech["weighted_irr"] - 0.125) < 1e-6

    def test_all_deals_missing_irr(self):
        d1 = _make_deal(id=1, sector="Tech", irr=None)
        d2 = _make_deal(id=2, sector="Tech", irr=None)
        deals = [d1, d2]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        assert tech["weighted_irr"] is None


class TestLossRatio:
    def test_loss_ratio_count(self):
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=50, unrealized_value=0)  # MOIC < 1
        d2 = _make_deal(id=2, sector="Tech", equity_invested=100, realized_value=200, unrealized_value=0)  # MOIC = 2
        d3 = _make_deal(id=3, sector="Tech", equity_invested=100, realized_value=80, unrealized_value=0)  # MOIC < 1
        deals = [d1, d2, d3]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        # 2 out of 3 deals have MOIC < 1
        assert abs(tech["loss_ratio_count"] - 2 / 3) < 1e-6


class TestCrossTab:
    def test_cross_tab_structure(self):
        d1 = _make_deal(id=1, sector="Tech", geography="US", equity_invested=100, realized_value=200, unrealized_value=0)
        d2 = _make_deal(id=2, sector="Tech", geography="Europe", equity_invested=100, realized_value=300, unrealized_value=0)
        d3 = _make_deal(id=3, sector="Healthcare", geography="US", equity_invested=200, realized_value=500, unrealized_value=0)
        deals = [d1, d2, d3]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector", secondary_dim="geography")

        assert result["cross_tab"] is not None
        assert "US" in result["secondary_labels"]
        assert "Europe" in result["secondary_labels"]

        # Tech row should have US and Europe cells
        tech_row = result["cross_tab"]["Tech"]
        assert tech_row["US"] is not None
        assert tech_row["US"]["deal_count"] == 1
        assert tech_row["Europe"] is not None
        assert tech_row["Europe"]["deal_count"] == 1

    def test_dim2_equals_dim_treated_as_none(self):
        """Degenerate case: same dimension on both axes produces no cross-tab."""
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=200, unrealized_value=0)
        m = compute_deal_metrics(d1)
        result = compute_data_cuts_analytics([d1], {1: m}, primary_dim="sector", secondary_dim="sector")

        assert result["cross_tab"] is None
        assert result["secondary_dim"] is None


class TestDimValidation:
    def test_invalid_dim_falls_back(self):
        assert _validate_dim("invalid_xyz") == "sector"
        assert _validate_dim("") == "sector"
        assert _validate_dim(None) == "sector"

    def test_valid_dim_accepted(self):
        assert _validate_dim("geography") == "geography"
        assert _validate_dim("lead_partner") == "lead_partner"

    def test_case_insensitive(self):
        assert _validate_dim("SECTOR") == "sector"
        assert _validate_dim("Geography") == "geography"


class TestNilDimValues:
    def test_nil_sector_goes_to_unknown(self):
        d1 = _make_deal(id=1, sector=None, equity_invested=100, realized_value=200, unrealized_value=0)
        m = compute_deal_metrics(d1)
        result = compute_data_cuts_analytics([d1], {1: m}, primary_dim="sector")

        labels = [g["label"] for g in result["groups"]]
        assert "Unknown" in labels

    def test_nil_lead_partner_goes_to_unassigned(self):
        d1 = _make_deal(id=1, lead_partner=None, equity_invested=100, realized_value=200, unrealized_value=0)
        m = compute_deal_metrics(d1)
        result = compute_data_cuts_analytics([d1], {1: m}, primary_dim="lead_partner")

        labels = [g["label"] for g in result["groups"]]
        assert "Unassigned" in labels


class TestEmptyDeals:
    def test_empty_deals_returns_empty_groups(self):
        result = compute_data_cuts_analytics([], {}, primary_dim="sector")
        assert result["groups"] == []
        assert result["totals"]["deal_count"] == 0
        assert result["deal_count"] == 0


class TestDrilldown:
    def test_deals_sorted_by_equity_desc(self):
        d1 = _make_deal(id=1, sector="Tech", equity_invested=50, realized_value=100, unrealized_value=0)
        d2 = _make_deal(id=2, sector="Tech", equity_invested=200, realized_value=400, unrealized_value=0)
        d3 = _make_deal(id=3, sector="Tech", equity_invested=100, realized_value=150, unrealized_value=0)
        deals = [d1, d2, d3]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        equities = [d["equity_invested"] for d in tech["deals"]]
        assert equities == [200, 100, 50]


class TestVintageYear:
    def test_vintage_from_year_invested(self):
        d1 = _make_deal(id=1, year_invested=2019, investment_date=date(2020, 6, 1))
        m = compute_deal_metrics(d1)
        result = compute_data_cuts_analytics([d1], {1: m}, primary_dim="vintage_year")
        labels = [g["label"] for g in result["groups"]]
        assert "2019" in labels  # year_invested takes precedence

    def test_vintage_from_investment_date_fallback(self):
        d1 = _make_deal(id=1, year_invested=None, investment_date=date(2021, 3, 15))
        m = compute_deal_metrics(d1)
        result = compute_data_cuts_analytics([d1], {1: m}, primary_dim="vintage_year")
        labels = [g["label"] for g in result["groups"]]
        assert "2021" in labels

    def test_vintage_string_date_no_crash(self):
        """Legacy data with string date should not crash the vintage resolver."""
        d1 = _make_deal(id=1, year_invested=None, investment_date="2020-01-01")
        # Build a mock metrics dict directly (compute_deal_metrics crashes on string dates,
        # but our resolver should handle it gracefully)
        mock_metrics = {
            "equity": 100, "value_total": 200, "value_created": 100,
            "moic": 2.0, "gross_irr": 0.20, "hold_period": 3.0,
            "entry_tev_ebitda": 10.0, "exit_tev_ebitda": 15.0,
            "entry_tev_revenue": 4.0, "exit_tev_revenue": 4.0,
        }
        result = compute_data_cuts_analytics([d1], {1: mock_metrics}, primary_dim="vintage_year")
        labels = [g["label"] for g in result["groups"]]
        assert "Unknown" in labels


class TestChartPayload:
    def test_chart_datasets_contain_all_metrics(self):
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=200, unrealized_value=0)
        m = compute_deal_metrics(d1)
        result = compute_data_cuts_analytics([d1], {1: m}, primary_dim="sector")

        assert "weighted_moic" in result["chart_datasets"]
        assert "invested_equity" in result["chart_datasets"]
        assert "value_created" in result["chart_datasets"]
        assert len(result["chart_labels"]) == 1
        assert result["chart_labels"][0] == "Tech"


class TestDataQualityWarning:
    def test_warning_when_unknown_exceeds_20pct(self):
        deals = [
            _make_deal(id=1, sector="Tech"),
            _make_deal(id=2, sector=None),
            _make_deal(id=3, sector=None),
        ]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        assert result["data_quality_warning"] is not None
        assert result["data_quality_warning"]["count"] == 2

    def test_no_warning_when_below_threshold(self):
        deals = [
            _make_deal(id=i, sector="Tech") for i in range(1, 11)
        ]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        assert result["data_quality_warning"] is None


class TestPctOfPortfolio:
    def test_pct_of_invested(self):
        """Group with half the portfolio equity → pct_of_invested = 0.5."""
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=200, unrealized_value=0)
        d2 = _make_deal(id=2, sector="Healthcare", equity_invested=100, realized_value=300, unrealized_value=0)
        deals = [d1, d2]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        hc = next(g for g in result["groups"] if g["label"] == "Healthcare")
        assert abs(tech["pct_of_invested"] - 0.5) < 1e-6
        assert abs(hc["pct_of_invested"] - 0.5) < 1e-6

    def test_pct_of_total_value(self):
        """Group total value as fraction of portfolio total value."""
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=300, unrealized_value=0)
        d2 = _make_deal(id=2, sector="Healthcare", equity_invested=100, realized_value=100, unrealized_value=0)
        deals = [d1, d2]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        # Tech total_value=300, portfolio total_value=400, pct = 0.75
        assert abs(tech["pct_of_total"] - 0.75) < 1e-6

    def test_pct_totals_row_is_one(self):
        """Totals row pct_of_invested should be 1.0 (100%)."""
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=200, unrealized_value=0)
        deals = [d1]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        assert abs(result["totals"]["pct_of_invested"] - 1.0) < 1e-6
        assert abs(result["totals"]["pct_of_total"] - 1.0) < 1e-6

    def test_empty_deals_no_crash(self):
        """Empty portfolio → pct fields are None, no ZeroDivisionError."""
        result = compute_data_cuts_analytics([], {}, primary_dim="sector")
        assert result["totals"]["pct_of_invested"] is None
        assert result["totals"]["pct_of_total"] is None

    def test_loss_ratio_capital_populated(self):
        """Losing deal → loss_ratio_capital is populated."""
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=50, unrealized_value=0)
        deals = [d1]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        tech = next(g for g in result["groups"] if g["label"] == "Tech")
        assert tech["loss_ratio_capital"] is not None
        assert tech["loss_ratio_capital"] > 0

    def test_chart_datasets_include_new_metrics(self):
        """Chart datasets include pct_of_invested, pct_of_total, loss_ratio_capital."""
        d1 = _make_deal(id=1, sector="Tech", equity_invested=100, realized_value=200, unrealized_value=0)
        deals = [d1]
        metrics = {d.id: compute_deal_metrics(d) for d in deals}
        result = compute_data_cuts_analytics(deals, metrics, primary_dim="sector")

        assert "pct_of_invested" in result["chart_datasets"]
        assert "pct_of_total" in result["chart_datasets"]
        assert "loss_ratio_capital" in result["chart_datasets"]

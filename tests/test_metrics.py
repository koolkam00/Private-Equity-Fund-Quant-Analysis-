from datetime import date
from types import SimpleNamespace

from app import _rank_benchmark_metric
from models import (
    Deal,
    DealCashflowEvent,
    DealQuarterSnapshot,
    DealUnderwriteBaseline,
    FundQuarterSnapshot,
    db,
)
from services.metrics import (
    build_methodology_payload,
    compute_bridge_aggregate,
    compute_bridge_view,
    compute_deals_rollup_details,
    compute_deal_trajectory_analysis,
    compute_deal_metrics,
    compute_ic_memo_payload,
    compute_deal_track_record,
    compute_data_quality,
    compute_exit_readiness_analysis,
    compute_exit_type_performance,
    compute_fund_liquidity_analysis,
    compute_loss_and_distribution,
    compute_portfolio_analytics,
    compute_stress_lab_analysis,
    compute_underwrite_outcome_analysis,
    compute_valuation_quality_analysis,
    compute_value_creation_mix,
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
        "lead_partner": "Unassigned",
        "deal_type": "Platform",
        "entry_channel": "Unknown",
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
        "fund_size": None,
        "net_irr": None,
        "net_moic": None,
        "net_dpi": None,
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


def test_benchmark_threshold_boundaries_are_inclusive():
    thresholds = {
        2019: {
            "net_irr": {
                "lower_quartile": 0.10,
                "median": 0.15,
                "upper_quartile": 0.20,
                "top_5": 0.30,
            }
        }
    }

    assert _rank_benchmark_metric(0.30, 2019, "net_irr", thresholds, "Buyout")["rank_code"] == "top5"
    assert _rank_benchmark_metric(0.20, 2019, "net_irr", thresholds, "Buyout")["rank_code"] == "q1"
    assert _rank_benchmark_metric(0.15, 2019, "net_irr", thresholds, "Buyout")["rank_code"] == "q2"
    assert _rank_benchmark_metric(0.10, 2019, "net_irr", thresholds, "Buyout")["rank_code"] == "q3"
    assert _rank_benchmark_metric(0.09, 2019, "net_irr", thresholds, "Buyout")["rank_code"] == "q4"


def test_benchmark_ranking_handles_missing_top5():
    thresholds = {
        2019: {
            "net_moic": {
                "lower_quartile": 1.3,
                "median": 1.7,
                "upper_quartile": 2.1,
            }
        }
    }
    rank = _rank_benchmark_metric(2.2, 2019, "net_moic", thresholds, "Buyout")
    assert rank["rank_code"] == "q1"
    assert rank["label"] == "1st Quartile"


def test_benchmark_ranking_returns_na_when_thresholds_missing():
    thresholds = {
        2020: {
            "net_dpi": {
                "median": 1.0,
            }
        }
    }
    rank = _rank_benchmark_metric(1.1, 2020, "net_dpi", thresholds, "Buyout")
    assert rank["rank_code"] == "na"
    assert rank["reason"] == "missing_thresholds"


def test_benchmark_ranking_returns_na_when_metric_missing_or_no_asset_selected():
    thresholds = {
        2021: {
            "net_irr": {
                "lower_quartile": 0.10,
                "median": 0.15,
                "upper_quartile": 0.20,
                "top_5": 0.27,
            }
        }
    }
    no_metric = _rank_benchmark_metric(None, 2021, "net_irr", thresholds, "Buyout")
    assert no_metric["rank_code"] == "na"
    assert no_metric["reason"] == "missing_metric"

    no_asset = _rank_benchmark_metric(0.2, 2021, "net_irr", thresholds, "")
    assert no_asset["rank_code"] == "na"
    assert no_asset["reason"] == "no_asset_class_selected"


def test_deal_metrics_core_identity():
    deal = _make_deal()
    m = compute_deal_metrics(deal)
    assert abs(m["moic"] - 2.0) < 1e-9
    assert abs(m["value_created"] - 100.0) < 1e-9
    assert m["implied_irr"] is not None


def test_ebitda_growth_handles_negative_entry_base():
    deal = _make_deal(
        id=11,
        entry_ebitda=-10,
        exit_ebitda=-5,
        investment_date=date(2020, 1, 1),
        exit_date=date(2022, 1, 1),
    )
    m = compute_deal_metrics(deal)
    assert abs(m["ebitda_growth"] - 50.0) < 1e-9


def test_ebitda_cagr_handles_negative_to_negative_paths():
    deal = _make_deal(
        id=12,
        entry_ebitda=-10,
        exit_ebitda=-5,
        investment_date=date(2020, 1, 1),
        exit_date=date(2022, 1, 1),
    )
    m = compute_deal_metrics(deal)
    assert m["ebitda_cagr"] is not None
    expected = ((abs(deal.entry_ebitda) / abs(deal.exit_ebitda)) ** (1.0 / m["hold_period"]) - 1.0) * 100.0
    assert abs(m["ebitda_cagr"] - expected) < 1e-9


def test_ebitda_cagr_is_none_on_sign_flip():
    deal = _make_deal(
        id=13,
        entry_ebitda=-10,
        exit_ebitda=5,
        investment_date=date(2020, 1, 1),
        exit_date=date(2022, 1, 1),
    )
    m = compute_deal_metrics(deal)
    assert m["ebitda_growth"] is not None
    assert m["ebitda_cagr"] is None


def test_additive_bridge_reconciles_exactly():
    deal = _make_deal()
    b = compute_bridge_view(deal, model="additive", basis="fund", unit="dollar", warnings=[])
    assert b["ready"] is True
    drivers = b["drivers_dollar"]
    total = drivers["revenue"] + drivers["margin"] + drivers["multiple"] + drivers["leverage"] + drivers["other"]
    expected = (deal.realized_value + deal.unrealized_value - deal.equity_invested)
    assert abs(total - expected) < 1e-6


def test_additive_bridge_treats_missing_realized_unrealized_as_zero():
    deal = _make_deal(id=14, realized_value=None, unrealized_value=None)
    b = compute_bridge_view(deal, model="additive", basis="fund", unit="dollar", warnings=[])
    assert b["ready"] is True
    assert abs(b["fund_value_created"] - (-deal.equity_invested)) < 1e-9
    subtotal = sum((b["drivers_dollar"].get(k) or 0.0) for k in ("revenue", "margin", "multiple", "leverage", "other"))
    assert abs(subtotal - b["fund_value_created"]) < 1e-9


def test_bridge_rejects_non_additive_model():
    deal = _make_deal()
    try:
        compute_bridge_view(deal, model="multiplicative", basis="fund", unit="dollar", warnings=[])
        assert False, "Expected ValueError for non-additive model"
    except ValueError:
        assert True


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
    assert abs(p["entry"]["ebitda"]["wavg"] - 25.0) < 1e-9
    assert abs(p["entry"]["tev"]["wavg"] - 475.0) < 1e-9


def test_negative_tev_multiples_are_treated_as_unavailable():
    deal = _make_deal(id=20, entry_ebitda=-10, exit_ebitda=-5)
    metrics = compute_deal_metrics(deal)
    assert metrics["entry_tev_ebitda"] is None
    assert metrics["exit_tev_ebitda"] is None
    assert metrics["bridge_ready"] is True
    bridge = metrics["bridge_additive_fund"]
    assert bridge["calculation_method"] == "revenue_multiple_fallback"
    assert bridge["fallback_reason"] == "negative_ebitda"
    assert bridge["drivers_dollar"]["margin"] == 0.0
    subtotal = sum((bridge["drivers_dollar"].get(k) or 0.0) for k in ("revenue", "margin", "multiple", "leverage", "other"))
    assert abs(subtotal - metrics["value_created"]) < 1e-9
    assert any("TEV/EBITDA" in w for w in metrics["_warnings"])


def test_negative_tev_ebitda_is_excluded_from_multiple_aggregates():
    d1 = _make_deal(id=21, equity_invested=100, entry_enterprise_value=100, entry_ebitda=10, entry_revenue=10)
    d2 = _make_deal(id=22, equity_invested=300, entry_enterprise_value=600, entry_ebitda=-30, entry_revenue=-30)
    metrics = {21: compute_deal_metrics(d1), 22: compute_deal_metrics(d2)}
    out = compute_portfolio_analytics([d1, d2], metrics_by_id=metrics)
    assert abs(out["entry"]["tev_ebitda"]["avg"] - 10.0) < 1e-9
    assert abs(out["entry"]["tev_ebitda"]["wavg"] - 10.0) < 1e-9
    assert abs(out["entry"]["tev_revenue"]["avg"] - (-5.0)) < 1e-9
    assert abs(out["entry"]["tev_revenue"]["wavg"] - (-12.5)) < 1e-9


def test_signed_leverage_ratios_are_preserved():
    deal = _make_deal(id=23, entry_enterprise_value=100, entry_net_debt=-20, exit_enterprise_value=110, exit_net_debt=-10)
    metrics = compute_deal_metrics(deal)
    assert metrics["entry_net_debt_tev"] is not None
    assert metrics["exit_net_debt_tev"] is not None
    assert metrics["entry_net_debt_tev"] < 0
    assert metrics["exit_net_debt_tev"] < 0


def test_bridge_aggregate_tracks_fallback_ready_count():
    d1 = _make_deal(id=24)
    d2 = _make_deal(id=25, entry_ebitda=-8, exit_ebitda=-4)
    agg = compute_bridge_aggregate([d1, d2], basis="fund")
    assert agg["ready_count"] == 2
    assert agg["fallback_ready_count"] == 1


def test_bridge_fallback_when_ebitda_is_missing():
    deal = _make_deal(id=29, entry_ebitda=None, exit_ebitda=None)
    metrics = compute_deal_metrics(deal)
    assert metrics["bridge_ready"] is True
    bridge = metrics["bridge_additive_fund"]
    assert bridge["calculation_method"] == "revenue_multiple_fallback"
    assert bridge["fallback_reason"] == "negative_ebitda"


def test_bridge_fallback_when_ebitda_is_zero():
    deal = _make_deal(id=30, entry_ebitda=0.0, exit_ebitda=0.0)
    metrics = compute_deal_metrics(deal)
    assert metrics["bridge_ready"] is True
    bridge = metrics["bridge_additive_fund"]
    assert bridge["calculation_method"] == "revenue_multiple_fallback"
    assert bridge["fallback_reason"] == "negative_ebitda"


def test_bridge_fallback_when_revenue_is_zero_but_ebitda_is_available():
    deal = _make_deal(id=31, entry_revenue=0.0, exit_revenue=0.0, entry_ebitda=10.0, exit_ebitda=20.0)
    metrics = compute_deal_metrics(deal)
    assert metrics["bridge_ready"] is True
    bridge = metrics["bridge_additive_fund"]
    assert bridge["calculation_method"] == "ebitda_multiple_fallback"
    assert bridge["fallback_reason"] == "missing_revenue"
    assert bridge["display_drivers"][0]["key"] == "ebitda_growth"
    assert all(row["key"] != "margin" for row in bridge["display_drivers"])
    assert bridge["drivers_dollar"]["revenue"] == bridge["display_drivers"][0]["dollar"]
    assert bridge["drivers_dollar"]["margin"] == 0.0
    subtotal = sum((row.get("dollar") or 0.0) for row in bridge["display_drivers"])
    assert abs(subtotal - metrics["value_created"]) < 1e-9


def test_bridge_missing_revenue_fallback_requires_both_revenues_missing_or_zero():
    deal = _make_deal(id=34, entry_revenue=0.0, exit_revenue=120.0, entry_ebitda=10.0, exit_ebitda=20.0)
    metrics = compute_deal_metrics(deal)
    assert metrics["bridge_ready"] is False
    bridge = metrics["bridge_additive_fund"]
    assert bridge["calculation_method"] is None
    assert any("Partial revenue history" in warning for warning in metrics["_warnings"])


def test_bridge_aggregate_counts_both_fallback_methods():
    d1 = _make_deal(id=32, entry_ebitda=-8.0, exit_ebitda=-4.0)
    d2 = _make_deal(id=33, entry_revenue=None, exit_revenue=None, entry_ebitda=12.0, exit_ebitda=18.0)
    agg = compute_bridge_aggregate([d1, d2], basis="fund")
    assert agg["ready_count"] == 2
    assert agg["fallback_ready_count"] == 2
    display_keys = {row["key"] for row in agg["display_drivers"]}
    assert "revenue" in display_keys
    assert "ebitda_growth" in display_keys


def test_data_quality_bridge_ready_uses_computed_metric_flag():
    d1 = _make_deal(id=26)
    d2 = _make_deal(id=27, entry_ebitda=-10, exit_ebitda=-5)
    d3 = _make_deal(id=28, entry_enterprise_value=-100, exit_enterprise_value=130, entry_ebitda=10, exit_ebitda=12)
    metrics = {d.id: compute_deal_metrics(d) for d in (d1, d2, d3)}
    quality = compute_data_quality([d1, d2, d3], metrics)
    assert quality["bridge_ready"] == 2


def test_bridge_aggregate_outputs_three_units():
    deals = [_make_deal(id=1), _make_deal(id=2, equity_invested=200, realized_value=420, unrealized_value=0)]
    agg = compute_bridge_aggregate(deals, basis="fund")
    assert "dollar" in agg["drivers"]
    assert "moic" in agg["drivers"]
    assert "pct" in agg["drivers"]


def test_value_creation_mix_grouping_variants():
    deals = [
        _make_deal(id=1, fund_number="Fund I", sector="Tech", exit_type="Strategic Sale"),
        _make_deal(id=2, fund_number="Fund II", sector="Healthcare", exit_type="Secondary Buyout"),
    ]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}

    by_fund = compute_value_creation_mix(deals, metrics_by_id=metrics, group_by="fund")
    by_sector = compute_value_creation_mix(deals, metrics_by_id=metrics, group_by="sector")
    by_exit = compute_value_creation_mix(deals, metrics_by_id=metrics, group_by="exit_type")

    assert sorted(by_fund["labels"]) == ["Fund I", "Fund II"]
    assert sorted(by_sector["labels"]) == ["Healthcare", "Tech"]
    assert sorted(by_exit["labels"]) == ["Secondary Buyout", "Strategic Sale"]


def test_exit_type_performance_uses_calculated_moic():
    deals = [
        _make_deal(id=1, exit_type="Strategic Sale", equity_invested=100, realized_value=150, unrealized_value=0),
        _make_deal(id=2, exit_type="Strategic Sale", equity_invested=300, realized_value=300, unrealized_value=0),
    ]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}
    out = compute_exit_type_performance(deals, metrics_by_id=metrics)

    assert out["labels"] == ["Strategic Sale"]
    # Calculated MOIC = total value / total equity = 450 / 400 = 1.125x
    assert abs(out["calculated_moic"][0] - 1.125) < 1e-9


def test_deal_track_record_groups_and_subtotals():
    deals = [
        _make_deal(
            id=1,
            fund_number="Fund I",
            company_name="Alpha",
            status="Fully Realized",
            equity_invested=100,
            realized_value=170,
            unrealized_value=0,
        ),
        _make_deal(
            id=2,
            fund_number="Fund I",
            company_name="Beta",
            status="Unrealized",
            equity_invested=50,
            realized_value=0,
            unrealized_value=70,
        ),
        _make_deal(
            id=3,
            fund_number="Fund II",
            company_name="Gamma",
            status="Partially Realized",
            equity_invested=80,
            realized_value=60,
            unrealized_value=40,
        ),
    ]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}
    out = compute_deal_track_record(deals, metrics_by_id=metrics)

    assert len(out["funds"]) == 2
    assert out["funds"][0]["fund_name"] == "Fund I"
    assert out["funds"][0]["totals"]["deal_count"] == 2
    assert abs(out["funds"][0]["totals"]["moic"] - 1.6) < 1e-9  # (170 + 70) / 150

    overall = out["overall"]["totals"]
    assert overall["deal_count"] == 3
    assert abs(overall["invested_equity"] - 230.0) < 1e-9
    assert abs(overall["total_value"] - 340.0) < 1e-9
    assert abs(overall["moic"] - (340.0 / 230.0)) < 1e-9


def test_deal_track_record_rows_ordered_by_status_then_investment_date():
    deals = [
        _make_deal(
            id=1,
            fund_number="Fund I",
            company_name="Unrealized Later",
            status="Unrealized",
            investment_date=date(2021, 1, 1),
            equity_invested=100,
            realized_value=0,
            unrealized_value=120,
        ),
        _make_deal(
            id=2,
            fund_number="Fund I",
            company_name="Fully Earlier",
            status="Fully Realized",
            investment_date=date(2018, 1, 1),
            equity_invested=90,
            realized_value=130,
            unrealized_value=0,
        ),
        _make_deal(
            id=3,
            fund_number="Fund I",
            company_name="Partially Earlier",
            status="Partially Realized",
            investment_date=date(2017, 6, 1),
            equity_invested=80,
            realized_value=50,
            unrealized_value=35,
        ),
        _make_deal(
            id=4,
            fund_number="Fund I",
            company_name="Fully Later",
            status="Fully Realized",
            investment_date=date(2020, 1, 1),
            equity_invested=70,
            realized_value=90,
            unrealized_value=0,
        ),
    ]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}
    out = compute_deal_track_record(deals, metrics_by_id=metrics)

    rows = out["funds"][0]["rows"]
    ordered = [(r["status"], r["company_name"]) for r in rows]
    assert ordered == [
        ("Fully Realized", "Fully Earlier"),
        ("Fully Realized", "Fully Later"),
        ("Partially Realized", "Partially Earlier"),
        ("Unrealized", "Unrealized Later"),
    ]


def test_track_record_gross_irr_prefers_uploaded_irr():
    deal = _make_deal(id=1, company_name="Uploaded IRR Co", irr=0.25, equity_invested=100, realized_value=140, unrealized_value=0)
    metrics = {deal.id: compute_deal_metrics(deal)}
    out = compute_deal_track_record([deal], metrics_by_id=metrics)
    row = out["funds"][0]["rows"][0]
    assert abs(row["gross_irr"] - 0.25) < 1e-9


def test_track_record_gross_irr_uses_uploaded_only():
    deal = _make_deal(id=1, company_name="No Uploaded Gross IRR Co", irr=None, equity_invested=100, realized_value=170, unrealized_value=0)
    metrics = {deal.id: compute_deal_metrics(deal)}
    out = compute_deal_track_record([deal], metrics_by_id=metrics)
    row = out["funds"][0]["rows"][0]
    assert row["gross_irr"] is None


def test_track_record_percent_columns_use_fund_invested_and_fund_size():
    deals = [
        _make_deal(id=1, company_name="Alpha", fund_number="Fund I", equity_invested=40, realized_value=60, unrealized_value=0, fund_size=200),
        _make_deal(id=2, company_name="Beta", fund_number="Fund I", equity_invested=60, realized_value=90, unrealized_value=0, fund_size=200),
    ]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}
    out = compute_deal_track_record(deals, metrics_by_id=metrics)
    rows = {row["company_name"]: row for row in out["funds"][0]["rows"]}
    assert abs(rows["Alpha"]["pct_total_invested"] - 0.4) < 1e-9
    assert abs(rows["Beta"]["pct_total_invested"] - 0.6) < 1e-9
    assert abs(rows["Alpha"]["pct_fund_size"] - 0.2) < 1e-9
    assert abs(rows["Beta"]["pct_fund_size"] - 0.3) < 1e-9


def test_track_record_rollups_and_net_conflict():
    deals = [
        _make_deal(
            id=1,
            company_name="Alpha",
            fund_number="Fund X",
            status="Fully Realized",
            equity_invested=100,
            realized_value=150,
            unrealized_value=0,
            fund_size=500,
            net_irr=0.18,
            net_moic=1.9,
            net_dpi=1.2,
        ),
        _make_deal(
            id=2,
            company_name="Beta",
            fund_number="Fund X",
            status="Partially Realized",
            equity_invested=50,
            realized_value=20,
            unrealized_value=50,
            fund_size=500,
            net_irr=0.22,  # conflict vs 0.18 above
            net_moic=1.9,
            net_dpi=1.2,
        ),
    ]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}
    out = compute_deal_track_record(deals, metrics_by_id=metrics)
    fund = out["funds"][0]

    all_rollup = fund["summary_rollups"][-1]["totals"]
    assert abs(all_rollup["gross_moic"] - (220.0 / 150.0)) < 1e-9
    assert abs(all_rollup["realized_gross_moic"] - (170.0 / 150.0)) < 1e-9
    assert abs(all_rollup["unrealized_gross_moic"] - (50.0 / 150.0)) < 1e-9
    assert fund["net_performance"]["conflicts"]["net_irr"] is True
    assert fund["net_performance"]["net_irr"] is None
    assert abs(fund["net_performance"]["net_moic"] - 1.9) < 1e-9


def test_compute_deals_rollup_details_keys_and_subset_math():
    deals = [
        _make_deal(
            id=1,
            company_name="Alpha",
            fund_number="Fund I",
            status="Fully Realized",
            equity_invested=100,
            realized_value=160,
            unrealized_value=0,
        ),
        _make_deal(
            id=2,
            company_name="Beta",
            fund_number="Fund I",
            status="Unrealized",
            equity_invested=50,
            realized_value=0,
            unrealized_value=65,
            exit_date=None,
        ),
        _make_deal(
            id=3,
            company_name="Gamma",
            fund_number="Fund II",
            status="Partially Realized",
            equity_invested=80,
            realized_value=60,
            unrealized_value=35,
        ),
    ]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}
    track_record = compute_deal_track_record(deals, metrics_by_id=metrics)

    details = compute_deals_rollup_details(deals, track_record, metrics_by_id=metrics)
    details_repeat = compute_deals_rollup_details(deals, track_record, metrics_by_id=metrics)

    assert details
    assert set(details.keys()) == set(details_repeat.keys())
    assert len(details.keys()) == len(set(details.keys()))
    assert any(key.startswith("fund_fund_i__status_") for key in details.keys())
    assert any(key.startswith("fund_fund_i__summary_") for key in details.keys())
    assert any(key.startswith("overall__status_") for key in details.keys())
    assert any(key.startswith("overall__summary_") for key in details.keys())

    fund_i = next(fund for fund in track_record["funds"] if fund["fund_name"] == "Fund I")
    fund_i_fully = next(rollup for rollup in fund_i["status_rollups"] if rollup["status"] == "Fully Realized")
    row_key = fund_i_fully["row_key"]
    out = details[row_key]

    subset = [d for d in deals if d.fund_number == "Fund I" and d.status == "Fully Realized"]
    subset_metrics = {d.id: metrics[d.id] for d in subset}
    expected_portfolio = compute_portfolio_analytics(subset, metrics_by_id=subset_metrics)
    expected_bridge = compute_bridge_aggregate(subset, basis="fund")

    assert out["deal_count"] == len(subset)
    assert abs(out["entry_exit"]["returns"]["gross_moic"]["avg"] - expected_portfolio["returns"]["gross_moic"]["avg"]) < 1e-9
    assert abs(out["entry_exit"]["returns"]["gross_moic"]["wavg"] - expected_portfolio["returns"]["gross_moic"]["wavg"]) < 1e-9
    assert abs((out["bridge"]["drivers"]["dollar"]["revenue"] or 0.0) - (expected_bridge["drivers"]["dollar"]["revenue"] or 0.0)) < 1e-9
    assert abs((out["bridge"]["drivers"]["moic"]["multiple"] or 0.0) - (expected_bridge["drivers"]["moic"]["multiple"] or 0.0)) < 1e-9


def test_ic_memo_payload_schema_and_bridge_consistency():
    deals = [
        _make_deal(
            id=1,
            company_name="Alpha",
            fund_number="Fund I",
            sector="Tech",
            geography="US",
            status="Fully Realized",
            equity_invested=100,
            realized_value=180,
            unrealized_value=0,
        ),
        _make_deal(
            id=2,
            company_name="Beta",
            fund_number="Fund I",
            sector="Tech",
            geography="US",
            status="Unrealized",
            equity_invested=100,
            realized_value=0,
            unrealized_value=150,
        ),
        _make_deal(
            id=3,
            company_name="Gamma",
            fund_number="Fund II",
            sector="Healthcare",
            geography="UK",
            status="Partially Realized",
            equity_invested=80,
            realized_value=40,
            unrealized_value=60,
        ),
    ]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}
    out = compute_ic_memo_payload(deals, metrics_by_id=metrics)

    for key in ("meta", "executive", "bridge", "risk", "operating", "slicing", "team"):
        assert key in out

    assert len(out["executive"]["top_5_deals"]) == 3
    assert out["executive"]["top_5_deals"][0]["company_name"] == "Alpha"
    assert out["executive"]["bottom_5_deals"][0]["company_name"] == "Gamma"

    agg = compute_bridge_aggregate(deals, basis="fund")
    by_driver = {row["driver"]: row for row in out["bridge"]["table_rows"]}
    expected_rows = {row["key"]: row for row in agg["display_drivers"]}
    assert set(by_driver.keys()) == set(expected_rows.keys())
    for driver, expected in expected_rows.items():
        assert abs((by_driver[driver]["dollar"] or 0.0) - (expected["dollar"] or 0.0)) < 1e-9
        assert abs((by_driver[driver]["moic"] or 0.0) - (expected["moic"] or 0.0)) < 1e-9


def test_ic_memo_status_rollup_and_decile_rule():
    deals = [
        _make_deal(id=1, company_name="A", status="Fully Realized", equity_invested=100, realized_value=150, unrealized_value=0),
        _make_deal(id=2, company_name="B", status="Partially Realized", equity_invested=100, realized_value=120, unrealized_value=0),
        _make_deal(id=3, company_name="C", status="Unrealized", equity_invested=100, realized_value=0, unrealized_value=80),
    ]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}
    out = compute_ic_memo_payload(deals, metrics_by_id=metrics, decile_pct=0.10, decile_min=1)
    status_dim = out["slicing"]["dimensions"]["status_rollup"]
    by_label = {row["label"]: row for row in status_dim["groups"]}

    assert by_label["Realized"]["deal_count"] == 2
    assert by_label["Unrealized"]["deal_count"] == 1
    assert len(status_dim["top_decile"]) == 1
    assert len(status_dim["bottom_decile"]) == 1


def test_methodology_payload_contract_and_uniqueness():
    payload = build_methodology_payload()
    assert set(payload.keys()) == {"meta", "sections", "glossary", "rules"}
    assert payload["meta"]["title"] == "Calculation Methodology & Audit"
    assert payload["sections"]
    assert payload["glossary"]
    assert payload["rules"]

    section_ids = [section["id"] for section in payload["sections"]]
    assert len(section_ids) == len(set(section_ids))

    metric_ids = []
    for section in payload["sections"]:
        assert {"id", "title", "summary", "items"} <= set(section.keys())
        for item in section["items"]:
            assert {
                "id",
                "name",
                "formula",
                "code_formula",
                "variables",
                "interpretation",
                "edge_cases",
                "units",
                "source_refs",
            } <= set(item.keys())
            metric_ids.append(item["id"])
    assert len(metric_ids) == len(set(metric_ids))


def test_methodology_payload_contains_canonical_formula_strings():
    payload = build_methodology_payload()
    by_id = {
        item["id"]: item
        for section in payload["sections"]
        for item in section["items"]
    }

    assert by_id["metric-gross-moic"]["formula"] == "(Realized Value + Unrealized Value) / Equity Invested"
    assert by_id["metric-implied-irr"]["formula"] == "(Gross MOIC ^ (1 / Hold Years)) - 1"
    assert by_id["metric-loss-ratio-count"]["formula"] == "(Deals with MOIC < 1.0x / Deals with valid MOIC) * 100"
    assert by_id["metric-loss-ratio-capital"]["formula"] == "(Equity invested in deals with MOIC < 1.0x / Total evaluated equity) * 100"
    assert by_id["metric-bridge-revenue"]["formula"] == "(Exit Revenue - Entry Revenue) * m0 * x0"
    assert "Entry EBITDA <= 0 or Exit EBITDA <= 0" in by_id["metric-bridge-fallback-negative-ebitda"]["formula"]
    assert "Entry Revenue and Exit Revenue are missing/near-zero" in by_id["metric-bridge-fallback-missing-revenue"]["formula"]
    assert by_id["metric-tvpi"]["formula"] == "(Distributed + NAV) / Paid-In"
    assert "Stressed EBITDA = max(0, Current EBITDA * (1 + EBITDA Shock))" in by_id["metric-stress-lab-core"]["formula"]

def _add_db_deal(**kwargs):
    defaults = {
        "company_name": "Deal Co",
        "fund_number": "Fund IX",
        "status": "Unrealized",
        "investment_date": date(2020, 1, 1),
        "exit_date": None,
        "equity_invested": 100.0,
        "realized_value": 0.0,
        "unrealized_value": 140.0,
        "entry_revenue": 80.0,
        "entry_ebitda": 10.0,
        "entry_enterprise_value": 120.0,
        "entry_net_debt": 30.0,
        "exit_revenue": 110.0,
        "exit_ebitda": 16.0,
        "exit_enterprise_value": 180.0,
        "exit_net_debt": 20.0,
    }
    defaults.update(kwargs)
    deal = Deal(**defaults)
    db.session.add(deal)
    db.session.flush()
    return deal


def test_fund_liquidity_analysis_formulas(app_context):
    d1 = _add_db_deal(company_name="LQ A", equity_invested=100, realized_value=10, unrealized_value=110)
    d2 = _add_db_deal(company_name="LQ B", equity_invested=200, realized_value=40, unrealized_value=200)
    db.session.add(
        FundQuarterSnapshot(
            fund_number="Fund IX",
            quarter_end=date(2025, 12, 31),
            committed_capital=500,
            paid_in_capital=300,
            distributed_capital=120,
            nav=240,
            unfunded_commitment=200,
        )
    )
    db.session.add_all(
        [
            DealQuarterSnapshot(deal_id=d1.id, quarter_end=date(2025, 12, 31), equity_value=120),
            DealQuarterSnapshot(deal_id=d2.id, quarter_end=date(2025, 12, 31), equity_value=240),
        ]
    )
    db.session.commit()

    out = compute_fund_liquidity_analysis([d1, d2])
    assert out["has_data"] is True
    assert len(out["quarters"]) == 1
    assert abs(out["tvpi"][0] - ((120 + 240) / 300)) < 1e-9
    assert abs(out["dpi"][0] - (120 / 300)) < 1e-9
    assert abs(out["rvpi"][0] - (240 / 300)) < 1e-9
    assert abs(out["pic"][0] - (300 / 500)) < 1e-9
    assert abs(out["gross_tvpi"][0] - (360 / 300)) < 1e-9


def test_underwrite_outcome_uses_deltas_and_coverage(app_context):
    deal = _add_db_deal(
        company_name="UW Co",
        status="Fully Realized",
        investment_date=date(2020, 1, 1),
        exit_date=date(2024, 1, 1),
        equity_invested=100,
        realized_value=180,
        unrealized_value=0,
        irr=0.15,
    )
    db.session.add(
        DealUnderwriteBaseline(
            deal_id=deal.id,
            baseline_date=date(2020, 1, 1),
            target_irr=0.12,
            target_moic=1.50,
            target_hold_years=5.0,
            target_exit_multiple=12.0,
            target_revenue_cagr=0.05,
            target_ebitda_cagr=0.08,
        )
    )
    db.session.commit()

    metrics = {deal.id: compute_deal_metrics(deal)}
    out = compute_underwrite_outcome_analysis([deal], metrics_by_id=metrics)

    assert out["coverage"]["deal_count"] == 1
    assert abs(out["rows"][0]["actual_moic"] - 1.8) < 1e-9
    assert abs(out["rows"][0]["delta_moic"] - 0.3) < 1e-9
    assert out["coverage"]["avg_delta_irr"] is not None


def test_valuation_quality_staleness_markdown_and_mark_error(app_context):
    unrealized = _add_db_deal(
        company_name="Mark Co",
        status="Unrealized",
        investment_date=date(2021, 1, 1),
        equity_invested=100,
        realized_value=0,
        unrealized_value=90,
    )
    realized = _add_db_deal(
        company_name="Exit Mark Co",
        status="Fully Realized",
        investment_date=date(2019, 1, 1),
        exit_date=date(2024, 12, 31),
        equity_invested=100,
        realized_value=140,
        unrealized_value=0,
    )
    db.session.add_all(
        [
            DealQuarterSnapshot(deal_id=unrealized.id, quarter_end=date(2024, 12, 31), equity_value=150),
            DealQuarterSnapshot(deal_id=unrealized.id, quarter_end=date(2025, 12, 31), equity_value=90),
            DealQuarterSnapshot(deal_id=realized.id, quarter_end=date(2024, 9, 30), equity_value=125),
        ]
    )
    db.session.commit()

    out = compute_valuation_quality_analysis([unrealized, realized], as_of_date=date(2026, 2, 23))
    assert out["summary"]["unrealized_count"] == 1
    assert out["summary"]["markdown_deal_count"] == 1
    assert out["summary"]["avg_abs_mark_error"] is not None
    assert out["mark_error_rows"][0]["company_name"] == "Exit Mark Co"


def test_exit_readiness_and_stress_lab_outputs(app_context):
    deal = _add_db_deal(
        company_name="Readiness Co",
        status="Unrealized",
        investment_date=date(2018, 1, 1),
        equity_invested=100,
        realized_value=0,
        unrealized_value=260,
        entry_ebitda=10,
        exit_ebitda=22,
        entry_enterprise_value=120,
        exit_enterprise_value=260,
        entry_net_debt=40,
        exit_net_debt=20,
    )
    db.session.add(
        DealUnderwriteBaseline(
            deal_id=deal.id,
            baseline_date=date(2018, 1, 1),
            target_moic=2.0,
            target_hold_years=5.0,
            target_ebitda_cagr=0.08,
        )
    )
    db.session.add_all(
        [
            DealQuarterSnapshot(deal_id=deal.id, quarter_end=date(2024, 12, 31), equity_value=210),
            DealQuarterSnapshot(deal_id=deal.id, quarter_end=date(2025, 12, 31), equity_value=250),
        ]
    )
    db.session.commit()

    metrics = {deal.id: compute_deal_metrics(deal)}
    readiness = compute_exit_readiness_analysis([deal], metrics_by_id=metrics)
    assert readiness["summary"]["deal_count"] == 1
    assert readiness["rows"][0]["time_above_target_years"] is not None

    stress = compute_stress_lab_analysis(
        [deal],
        scenario={"default_multiple_shock": 0.0, "default_ebitda_shock": 0.0},
        deal_overrides={deal.id: {"multiple_shock": -2.0, "ebitda_shock": -0.20, "expected_hold_years": 8.0}},
        metrics_by_id=metrics,
    )
    assert stress["summary"]["deal_count"] == 1
    assert stress["summary"]["stressed_total_value"] < stress["summary"]["base_total_value"]
    row = stress["deal_rows"][0]
    assert row["current_ebitda"] is not None
    assert row["current_multiple"] is not None
    assert row["stressed_ebitda"] is not None
    assert row["stressed_multiple"] is not None
    assert row["stressed_implied_irr"] is not None
    assert abs(row["multiple_shock"] - (-2.0)) < 1e-9
    assert abs(row["ebitda_shock"] - (-0.20)) < 1e-9
    assert abs(row["expected_hold_period"] - 8.0) < 1e-9


def test_deal_trajectory_uses_quarterly_and_cashflow_series(app_context):
    deal = _add_db_deal(company_name="Trajectory Co")
    db.session.add_all(
        [
            DealQuarterSnapshot(
                deal_id=deal.id,
                quarter_end=date(2024, 3, 31),
                revenue=100,
                ebitda=20,
                enterprise_value=220,
                net_debt=40,
                equity_value=180,
            ),
            DealQuarterSnapshot(
                deal_id=deal.id,
                quarter_end=date(2024, 6, 30),
                revenue=106,
                ebitda=22,
                enterprise_value=235,
                net_debt=38,
                equity_value=197,
            ),
            DealCashflowEvent(deal_id=deal.id, event_date=date(2024, 1, 15), event_type="Capital Call", amount=-20),
            DealCashflowEvent(deal_id=deal.id, event_date=date(2024, 8, 20), event_type="Distribution", amount=8),
        ]
    )
    db.session.commit()

    metrics = {deal.id: compute_deal_metrics(deal)}
    out = compute_deal_trajectory_analysis([deal], deal_id=deal.id, metrics_by_id=metrics)
    assert out["has_data"] is True
    assert out["selected_deal_id"] == deal.id
    assert len(out["trajectory"]) == 2
    assert len(out["cashflow_curve"]) >= 1


def test_stress_lab_negative_shocks_do_not_increase_moic(app_context):
    deal = _add_db_deal(
        company_name="Negative Shock Co",
        status="Unrealized",
        investment_date=date(2021, 1, 1),
        equity_invested=100,
        realized_value=0,
        unrealized_value=120,
        exit_ebitda=20,
        exit_enterprise_value=170,
        exit_net_debt=50,
    )
    db.session.commit()

    metrics = {deal.id: compute_deal_metrics(deal)}
    out = compute_stress_lab_analysis(
        [deal],
        deal_overrides={deal.id: {"multiple_shock": -1.0, "ebitda_shock": -0.10}},
        metrics_by_id=metrics,
    )
    row = out["deal_rows"][0]
    assert row["stressed_total_value"] <= row["current_total_value"]
    assert row["stressed_moic"] <= row["current_moic"]


def test_stress_lab_rows_sorted_by_fund_date_status(app_context):
    d3 = _add_db_deal(
        company_name="C",
        fund_number="Fund II",
        status="Unrealized",
        investment_date=date(2022, 1, 1),
        equity_invested=100,
        realized_value=0,
        unrealized_value=130,
    )
    d2 = _add_db_deal(
        company_name="B",
        fund_number="Fund I",
        status="Unrealized",
        investment_date=date(2021, 1, 1),
        equity_invested=100,
        realized_value=0,
        unrealized_value=120,
    )
    d1 = _add_db_deal(
        company_name="A",
        fund_number="Fund I",
        status="Fully Realized",
        investment_date=date(2021, 1, 1),
        equity_invested=100,
        realized_value=120,
        unrealized_value=0,
    )
    db.session.commit()

    deals = [d3, d2, d1]
    metrics = {d.id: compute_deal_metrics(d) for d in deals}
    out = compute_stress_lab_analysis(deals, metrics_by_id=metrics)
    ordered = [row["company_name"] for row in out["deal_rows"]]
    # Fund I first, then same investment date with Fully Realized before Unrealized, then Fund II.
    assert ordered == ["A", "B", "C"]
    assert [row["fund_number"] for row in out["fund_subtotals"]] == ["Fund I", "Fund II"]
    fund_i = out["fund_subtotals_map"]["Fund I"]
    assert abs(fund_i["current_total_value"] - 240.0) < 1e-9
    assert abs(fund_i["stressed_total_value"] - 240.0) < 1e-9
    assert abs(fund_i["current_moic"] - 1.2) < 1e-9
    assert abs(fund_i["stressed_moic"] - 1.2) < 1e-9
    assert abs(fund_i["delta_value"] - 0.0) < 1e-9
    assert fund_i["stressed_implied_irr"] is not None
    assert abs(fund_i["delta_moic"] - 0.0) < 1e-9


def test_stress_lab_expected_hold_period_changes_stressed_irr(app_context):
    deal = _add_db_deal(
        company_name="Hold Override Co",
        status="Unrealized",
        investment_date=date(2022, 1, 1),
        equity_invested=100,
        realized_value=0,
        unrealized_value=180,
        exit_ebitda=20,
        exit_enterprise_value=210,
        exit_net_debt=30,
    )
    db.session.commit()

    metrics = {deal.id: compute_deal_metrics(deal)}
    short_hold = compute_stress_lab_analysis(
        [deal],
        deal_overrides={deal.id: {"multiple_shock": 0.0, "ebitda_shock": 0.0, "expected_hold_years": 4.0}},
        metrics_by_id=metrics,
    )["deal_rows"][0]
    long_hold = compute_stress_lab_analysis(
        [deal],
        deal_overrides={deal.id: {"multiple_shock": 0.0, "ebitda_shock": 0.0, "expected_hold_years": 8.0}},
        metrics_by_id=metrics,
    )["deal_rows"][0]

    assert short_hold["stressed_moic"] == long_hold["stressed_moic"]
    assert short_hold["stressed_implied_irr"] > long_hold["stressed_implied_irr"]

"""Private credit loan-level analytics and portfolio aggregation."""

from __future__ import annotations

from datetime import date
from collections import defaultdict

from services.metrics.common import safe_divide


# ---------------------------------------------------------------------------
# Per-loan metrics
# ---------------------------------------------------------------------------


def compute_credit_loan_metrics(loan, as_of_date=None):
    """Compute per-loan return and risk metrics for a single CreditLoan.

    Returns a dict with: income_return, price_return, total_return,
    current_yield, ltv_delta, hold_months, and _warnings list.
    """
    warnings = []
    if as_of_date is None:
        as_of_date = loan.as_of_date or date.today()

    # Hold period
    hold_months = None
    if loan.close_date:
        delta = (loan.exit_date or as_of_date) - loan.close_date
        hold_months = max(delta.days / 30.44, 0) if delta.days > 0 else None

    hold_years = (hold_months / 12.0) if hold_months and hold_months > 0 else None

    # Cost basis: hold_size adjusted for OID
    cost_basis = None
    if loan.hold_size is not None and loan.hold_size > 0:
        oid = loan.fee_oid if loan.fee_oid is not None else 0.0
        cost_basis = loan.hold_size * (1.0 - oid)
    elif loan.hold_size is not None and loan.hold_size == 0:
        warnings.append("hold_size is zero, cannot compute returns")

    # Income return: (cumulative interest + fees + PIK accrual) / cost_basis
    interest = loan.cumulative_interest_income or 0.0
    fees = loan.cumulative_fee_income or 0.0

    pik_accrual = 0.0
    if loan.pik_toggle and loan.pik_rate is not None and loan.hold_size and hold_years:
        pik_accrual = loan.pik_rate * loan.hold_size * hold_years

    income_return = safe_divide(interest + fees + pik_accrual, cost_basis)

    # Price return: (fair_value - cost_basis) / cost_basis
    price_return = None
    if loan.fair_value is not None and cost_basis is not None and cost_basis > 0:
        price_return = safe_divide(loan.fair_value - cost_basis, cost_basis)

    # Total return
    total_return = None
    if income_return is not None and price_return is not None:
        total_return = income_return + price_return
    elif income_return is not None:
        total_return = income_return

    # Current yield: coupon_rate (annualized, already decimal)
    current_yield = loan.coupon_rate

    # LTV delta
    ltv_delta = None
    if loan.entry_ltv is not None and loan.current_ltv is not None:
        ltv_delta = loan.current_ltv - loan.entry_ltv

    return {
        "income_return": income_return,
        "price_return": price_return,
        "total_return": total_return,
        "current_yield": current_yield,
        "yield_to_maturity": loan.yield_to_maturity,
        "ltv_delta": ltv_delta,
        "hold_months": hold_months,
        "hold_years": hold_years,
        "cost_basis": cost_basis,
        "pik_accrual": pik_accrual,
        "interest_coverage_ratio": loan.interest_coverage_ratio,
        "dscr": loan.dscr,
        "moic": loan.moic,
        "gross_irr": loan.gross_irr,
        "_warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Traffic light health signal
# ---------------------------------------------------------------------------


def _loan_traffic_light(loan):
    """Return 'green', 'yellow', 'red', or 'gray' for a single loan."""
    ds = (loan.default_status or "").strip()
    if ds in ("Default", "Restructured"):
        return "red"
    ltv = loan.current_ltv
    cov = loan.interest_coverage_ratio
    covenant_ok = loan.covenant_compliant

    # Red: LTV > 90% or coverage < 1.0
    if ltv is not None and ltv > 0.90:
        return "red"
    if cov is not None and cov < 1.0:
        return "red"

    # Yellow: Watch List OR LTV 75-90% OR coverage 1.0-1.5 OR covenant breach
    if ds == "Watch List":
        return "yellow"
    if ltv is not None and ltv > 0.75:
        return "yellow"
    if cov is not None and cov < 1.5:
        return "yellow"
    if covenant_ok is False:
        return "yellow"

    # Green: Performing + LTV < 75% + coverage > 1.5 + no covenant breach
    if ds == "Performing":
        if ltv is not None and cov is not None:
            return "green"
        # Insufficient data for full classification
        return "green" if ltv is not None or cov is not None else "gray"

    return "gray"


def compute_traffic_lights(loans):
    """Compute portfolio-level traffic light signal and per-loan breakdown."""
    lights = {"green": 0.0, "yellow": 0.0, "red": 0.0, "gray": 0.0}
    total_hold = 0.0
    per_loan = []

    for loan in loans:
        signal = _loan_traffic_light(loan)
        hold = loan.hold_size or 0.0
        lights[signal] += hold
        total_hold += hold
        per_loan.append({"loan_id": loan.id, "company": loan.company_name, "signal": signal})

    pcts = {}
    for k in lights:
        pcts[k] = safe_divide(lights[k], total_hold, 0.0) if total_hold > 0 else 0.0

    # Portfolio signal
    if pcts.get("red", 0) > 0.05:
        portfolio_signal = "red"
    elif pcts.get("yellow", 0) > 0.10:
        portfolio_signal = "yellow"
    elif pcts.get("green", 0) > 0.90:
        portfolio_signal = "green"
    else:
        portfolio_signal = "yellow"

    return {
        "portfolio_signal": portfolio_signal,
        "by_signal": lights,
        "pcts": pcts,
        "per_loan": per_loan,
    }


# ---------------------------------------------------------------------------
# Top concerns
# ---------------------------------------------------------------------------


def compute_top_concerns(loans, metrics_by_id=None, limit=5):
    """Auto-generate top concerns ranked by severity."""
    concerns = []
    total_hold = sum((l.hold_size or 0) for l in loans)

    for loan in loans:
        hold_pct = safe_divide(loan.hold_size, total_hold, 0.0) if total_hold > 0 else 0.0

        # Severity 1: Default or Restructured
        if (loan.default_status or "") in ("Default", "Restructured"):
            concerns.append({
                "company": loan.company_name,
                "severity": 1,
                "reason": f"{loan.default_status}",
                "metric": f"Hold: {hold_pct:.1%}" if hold_pct else None,
                "hold_pct": hold_pct,
            })

        # Severity 2: Covenant breach
        if loan.covenant_compliant is False:
            concerns.append({
                "company": loan.company_name,
                "severity": 2,
                "reason": "Covenant breach",
                "metric": f"Type: {loan.covenant_type or 'Unknown'}",
                "hold_pct": hold_pct,
            })

        # Severity 3: Watch List with high LTV
        if (loan.default_status or "") == "Watch List" and loan.current_ltv is not None and loan.current_ltv > 0.80:
            concerns.append({
                "company": loan.company_name,
                "severity": 3,
                "reason": "Watch List + high LTV",
                "metric": f"LTV: {loan.current_ltv:.0%}",
                "hold_pct": hold_pct,
            })

        # Severity 4: Maturing within 6 months
        if loan.maturity_date:
            days_to_maturity = (loan.maturity_date - date.today()).days
            if 0 < days_to_maturity <= 180:
                concerns.append({
                    "company": loan.company_name,
                    "severity": 4,
                    "reason": f"Matures in {days_to_maturity} days",
                    "metric": f"Maturity: {loan.maturity_date.isoformat()}",
                    "hold_pct": hold_pct,
                })

        # Severity 5: Large PIK accrual
        if loan.pik_toggle and loan.pik_rate and loan.hold_size:
            m = (metrics_by_id or {}).get(loan.id, {})
            pik_acc = m.get("pik_accrual", 0)
            if loan.hold_size > 0 and pik_acc > 0.20 * loan.hold_size:
                concerns.append({
                    "company": loan.company_name,
                    "severity": 5,
                    "reason": "PIK accrual > 20% of hold",
                    "metric": f"PIK rate: {loan.pik_rate:.1%}",
                    "hold_pct": hold_pct,
                })

    concerns.sort(key=lambda c: (c["severity"], -(c.get("hold_pct") or 0)))
    return concerns[:limit]


# ---------------------------------------------------------------------------
# Portfolio analytics
# ---------------------------------------------------------------------------


def compute_credit_portfolio_analytics(loans, metrics_by_id=None):
    """Aggregate portfolio-level credit analytics.

    Returns total commitments, deployed capital, weighted average yield/LTV,
    status distribution, rating distribution, and traffic lights.
    """
    if metrics_by_id is None:
        metrics_by_id = {l.id: compute_credit_loan_metrics(l) for l in loans}

    total_issue = 0.0
    total_hold = 0.0
    weighted_yield = 0.0
    weighted_ltv = 0.0
    weighted_spread = 0.0
    yield_denom = 0.0
    ltv_denom = 0.0
    spread_denom = 0.0

    status_dist = defaultdict(int)
    rating_dist = defaultdict(int)
    fund_set = set()

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        hold = (loan.hold_size or 0.0) * fx
        issue = (loan.issue_size or 0.0) * fx

        total_issue += issue
        total_hold += hold

        if loan.coupon_rate is not None and hold > 0:
            weighted_yield += loan.coupon_rate * hold
            yield_denom += hold

        if loan.current_ltv is not None and hold > 0:
            weighted_ltv += loan.current_ltv * hold
            ltv_denom += hold

        if loan.spread_bps is not None and hold > 0:
            weighted_spread += loan.spread_bps * hold
            spread_denom += hold

        status_dist[loan.default_status or "Unknown"] += 1
        if loan.internal_credit_rating is not None:
            rating_dist[loan.internal_credit_rating] += 1

        fund_set.add(loan.fund_name)

    performing_count = status_dist.get("Performing", 0)
    pct_performing = safe_divide(performing_count, len(loans), 0.0) if loans else 0.0

    traffic = compute_traffic_lights(loans)
    concerns = compute_top_concerns(loans, metrics_by_id)

    return {
        "total_issue_size": total_issue,
        "total_deployed": total_hold,
        "loan_count": len(loans),
        "fund_count": len(fund_set),
        "wavg_yield": safe_divide(weighted_yield, yield_denom),
        "wavg_ltv": safe_divide(weighted_ltv, ltv_denom),
        "wavg_spread_bps": safe_divide(weighted_spread, spread_denom),
        "pct_performing": pct_performing,
        "status_distribution": dict(status_dist),
        "rating_distribution": dict(rating_dist),
        "traffic_lights": traffic,
        "top_concerns": concerns,
    }


# ---------------------------------------------------------------------------
# Yield attribution
# ---------------------------------------------------------------------------


def compute_credit_yield_attribution(loans, metrics_by_id=None):
    """Decompose portfolio returns into coupon, fee, PIK, and price components."""
    if metrics_by_id is None:
        metrics_by_id = {l.id: compute_credit_loan_metrics(l) for l in loans}

    total_interest = 0.0
    total_fees = 0.0
    total_pik = 0.0
    total_price_change = 0.0
    total_cost_basis = 0.0

    by_fund = defaultdict(lambda: {"interest": 0, "fees": 0, "pik": 0, "price": 0, "basis": 0, "count": 0})

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        m = metrics_by_id.get(loan.id, {})

        interest = (loan.cumulative_interest_income or 0.0) * fx
        fees = (loan.cumulative_fee_income or 0.0) * fx
        pik = (m.get("pik_accrual") or 0.0) * fx
        basis = (m.get("cost_basis") or 0.0) * fx
        fv = (loan.fair_value or 0.0) * fx
        price_chg = (fv - basis) if basis > 0 and loan.fair_value is not None else 0.0

        total_interest += interest
        total_fees += fees
        total_pik += pik
        total_price_change += price_chg
        total_cost_basis += basis

        fund = loan.fund_name or "Unknown"
        by_fund[fund]["interest"] += interest
        by_fund[fund]["fees"] += fees
        by_fund[fund]["pik"] += pik
        by_fund[fund]["price"] += price_chg
        by_fund[fund]["basis"] += basis
        by_fund[fund]["count"] += 1

    total_income = total_interest + total_fees + total_pik + total_price_change

    return {
        "coupon_income": total_interest,
        "fee_income": total_fees,
        "pik_accrual": total_pik,
        "price_appreciation": total_price_change,
        "total_return_dollars": total_income,
        "total_return_pct": safe_divide(total_income, total_cost_basis),
        "by_fund": {k: dict(v) for k, v in by_fund.items()},
    }


# ---------------------------------------------------------------------------
# Stress scenarios
# ---------------------------------------------------------------------------


def compute_credit_stress_scenarios(loans, scenario):
    """V1: Independent per-loan stress testing.

    scenario dict keys:
        default_rate_shock: float (e.g. 0.05 = 5% of loans default)
        recovery_rate_shock: float (e.g. 0.40 = 40% recovery on defaults)
        rate_shock_bps: int (e.g. 200 = +200bps to floating rate loans)
    """
    default_shock = scenario.get("default_rate_shock", 0.0)
    recovery_shock = scenario.get("recovery_rate_shock", 0.40)
    rate_shock = scenario.get("rate_shock_bps", 0)

    # Sort loans by credit quality (worst first) for default assignment
    sorted_loans = sorted(loans, key=lambda l: -(l.internal_credit_rating or 0))

    total_hold = sum((l.hold_size or 0) for l in loans)
    target_default_amount = total_hold * default_shock

    base_nav = 0.0
    stressed_nav = 0.0
    defaulted_amount = 0.0
    impacted_loans = []

    for loan in sorted_loans:
        hold = loan.hold_size or 0.0
        fv = loan.fair_value if loan.fair_value is not None else hold
        base_nav += fv

        # Apply default shock: default worst-rated loans first
        is_already_default = (loan.default_status or "") in ("Default", "Restructured")
        if not is_already_default and defaulted_amount < target_default_amount and hold > 0:
            default_this = min(hold, target_default_amount - defaulted_amount)
            loss = default_this * (1.0 - recovery_shock)
            remaining = fv - loss
            stressed_nav += max(remaining, 0.0)
            defaulted_amount += default_this
            impacted_loans.append({
                "company": loan.company_name,
                "hold_size": hold,
                "loss": loss,
                "stress_type": "default",
            })
        elif is_already_default:
            # Already defaulted, apply recovery rate
            stressed_nav += hold * recovery_shock
        else:
            # Rate shock: floating loans lose value proportional to rate increase
            if (loan.fixed_or_floating or "").lower() == "floating" and rate_shock != 0:
                # Approximate: rate shock affects yield, not directly NAV for floating
                # But spread compression could reduce fair value
                stressed_nav += fv
            else:
                stressed_nav += fv

    nav_impact_pct = safe_divide(stressed_nav - base_nav, base_nav) if base_nav > 0 else 0.0

    return {
        "base_nav": base_nav,
        "stressed_nav": stressed_nav,
        "nav_impact_pct": nav_impact_pct,
        "nav_impact_dollars": stressed_nav - base_nav,
        "defaults_triggered": len(impacted_loans),
        "defaulted_amount": defaulted_amount,
        "impacted_loans": impacted_loans,
        "scenario": scenario,
    }


# ---------------------------------------------------------------------------
# Concentration
# ---------------------------------------------------------------------------


def compute_credit_concentration(loans, metrics_by_id=None):
    """Sector, geography, sponsor, security type breakdowns with HHI."""
    total_hold = sum((l.hold_size or 0) for l in loans)

    def _build_breakdown(loans, attr):
        groups = defaultdict(lambda: {"hold": 0.0, "count": 0})
        for loan in loans:
            key = getattr(loan, attr, None) or "Unknown"
            groups[key]["hold"] += loan.hold_size or 0.0
            groups[key]["count"] += 1
        result = []
        for key, val in sorted(groups.items(), key=lambda x: -x[1]["hold"]):
            pct = safe_divide(val["hold"], total_hold, 0.0)
            result.append({"name": key, "hold": val["hold"], "count": val["count"], "pct": pct})
        return result

    def _hhi(breakdown):
        return sum((item["pct"] ** 2) for item in breakdown if item.get("pct"))

    by_sector = _build_breakdown(loans, "sector")
    by_geography = _build_breakdown(loans, "geography")
    by_sponsor = _build_breakdown(loans, "sponsor")
    by_security = _build_breakdown(loans, "security_type")
    by_rating = _build_breakdown(loans, "internal_credit_rating")

    # Top-N single-name exposure
    loan_exposures = sorted(
        [{"company": l.company_name, "hold": l.hold_size or 0, "pct": safe_divide(l.hold_size, total_hold, 0.0)}
         for l in loans],
        key=lambda x: -(x.get("hold") or 0),
    )

    return {
        "total_hold": total_hold,
        "by_sector": by_sector,
        "by_geography": by_geography,
        "by_sponsor": by_sponsor,
        "by_security": by_security,
        "by_rating": by_rating,
        "hhi_sector": _hhi(by_sector),
        "hhi_geography": _hhi(by_geography),
        "hhi_sponsor": _hhi(by_sponsor),
        "top_5": loan_exposures[:5],
        "top_10": loan_exposures[:10],
    }


# ---------------------------------------------------------------------------
# Vintage comparison
# ---------------------------------------------------------------------------


def compute_credit_vintage_comparison(loans, fund_names=None):
    """Compare credit funds side by side."""
    by_fund = defaultdict(list)
    for loan in loans:
        fn = loan.fund_name or "Unknown"
        if fund_names is None or fn in fund_names:
            by_fund[fn].append(loan)

    funds = []
    for fund_name, fund_loans in sorted(by_fund.items()):
        metrics = {l.id: compute_credit_loan_metrics(l) for l in fund_loans}
        total_hold = sum((l.hold_size or 0) for l in fund_loans)
        total_issue = sum((l.issue_size or 0) for l in fund_loans)

        # Weighted averages
        w_yield = sum((l.coupon_rate or 0) * (l.hold_size or 0) for l in fund_loans)
        w_ltv = sum((l.current_ltv or 0) * (l.hold_size or 0) for l in fund_loans)
        denom = sum((l.hold_size or 0) for l in fund_loans if l.coupon_rate is not None)
        ltv_denom = sum((l.hold_size or 0) for l in fund_loans if l.current_ltv is not None)

        performing = sum(1 for l in fund_loans if (l.default_status or "") == "Performing")
        defaults = sum(1 for l in fund_loans if (l.default_status or "") in ("Default", "Restructured"))

        vintages = [l.vintage_year for l in fund_loans if l.vintage_year]

        funds.append({
            "fund_name": fund_name,
            "vintage_year": min(vintages) if vintages else None,
            "loan_count": len(fund_loans),
            "total_deployed": total_hold,
            "total_issue_size": total_issue,
            "deployment_pct": safe_divide(total_hold, total_issue),
            "wavg_yield": safe_divide(w_yield, denom),
            "wavg_ltv": safe_divide(w_ltv, ltv_denom),
            "pct_performing": safe_divide(performing, len(fund_loans), 0.0),
            "default_count": defaults,
            "loss_rate": safe_divide(defaults, len(fund_loans), 0.0),
            "rating_distribution": _rating_dist(fund_loans),
        })

    return {"funds": funds}


def _rating_dist(loans):
    dist = defaultdict(int)
    for l in loans:
        if l.internal_credit_rating is not None:
            dist[l.internal_credit_rating] += 1
    return dict(dist)


# ---------------------------------------------------------------------------
# Maturity profile
# ---------------------------------------------------------------------------


def compute_credit_maturity_profile(loans):
    """Build maturity wall data and weighted average life."""
    from collections import defaultdict

    by_month = defaultdict(lambda: {"hold": 0.0, "count": 0})
    total_hold = 0.0
    weighted_life = 0.0
    life_denom = 0.0
    today = date.today()

    for loan in loans:
        hold = loan.hold_size or 0.0
        total_hold += hold

        if loan.maturity_date:
            key = loan.maturity_date.strftime("%Y-%m")
            by_month[key]["hold"] += hold
            by_month[key]["count"] += 1

            years_to_maturity = max((loan.maturity_date - today).days / 365.25, 0)
            if hold > 0:
                weighted_life += years_to_maturity * hold
                life_denom += hold

    wal = safe_divide(weighted_life, life_denom)

    # Sort by month
    maturity_wall = [
        {"month": k, "hold": v["hold"], "count": v["count"],
         "pct": safe_divide(v["hold"], total_hold, 0.0)}
        for k, v in sorted(by_month.items())
    ]

    # Loans maturing within windows
    maturing_6m = sum(1 for l in loans if l.maturity_date and 0 < (l.maturity_date - today).days <= 180)
    maturing_12m = sum(1 for l in loans if l.maturity_date and 0 < (l.maturity_date - today).days <= 365)
    already_matured = sum(1 for l in loans if l.maturity_date and (l.maturity_date - today).days <= 0)

    return {
        "weighted_average_life": wal,
        "maturity_wall": maturity_wall,
        "total_hold": total_hold,
        "maturing_6m": maturing_6m,
        "maturing_12m": maturing_12m,
        "already_matured": already_matured,
        "no_maturity_date": sum(1 for l in loans if not l.maturity_date),
    }

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

    # --- NEW: Mark-to-market metrics (when LP provides total_value/entry_loan_amount) ---
    total_return_mtm = None
    entry_amt = loan.entry_loan_amount if hasattr(loan, 'entry_loan_amount') else None
    tv = loan.total_value if hasattr(loan, 'total_value') else None
    if entry_amt is not None and entry_amt > 0 and tv is not None:
        total_return_mtm = safe_divide(tv, entry_amt) - 1.0

    warrant_upside = None
    uwev = getattr(loan, 'unrealized_warrant_equity_value', None)
    if uwev is not None and entry_amt is not None and entry_amt > 0:
        warrant_upside = safe_divide(uwev, entry_amt)

    revenue_growth = None
    rev_entry = getattr(loan, 'ttm_revenue_entry', None)
    rev_current = getattr(loan, 'ttm_revenue_current', None)
    if rev_entry is not None and rev_entry > 0 and rev_current is not None:
        revenue_growth = safe_divide(rev_current, rev_entry) - 1.0

    deployment_pct = None
    committed = getattr(loan, 'committed_amount', None)
    current_inv = getattr(loan, 'current_invested_capital', None)
    if committed is not None and committed > 0 and current_inv is not None:
        deployment_pct = safe_divide(current_inv, committed)

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
        "total_return_mtm": total_return_mtm,
        "warrant_upside": warrant_upside,
        "revenue_growth": revenue_growth,
        "deployment_pct": deployment_pct,
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

    # --- NEW: IRR/MOIC-based signals (when LP provides these instead of LTV/coverage) ---
    status = (loan.status or "").strip()
    moic_val = loan.moic
    irr_val = loan.gross_irr
    tv = getattr(loan, 'total_value', None)
    entry_amt = getattr(loan, 'entry_loan_amount', None) or loan.hold_size

    # Red: underwater (total_value < entry amount) or MOIC < 0.8
    if tv is not None and entry_amt is not None and entry_amt > 0 and tv < entry_amt * 0.8:
        return "red"
    if moic_val is not None and moic_val < 0.8:
        return "red"

    # Yellow: underperforming (MOIC 0.8-1.0, or IRR below estimated)
    if moic_val is not None and moic_val < 1.0:
        return "yellow"
    est_irr = getattr(loan, 'estimated_irr_at_entry', None)
    if irr_val is not None and est_irr is not None and irr_val < est_irr * 0.5:
        return "yellow"

    # Green: Realized with MOIC >= 1.0 or positive IRR
    if status == "Realized" and moic_val is not None and moic_val >= 1.0:
        return "green"
    if irr_val is not None and irr_val > 0:
        return "green"
    if moic_val is not None and moic_val >= 1.0:
        return "green"

    return "gray"


def compute_traffic_lights(loans):
    """Compute portfolio-level traffic light signal and per-loan breakdown."""
    lights = {"green": 0.0, "yellow": 0.0, "red": 0.0, "gray": 0.0}
    total_hold = 0.0
    per_loan = []

    for loan in loans:
        signal = _loan_traffic_light(loan)
        hold = loan.hold_size or getattr(loan, 'entry_loan_amount', None) or 0.0
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
    total_hold = sum((l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in loans)

    for loan in loans:
        hold = loan.hold_size or getattr(loan, 'entry_loan_amount', None) or 0
        hold_pct = safe_divide(hold, total_hold, 0.0) if total_hold > 0 else 0.0

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

        # NEW: Severity 1 - Underwater (total_value < entry_loan_amount)
        entry_amt = getattr(loan, 'entry_loan_amount', None) or loan.hold_size
        tv = getattr(loan, 'total_value', None)
        if tv is not None and entry_amt is not None and entry_amt > 0 and tv < entry_amt * 0.9:
            shortfall_pct = safe_divide(entry_amt - tv, entry_amt, 0.0)
            concerns.append({
                "company": loan.company_name,
                "severity": 1,
                "reason": "Underwater",
                "metric": f"Value: {tv/entry_amt:.0%} of entry" if entry_amt > 0 else None,
                "hold_pct": hold_pct,
            })

        # NEW: Severity 2 - IRR significantly below estimate
        irr_val = loan.gross_irr
        est_irr = getattr(loan, 'estimated_irr_at_entry', None)
        if irr_val is not None and est_irr is not None and est_irr > 0 and irr_val < est_irr * 0.5:
            concerns.append({
                "company": loan.company_name,
                "severity": 2,
                "reason": "IRR underperforming",
                "metric": f"IRR: {irr_val:.1%} vs {est_irr:.1%} est.",
                "hold_pct": hold_pct,
            })

        # NEW: Severity 4 - Declining revenue
        rev_entry = getattr(loan, 'ttm_revenue_entry', None)
        rev_current = getattr(loan, 'ttm_revenue_current', None)
        if rev_entry is not None and rev_entry > 0 and rev_current is not None and rev_current < rev_entry * 0.85:
            concerns.append({
                "company": loan.company_name,
                "severity": 4,
                "reason": "Revenue declining",
                "metric": f"Rev: {rev_current/rev_entry:.0%} of entry",
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

    # --- NEW: Aggregations for LP fields ---
    total_committed = 0.0
    total_entry_loan = 0.0
    total_current_invested = 0.0
    total_realized_proceeds = 0.0
    total_unrealized_loan = 0.0
    total_unrealized_warrant = 0.0
    total_total_value = 0.0
    total_equity_investment = 0.0
    weighted_irr = 0.0
    irr_denom = 0.0
    weighted_moic = 0.0
    moic_denom = 0.0
    weighted_cash_margin = 0.0
    cash_margin_denom = 0.0
    has_new_fields = False

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        entry_amt = (getattr(loan, 'entry_loan_amount', None) or 0.0) * fx
        hold = (loan.hold_size or 0.0) * fx
        weight = entry_amt if entry_amt > 0 else hold

        committed = (getattr(loan, 'committed_amount', None) or 0.0) * fx
        cur_inv = (getattr(loan, 'current_invested_capital', None) or 0.0) * fx
        rp = (getattr(loan, 'realized_proceeds', None) or 0.0) * fx
        ulv = (getattr(loan, 'unrealized_loan_value', None) or 0.0) * fx
        uwev = (getattr(loan, 'unrealized_warrant_equity_value', None) or 0.0) * fx
        tv = (getattr(loan, 'total_value', None) or 0.0) * fx
        eq_inv = (getattr(loan, 'equity_investment', None) or 0.0) * fx

        total_committed += committed
        total_entry_loan += entry_amt
        total_current_invested += cur_inv
        total_realized_proceeds += rp
        total_unrealized_loan += ulv
        total_unrealized_warrant += uwev
        total_total_value += tv
        total_equity_investment += eq_inv

        if loan.gross_irr is not None and weight > 0:
            weighted_irr += loan.gross_irr * weight
            irr_denom += weight
        if loan.moic is not None and weight > 0:
            weighted_moic += loan.moic * weight
            moic_denom += weight
        cm = getattr(loan, 'cash_margin', None)
        if cm is not None and weight > 0:
            weighted_cash_margin += cm * weight
            cash_margin_denom += weight

        if any(getattr(loan, f, None) is not None for f in ('entry_loan_amount', 'total_value', 'committed_amount')):
            has_new_fields = True

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
        # NEW: LP-field aggregations
        "total_committed": total_committed if has_new_fields else None,
        "total_entry_loan": total_entry_loan if has_new_fields else None,
        "total_current_invested": total_current_invested if has_new_fields else None,
        "deployment_ratio": safe_divide(total_current_invested, total_committed) if total_committed > 0 else None,
        "total_realized_proceeds": total_realized_proceeds if has_new_fields else None,
        "total_unrealized_loan": total_unrealized_loan if has_new_fields else None,
        "total_unrealized_warrant": total_unrealized_warrant if has_new_fields else None,
        "total_total_value": total_total_value if has_new_fields else None,
        "total_equity_investment": total_equity_investment if has_new_fields else None,
        "total_warrant_value": total_unrealized_warrant if total_unrealized_warrant > 0 else None,
        "weighted_avg_irr": safe_divide(weighted_irr, irr_denom),
        "weighted_avg_moic": safe_divide(weighted_moic, moic_denom),
        "wavg_cash_margin": safe_divide(weighted_cash_margin, cash_margin_denom),
        "has_new_fields": has_new_fields,
        "has_revenue_data": any(getattr(l, 'ttm_revenue_entry', None) is not None for l in loans),
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

    # --- NEW: Attribution from LP fields ---
    total_cash_margin_income = 0.0
    total_pik_margin_income = 0.0
    total_fee_income_new = 0.0
    total_warrant_upside = 0.0
    total_price_return_new = 0.0
    has_new_yield = False

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        entry_amt = (getattr(loan, 'entry_loan_amount', None) or 0.0) * fx
        cm = getattr(loan, 'cash_margin', None)
        pm = getattr(loan, 'pik_margin', None)
        cf = getattr(loan, 'closing_fee', None)
        ef = loan.fee_exit
        uwev = (getattr(loan, 'unrealized_warrant_equity_value', None) or 0.0) * fx
        ulv = getattr(loan, 'unrealized_loan_value', None)

        if cm is not None and entry_amt > 0:
            total_cash_margin_income += cm * entry_amt
            has_new_yield = True
        if pm is not None and entry_amt > 0:
            total_pik_margin_income += pm * entry_amt
            has_new_yield = True
        if cf is not None:
            total_fee_income_new += (cf or 0.0) * fx
        if ef is not None:
            total_fee_income_new += (ef or 0.0) * fx
        total_warrant_upside += uwev
        if ulv is not None and entry_amt > 0:
            total_price_return_new += (ulv * fx - entry_amt)

        fund = loan.fund_name or "Unknown"
        if has_new_yield:
            by_fund[fund]["cash_margin"] = by_fund[fund].get("cash_margin", 0) + (cm or 0) * entry_amt
            by_fund[fund]["pik_margin"] = by_fund[fund].get("pik_margin", 0) + (pm or 0) * entry_amt
            by_fund[fund]["warrant_upside"] = by_fund[fund].get("warrant_upside", 0) + uwev

    return {
        "coupon_income": total_interest,
        "fee_income": total_fees,
        "pik_accrual": total_pik,
        "price_appreciation": total_price_change,
        "total_return_dollars": total_income,
        "total_return_pct": safe_divide(total_income, total_cost_basis),
        "by_fund": {k: dict(v) for k, v in by_fund.items()},
        "cash_margin_income": total_cash_margin_income if has_new_yield else None,
        "pik_margin_income": total_pik_margin_income if has_new_yield else None,
        "fee_income_new": total_fee_income_new if has_new_yield else None,
        "warrant_upside": total_warrant_upside if total_warrant_upside > 0 else None,
        "price_return_new": total_price_return_new if has_new_yield else None,
        "has_new_yield": has_new_yield,
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
    # Use internal_credit_rating first, fall back to MOIC (lowest first)
    def _stress_sort_key(l):
        rating = l.internal_credit_rating
        if rating is not None:
            return (-rating, 0)
        moic_val = l.moic
        if moic_val is not None:
            return (0, moic_val)  # lower MOIC = worse = first
        irr_val = l.gross_irr
        if irr_val is not None:
            return (0, irr_val)
        return (0, 999)  # no data, sort last

    sorted_loans = sorted(loans, key=_stress_sort_key)

    total_hold = sum((l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in loans)
    target_default_amount = total_hold * default_shock

    base_nav = 0.0
    stressed_nav = 0.0
    defaulted_amount = 0.0
    impacted_loans = []

    for loan in sorted_loans:
        hold = loan.hold_size or getattr(loan, 'entry_loan_amount', None) or 0.0
        fv = loan.fair_value if loan.fair_value is not None else (getattr(loan, 'total_value', None) or hold)
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
    total_hold = sum((l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in loans)

    def _build_breakdown(loans, attr):
        groups = defaultdict(lambda: {"hold": 0.0, "count": 0})
        for loan in loans:
            key = getattr(loan, attr, None) or "Unknown"
            hold = loan.hold_size or getattr(loan, 'entry_loan_amount', None) or 0.0
            groups[key]["hold"] += hold
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
        [{"company": l.company_name, "hold": l.hold_size or getattr(l, 'entry_loan_amount', None) or 0,
          "pct": safe_divide(l.hold_size or getattr(l, 'entry_loan_amount', None) or 0, total_hold, 0.0)}
         for l in loans],
        key=lambda x: -(x.get("hold") or 0),
    )

    # Location (merged with geography)
    by_location = _build_breakdown(loans, "location")

    # Sourcing channel (if data present)
    has_sourcing = any(getattr(l, 'sourcing_channel', None) for l in loans)
    by_sourcing = _build_breakdown(loans, "sourcing_channel") if has_sourcing else []

    # Public/private (if data present)
    has_public = any(getattr(l, 'is_public', None) is not None for l in loans)
    by_public = []
    if has_public:
        groups = defaultdict(lambda: {"hold": 0.0, "count": 0})
        for loan in loans:
            key = "Public" if getattr(loan, 'is_public', None) else "Private"
            hold = loan.hold_size or getattr(loan, 'entry_loan_amount', None) or 0.0
            groups[key]["hold"] += hold
            groups[key]["count"] += 1
        for key, val in sorted(groups.items(), key=lambda x: -x[1]["hold"]):
            pct = safe_divide(val["hold"], total_hold, 0.0)
            by_public.append({"name": key, "hold": val["hold"], "count": val["count"], "pct": pct})

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
        "by_location": by_location,
        "by_sourcing": by_sourcing if has_sourcing else None,
        "by_public": by_public if has_public else None,
        "has_sourcing_data": has_sourcing,
        "has_public_data": has_public,
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
        total_hold = sum((l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans)
        total_issue = sum((l.issue_size or 0) for l in fund_loans)
        total_committed = sum((getattr(l, 'committed_amount', None) or 0) for l in fund_loans)
        total_entry_loan = sum((getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans)

        # Weighted averages
        w_yield = sum((l.coupon_rate or 0) * (l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans)
        w_ltv = sum((l.current_ltv or 0) * (l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans)
        denom = sum((l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans if l.coupon_rate is not None)
        ltv_denom = sum((l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans if l.current_ltv is not None)

        # NEW: IRR, MOIC, cash margin averages
        w_irr = sum((l.gross_irr or 0) * (l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans if l.gross_irr is not None)
        irr_denom = sum((l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans if l.gross_irr is not None)
        w_moic = sum((l.moic or 0) * (l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans if l.moic is not None)
        moic_denom = sum((l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans if l.moic is not None)
        w_cm = sum((getattr(l, 'cash_margin', None) or 0) * (l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans if getattr(l, 'cash_margin', None) is not None)
        cm_denom = sum((l.hold_size or getattr(l, 'entry_loan_amount', None) or 0) for l in fund_loans if getattr(l, 'cash_margin', None) is not None)

        total_warrant_val = sum((getattr(l, 'unrealized_warrant_equity_value', None) or 0) for l in fund_loans)

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
            "total_committed": total_committed,
            "total_entry_loan": total_entry_loan,
            "avg_irr": safe_divide(w_irr, irr_denom),
            "avg_moic": safe_divide(w_moic, moic_denom),
            "avg_cash_margin": safe_divide(w_cm, cm_denom),
            "warrant_coverage": safe_divide(total_warrant_val, total_hold) if total_hold > 0 else None,
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


def _parse_loan_term(term_str, entry_date=None):
    """Parse loan_term string into estimated maturity date.

    Patterns: "5 years", "60 months", "5Y", "3-5 years", "18M", "18m"
    Returns a date or None.
    """
    if not term_str or not entry_date:
        return None
    import re
    from datetime import timedelta
    s = str(term_str).strip().lower()

    # "5 years" or "5 year" or "5yr" or "5y"
    m = re.match(r'^(\d+(?:\.\d+)?)\s*(?:years?|yr?s?)$', s)
    if m:
        years = float(m.group(1))
        return entry_date + timedelta(days=years * 365.25)

    # "60 months" or "60 month" or "60m" or "18mo"
    m = re.match(r'^(\d+(?:\.\d+)?)\s*(?:months?|mos?|m)$', s)
    if m:
        months = float(m.group(1))
        return entry_date + timedelta(days=months * 30.44)

    # "3-5 years" (range, use midpoint)
    m = re.match(r'^(\d+)-(\d+)\s*(?:years?|yr?s?)$', s)
    if m:
        avg = (float(m.group(1)) + float(m.group(2))) / 2
        return entry_date + timedelta(days=avg * 365.25)

    return None


def compute_credit_maturity_profile(loans):
    """Build maturity wall data and weighted average life."""
    from collections import defaultdict

    by_month = defaultdict(lambda: {"hold": 0.0, "count": 0})
    total_hold = 0.0
    weighted_life = 0.0
    life_denom = 0.0
    today = date.today()

    for loan in loans:
        hold = loan.hold_size or getattr(loan, 'entry_loan_amount', None) or 0.0
        total_hold += hold

        mat_date = loan.maturity_date
        # Fallback: derive from loan_term + close_date
        if mat_date is None:
            loan_term = getattr(loan, 'loan_term', None)
            entry = loan.close_date
            mat_date = _parse_loan_term(loan_term, entry)

        if mat_date:
            key = mat_date.strftime("%Y-%m")
            by_month[key]["hold"] += hold
            by_month[key]["count"] += 1

            years_to_maturity = max((mat_date - today).days / 365.25, 0)
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
    def _effective_maturity(loan):
        if loan.maturity_date:
            return loan.maturity_date
        lt = getattr(loan, 'loan_term', None)
        return _parse_loan_term(lt, loan.close_date)

    maturing_6m = sum(1 for l in loans if _effective_maturity(l) and 0 < (_effective_maturity(l) - today).days <= 180)
    maturing_12m = sum(1 for l in loans if _effective_maturity(l) and 0 < (_effective_maturity(l) - today).days <= 365)
    already_matured = sum(1 for l in loans if _effective_maturity(l) and (_effective_maturity(l) - today).days <= 0)
    no_maturity = sum(1 for l in loans if not _effective_maturity(l))

    return {
        "weighted_average_life": wal,
        "maturity_wall": maturity_wall,
        "total_hold": total_hold,
        "maturing_6m": maturing_6m,
        "maturing_12m": maturing_12m,
        "already_matured": already_matured,
        "no_maturity_date": no_maturity,
    }

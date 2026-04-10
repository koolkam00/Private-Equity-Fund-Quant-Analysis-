"""Private credit loan-level analytics and portfolio aggregation."""

from __future__ import annotations

from datetime import date
from collections import defaultdict

from services.metrics.common import safe_divide


# Stress assumption: floating loan NAV haircut for spread compression risk
# under a parallel rate shock. 1.5% of fair value per +100bps captures the
# observed mark-to-market sensitivity for senior secured floating-rate paper.
FLOATING_NAV_HAIRCUT_PER_100BPS = 0.015


# ---------------------------------------------------------------------------
# Watchlist scoring weights
# ---------------------------------------------------------------------------
# These weights are deliberately exposed at module level so the score breakdown
# returned per-loan is auditable: 12 months from now, when an analyst asks
# "why did this loan score 92?", we can answer by inspecting the breakdown
# against these constants. Tune them only with deliberate intent.

WATCHLIST_WEIGHT_LTV_TREND = 20
WATCHLIST_WEIGHT_ICR_TREND = 20
WATCHLIST_WEIGHT_EBITDA_TREND = 15
WATCHLIST_WEIGHT_STATUS_AUTO = 100  # Hard trigger: any non-Performing status
WATCHLIST_WEIGHT_COVENANT = 100     # Hard trigger: covenant_compliant is False
WATCHLIST_WEIGHT_SPONSOR_HISTORY = 10
WATCHLIST_WEIGHT_FLOOR_ROLLOFF = 5

WATCHLIST_SCORE_CAP = 100
WATCHLIST_URGENT_THRESHOLD = 80
WATCHLIST_ATTENTION_THRESHOLD = 50
WATCHLIST_MONITOR_THRESHOLD = 20

# Sub-thresholds within trend scoring so the function reads cleanly
WATCHLIST_LTV_TREND_DELTA_THRESHOLD = 0.05  # +5pp LTV climb -> full LTV trend points
WATCHLIST_ICR_TREND_DELTA_THRESHOLD = 0.5   # -0.5x ICR drop -> full ICR trend points
WATCHLIST_LOW_FLOOR_THRESHOLD = 0.02         # Floor below 2% is vulnerable to rate cuts


# ---------------------------------------------------------------------------
# Per-loan metrics
# ---------------------------------------------------------------------------


def _credit_position_amount(loan, fx=1.0):
    """Current invested capital first, with legacy fallbacks for older uploads."""
    for field in ("current_invested_capital", "entry_loan_amount", "hold_size"):
        value = getattr(loan, field, None)
        if value is not None and value > 0:
            return value * fx

    total_value = getattr(loan, "total_value", None)
    moic = getattr(loan, "moic", None)
    if total_value is not None and moic is not None and moic > 0:
        derived = total_value / moic
        if derived > 0:
            return derived * fx

    return 0.0


def _credit_hold_amount(loan, fx=1.0):
    """Best available current hold/exposure for weighting and concentration."""
    for field in ("hold_size", "current_invested_capital", "entry_loan_amount"):
        value = getattr(loan, field, None)
        if value is not None and value > 0:
            return value * fx

    total_value = getattr(loan, "total_value", None)
    moic = getattr(loan, "moic", None)
    if total_value is not None and moic is not None and moic > 0:
        derived = total_value / moic
        if derived > 0:
            return derived * fx

    return 0.0


def _credit_resolve_scalar(values, tolerance=1e-9):
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return {"value": None, "conflict": False}
    base = clean[0]
    if any(abs(v - base) > tolerance for v in clean[1:]):
        return {"value": None, "conflict": True}
    return {"value": base, "conflict": False}


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
    entry_amt = _credit_position_amount(loan)
    tv = loan.total_value if hasattr(loan, 'total_value') else None
    if entry_amt > 0 and tv is not None:
        total_return_mtm = safe_divide(tv, entry_amt) - 1.0

    warrant_upside = None
    uwev = getattr(loan, 'unrealized_warrant_equity_value', None)
    if uwev is not None and entry_amt > 0:
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
    """Return 'green', 'yellow', 'red', or 'gray' for a single loan.

    Evaluates LTV/coverage signals AND IRR/MOIC/total-value signals so loans
    coming from LP-style uploads (no LTV but with MOIC/IRR) get a real color
    instead of falling through to gray.
    """
    ds = (loan.default_status or "").strip()
    status = (loan.status or "").strip()
    ltv = loan.current_ltv
    cov = loan.interest_coverage_ratio
    covenant_ok = loan.covenant_compliant
    moic_val = loan.moic
    irr_val = loan.gross_irr
    tv = getattr(loan, "total_value", None)
    entry_amt = _credit_position_amount(loan)
    est_irr = getattr(loan, "estimated_irr_at_entry", None)

    # ----- Hard reds -----
    if ds in ("Default", "Restructured"):
        return "red"
    if ltv is not None and ltv > 0.90:
        return "red"
    if cov is not None and cov < 1.0:
        return "red"
    # Underwater: total_value < 80% of entry amount
    if tv is not None and entry_amt > 0 and tv < entry_amt * 0.8:
        return "red"
    if moic_val is not None and moic_val < 0.8:
        return "red"

    # ----- Yellows -----
    if ds == "Watch List":
        return "yellow"
    if ltv is not None and ltv > 0.75:
        return "yellow"
    if cov is not None and cov < 1.5:
        return "yellow"
    if covenant_ok is False:
        return "yellow"
    # Underperforming MOIC (0.8-1.0)
    if moic_val is not None and moic_val < 1.0:
        return "yellow"
    # IRR materially below entry expectation
    if irr_val is not None and est_irr is not None and irr_val < est_irr * 0.5:
        return "yellow"
    # Slightly underwater (total_value < entry)
    if tv is not None and entry_amt > 0 and tv < entry_amt:
        return "yellow"

    # ----- Greens -----
    # Performing with both LTV and coverage in good shape
    if ds == "Performing" and ltv is not None and cov is not None:
        return "green"
    # Realized with positive MOIC
    if status == "Realized" and moic_val is not None and moic_val >= 1.0:
        return "green"
    # Positive IRR signal
    if irr_val is not None and irr_val > 0:
        return "green"
    # MOIC >= 1.0
    if moic_val is not None and moic_val >= 1.0:
        return "green"
    # Performing with at least partial LTV/coverage signal
    if ds == "Performing" and (ltv is not None or cov is not None):
        return "green"

    return "gray"


def compute_traffic_lights(loans):
    """Compute portfolio-level traffic light signal and per-loan breakdown."""
    lights = {"green": 0.0, "yellow": 0.0, "red": 0.0, "gray": 0.0}
    total_hold = 0.0
    per_loan = []

    for loan in loans:
        signal = _loan_traffic_light(loan)
        hold = _credit_position_amount(loan)
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
    total_hold = sum(_credit_position_amount(l) for l in loans)

    for loan in loans:
        hold = _credit_position_amount(loan)
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
        hold_size = _credit_position_amount(loan)
        if loan.pik_toggle and loan.pik_rate and hold_size > 0:
            m = (metrics_by_id or {}).get(loan.id, {})
            pik_acc = m.get("pik_accrual", 0)
            if pik_acc > 0.20 * hold_size:
                concerns.append({
                    "company": loan.company_name,
                    "severity": 5,
                    "reason": "PIK accrual > 20% of hold",
                    "metric": f"PIK rate: {loan.pik_rate:.1%}",
                    "hold_pct": hold_pct,
                })

        # NEW: Severity 1 - Underwater (total_value < entry_loan_amount)
        entry_amt = _credit_position_amount(loan)
        tv = getattr(loan, 'total_value', None)
        if tv is not None and entry_amt > 0 and tv < entry_amt * 0.9:
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
    yield_count = 0
    ltv_count = 0
    spread_count = 0

    status_dist = defaultdict(int)
    rating_dist = defaultdict(int)
    fund_set = set()

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        hold = _credit_position_amount(loan, fx)
        issue = (loan.issue_size or 0.0) * fx

        total_issue += issue
        total_hold += hold

        if loan.coupon_rate is not None and hold > 0:
            weighted_yield += loan.coupon_rate * hold
            yield_denom += hold
            yield_count += 1

        if loan.current_ltv is not None and hold > 0:
            weighted_ltv += loan.current_ltv * hold
            ltv_denom += hold
            ltv_count += 1

        if loan.spread_bps is not None and hold > 0:
            weighted_spread += loan.spread_bps * hold
            spread_denom += hold
            spread_count += 1

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
    irr_count = 0
    weighted_moic = 0.0
    moic_denom = 0.0
    moic_count = 0
    weighted_cash_margin = 0.0
    cash_margin_denom = 0.0
    cash_margin_count = 0
    has_new_fields = False

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        weight = _credit_position_amount(loan, fx)

        committed = (getattr(loan, 'committed_amount', None) or 0.0) * fx
        cur_inv = (getattr(loan, 'current_invested_capital', None) or 0.0) * fx
        rp = (getattr(loan, 'realized_proceeds', None) or 0.0) * fx
        ulv = (getattr(loan, 'unrealized_loan_value', None) or 0.0) * fx
        uwev = (getattr(loan, 'unrealized_warrant_equity_value', None) or 0.0) * fx
        tv = (getattr(loan, 'total_value', None) or 0.0) * fx
        eq_inv = (getattr(loan, 'equity_investment', None) or 0.0) * fx

        total_committed += committed
        total_entry_loan += weight
        total_current_invested += cur_inv
        total_realized_proceeds += rp
        total_unrealized_loan += ulv
        total_unrealized_warrant += uwev
        total_total_value += tv
        total_equity_investment += eq_inv

        if loan.gross_irr is not None and weight > 0:
            weighted_irr += loan.gross_irr * weight
            irr_denom += weight
            irr_count += 1
        if loan.moic is not None and weight > 0:
            weighted_moic += loan.moic * weight
            moic_denom += weight
            moic_count += 1
        cm = getattr(loan, 'cash_margin', None)
        if cm is not None and weight > 0:
            weighted_cash_margin += cm * weight
            cash_margin_denom += weight
            cash_margin_count += 1

        if any(
            getattr(loan, f, None) is not None
            for f in ('entry_loan_amount', 'current_invested_capital', 'total_value', 'committed_amount')
        ):
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
        # Coverage counts: how many loans contributed to each weighted average
        "yield_coverage": yield_count,
        "ltv_coverage": ltv_count,
        "spread_coverage": spread_count,
        "irr_coverage": irr_count,
        "moic_coverage": moic_count,
        "cash_margin_coverage": cash_margin_count,
    }


# ---------------------------------------------------------------------------
# Risk-page metrics (dedicated)
# ---------------------------------------------------------------------------


def compute_credit_risk_metrics(loans, metrics_by_id=None):
    """Risk-specific aggregates for the credit-risk page.

    Returns the fields the risk template uses today (status / rating / LTV /
    IRR / MOIC) PLUS new aggregates for ICR, DSCR, covenant breach, and
    recovery rate that the parser ingests but no existing metric surfaces.

    Uses the same `entry_amt if entry_amt > 0 else hold` weighting convention
    as compute_credit_portfolio_analytics so the two functions stay consistent.
    """
    if metrics_by_id is None:
        metrics_by_id = {l.id: compute_credit_loan_metrics(l) for l in loans}

    weighted_ltv = 0.0
    ltv_denom = 0.0
    ltv_count = 0
    weighted_irr = 0.0
    irr_denom = 0.0
    irr_count = 0
    weighted_moic = 0.0
    moic_denom = 0.0
    moic_count = 0
    weighted_icr = 0.0
    icr_denom = 0.0
    weighted_dscr = 0.0
    dscr_denom = 0.0
    weighted_recovery = 0.0
    recovery_denom = 0.0

    total_total_value = 0.0
    total_entry_loan = 0.0
    total_hold = 0.0
    covenant_breach_hold = 0.0
    covenant_breach_count = 0
    loans_with_icr = 0
    loans_with_dscr = 0
    loans_with_covenant_data = 0
    has_new_fields = False

    status_dist = defaultdict(int)
    rating_dist = defaultdict(int)

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        hold = _credit_position_amount(loan, fx)
        weight = _credit_position_amount(loan, fx)

        total_hold += hold
        total_entry_loan += weight
        total_total_value += (getattr(loan, "total_value", None) or 0.0) * fx

        if loan.current_ltv is not None and weight > 0:
            weighted_ltv += loan.current_ltv * weight
            ltv_denom += weight
            ltv_count += 1

        if loan.gross_irr is not None and weight > 0:
            weighted_irr += loan.gross_irr * weight
            irr_denom += weight
            irr_count += 1
        if loan.moic is not None and weight > 0:
            weighted_moic += loan.moic * weight
            moic_denom += weight
            moic_count += 1

        if loan.interest_coverage_ratio is not None and weight > 0:
            weighted_icr += loan.interest_coverage_ratio * weight
            icr_denom += weight
            loans_with_icr += 1

        if loan.dscr is not None and weight > 0:
            weighted_dscr += loan.dscr * weight
            dscr_denom += weight
            loans_with_dscr += 1

        if loan.covenant_compliant is not None:
            loans_with_covenant_data += 1
            if loan.covenant_compliant is False:
                covenant_breach_count += 1
                covenant_breach_hold += weight if weight > 0 else hold

        rec_rate = getattr(loan, "recovery_rate", None)
        if rec_rate is not None and (loan.default_status or "") in ("Default", "Restructured"):
            default_weight = weight if weight > 0 else hold
            if default_weight > 0:
                weighted_recovery += rec_rate * default_weight
                recovery_denom += default_weight

        status_dist[loan.default_status or "Unknown"] += 1
        if loan.internal_credit_rating is not None:
            rating_dist[loan.internal_credit_rating] += 1

        if any(
            getattr(loan, f, None) is not None
            for f in ("entry_loan_amount", "current_invested_capital", "total_value", "committed_amount")
        ):
            has_new_fields = True

    # Collateral, coverage ratio, equity cushion aggregations
    collateral_num = 0.0
    collateral_den = 0.0
    collateral_count = 0
    coverage_ratio_num = 0.0
    coverage_ratio_den = 0.0
    coverage_ratio_count = 0
    equity_cushion_num = 0.0
    equity_cushion_den = 0.0
    equity_cushion_count = 0
    total_current_collateral = 0.0

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        weight = _credit_position_amount(loan, fx)

        cur_coll = getattr(loan, "current_collateral", None)
        if cur_coll is not None:
            total_current_collateral += cur_coll * fx
            collateral_count += 1

        cur_cov = getattr(loan, "current_coverage_ratio", None)
        if cur_cov is not None and weight > 0:
            coverage_ratio_num += cur_cov * weight
            coverage_ratio_den += weight
            coverage_ratio_count += 1

        cur_ec = getattr(loan, "current_equity_cushion", None)
        if cur_ec is not None and weight > 0:
            equity_cushion_num += cur_ec * weight
            equity_cushion_den += weight
            equity_cushion_count += 1

    performing_count = status_dist.get("Performing", 0)
    pct_performing = safe_divide(performing_count, len(loans), 0.0) if loans else 0.0
    covenant_breach_pct = safe_divide(covenant_breach_hold, total_hold) if total_hold > 0 else None

    return {
        # Existing fields the risk template already consumes
        "pct_performing": pct_performing,
        "wavg_ltv": safe_divide(weighted_ltv, ltv_denom),
        "status_distribution": dict(status_dist),
        "rating_distribution": dict(rating_dist),
        "weighted_avg_irr": safe_divide(weighted_irr, irr_denom),
        "weighted_avg_moic": safe_divide(weighted_moic, moic_denom),
        "total_total_value": total_total_value if has_new_fields else None,
        "total_entry_loan": total_entry_loan if has_new_fields else None,
        "has_new_fields": has_new_fields,
        "has_revenue_data": any(getattr(l, "ttm_revenue_entry", None) is not None for l in loans),
        # NEW: ICR / DSCR / covenant / recovery aggregates
        "wavg_icr": safe_divide(weighted_icr, icr_denom),
        "wavg_dscr": safe_divide(weighted_dscr, dscr_denom),
        "loans_with_icr_count": loans_with_icr,
        "loans_with_dscr_count": loans_with_dscr,
        "covenant_breach_count": covenant_breach_count,
        "covenant_breach_pct": covenant_breach_pct,
        "loans_with_covenant_data": loans_with_covenant_data,
        "wavg_recovery_rate": safe_divide(weighted_recovery, recovery_denom),
        # Coverage counts
        "ltv_coverage": ltv_count,
        "irr_coverage": irr_count,
        "moic_coverage": moic_count,
        # Collateral & coverage
        "total_current_collateral": total_current_collateral if collateral_count > 0 else None,
        "wavg_coverage_ratio": safe_divide(coverage_ratio_num, coverage_ratio_den),
        "wavg_equity_cushion": safe_divide(equity_cushion_num, equity_cushion_den),
        "collateral_coverage": collateral_count,
        "coverage_ratio_coverage": coverage_ratio_count,
        "equity_cushion_coverage": equity_cushion_count,
    }


# ---------------------------------------------------------------------------
# Snapshot coverage helper
# ---------------------------------------------------------------------------


def compute_snapshot_coverage(loans, snapshots_by_loan=None, *, required_field=None):
    """Snapshot coverage stats for the page-top coverage banner.

    Tells the caller "what fraction of loans have snapshot data" so the UI can
    show a coverage indicator and gracefully degrade when the snapshots upload
    sheet is sparse or missing entirely. Pages that depend on a specific field
    (e.g., Migration Matrix needs ``internal_credit_rating``) should pass that
    field via ``required_field`` so the coverage count reflects field-level
    availability rather than just "any snapshot exists".

    Args:
        loans: CreditLoan objects in the current portfolio scope.
        snapshots_by_loan: Dict mapping loan_id -> list of snapshots, sorted by
            ``snapshot_date`` ascending. ``None`` is treated as empty.
        required_field: Optional snapshot attribute name. When provided, a loan
            only counts as "covered" if at least one of its snapshots has a
            non-null value for that attribute.

    Returns:
        dict with:
            coverage_pct: float in [0.0, 1.0]
            loans_covered: int
            loans_total: int
            latest_snapshot_date: date or None (max snapshot_date across portfolio)
            per_field_coverage: {field: pct} when required_field given, else {}
    """
    snapshots_by_loan = snapshots_by_loan or {}
    loans_total = len(loans)

    if loans_total == 0:
        return {
            "coverage_pct": 0.0,
            "loans_covered": 0,
            "loans_total": 0,
            "latest_snapshot_date": None,
            "per_field_coverage": {},
        }

    loans_covered = 0
    latest_date = None

    for loan in loans:
        snaps = snapshots_by_loan.get(loan.id, [])
        if not snaps:
            continue

        # Track the latest snapshot date across the whole portfolio
        for s in snaps:
            sd = getattr(s, "snapshot_date", None)
            if sd is not None and (latest_date is None or sd > latest_date):
                latest_date = sd

        # Coverage check: required_field forces "field non-null in at least one snap"
        if required_field is None:
            loans_covered += 1
        else:
            if any(getattr(s, required_field, None) is not None for s in snaps):
                loans_covered += 1

    coverage_pct = safe_divide(loans_covered, loans_total) or 0.0

    per_field = {}
    if required_field is not None:
        per_field[required_field] = coverage_pct

    return {
        "coverage_pct": coverage_pct,
        "loans_covered": loans_covered,
        "loans_total": loans_total,
        "latest_snapshot_date": latest_date,
        "per_field_coverage": per_field,
    }


def _latest_snapshot_value(snapshots_by_loan, loan_id, field):
    """Return the most recent non-null snapshot value for a field, or None.

    Snapshots in ``snapshots_by_loan`` are sorted by ``snapshot_date`` ascending
    (see ``peqa/services/credit_filtering.py``), so we walk in reverse and return
    the first non-null hit. This lets the watchlist/migration/fundamentals
    aggregators ask "what's the latest reading of X for this loan" without
    re-sorting per call.
    """
    snaps = (snapshots_by_loan or {}).get(loan_id, [])
    for s in reversed(snaps):
        v = getattr(s, field, None)
        if v is not None:
            return v
    return None


def _snapshot_trend(snapshots_by_loan, loan_id, field):
    """Compute the earliest -> latest trend for a snapshot field.

    Returns a dict with the earliest and latest non-null values, the delta
    (latest - earliest), a "up"/"down"/"flat" direction tag, and the snapshot
    dates that bracket the trend. Returns ``None`` when fewer than two non-null
    samples exist (a single point isn't a trend; the watchlist must not score
    a fake "deteriorating" signal off one observation).

    Used by the watchlist scorer (LTV trend, ICR trend, EBITDA trend) and by
    the migration matrix to detect rating drift across the snapshot window.
    """
    snaps = (snapshots_by_loan or {}).get(loan_id, [])
    if not snaps:
        return None

    non_null = [s for s in snaps if getattr(s, field, None) is not None]
    if len(non_null) < 2:
        return None

    earliest = non_null[0]
    latest = non_null[-1]
    earliest_v = getattr(earliest, field)
    latest_v = getattr(latest, field)
    delta = latest_v - earliest_v

    if delta > 0:
        direction = "up"
    elif delta < 0:
        direction = "down"
    else:
        direction = "flat"

    return {
        "earliest": earliest_v,
        "latest": latest_v,
        "delta": delta,
        "direction": direction,
        "earliest_date": getattr(earliest, "snapshot_date", None),
        "latest_date": getattr(latest, "snapshot_date", None),
    }


# ---------------------------------------------------------------------------
# Migration matrix
# ---------------------------------------------------------------------------


# Default rating labels for the standard 1-5 internal scale used by most
# credit funds. The matrix dynamically expands if a portfolio uses ratings
# outside this range; labels are looked up by integer key.
DEFAULT_RATING_LABELS = {
    1: "AAA",
    2: "AA",
    3: "A",
    4: "BBB",
    5: "BB+/Below",
}


def compute_credit_migration_matrix(loans, metrics_by_id=None, *, snapshots_by_loan=None):
    """Internal credit rating migration across the snapshot window.

    For each loan, finds the earliest non-null ``internal_credit_rating`` from
    its snapshots and the latest (snapshot or current loan field, whichever is
    most recent), then bins the from -> to transitions into a 2D matrix. Loans
    without two distinct rating observations are excluded from the matrix and
    counted separately as ``loans_without_history``.

    The matrix is a flat list-of-lists indexed by ``rating_order`` so the
    template can render it with two nested ``{% for %}`` loops without any
    nested-dict gymnastics.

    Args:
        loans: CreditLoan objects in scope.
        metrics_by_id: Unused, accepted for signature consistency with other
            page-level metric functions.
        snapshots_by_loan: Dict mapping loan_id -> list of snapshots sorted by
            snapshot_date asc.

    Returns:
        dict with rating_order, rating_labels, matrix (rows=from, cols=to),
        totals_from, totals_to, upgrade/stable/downgrade counts, at_risk_loans
        (downgraded by >= 1 notch, sorted by severity), loans_with_rating_data,
        loans_without_history, and a coverage block from compute_snapshot_coverage.
    """
    snapshots_by_loan = snapshots_by_loan or {}

    # Step 1: collect (from_rating, to_rating, loan) triples for every loan
    # that has at least two rating observations.
    transitions = []
    loans_without_history = 0
    loans_with_any_rating = 0

    # Discover the rating universe from actual data so the matrix scales to
    # whatever rating scale this portfolio uses.
    rating_universe = set()

    for loan in loans:
        # Earliest snapshot rating
        snap_trend = _snapshot_trend(snapshots_by_loan, loan.id, "internal_credit_rating")

        if snap_trend is not None:
            from_rating = snap_trend["earliest"]
            to_rating = snap_trend["latest"]
            loans_with_any_rating += 1
            rating_universe.add(int(from_rating))
            rating_universe.add(int(to_rating))
            transitions.append((int(from_rating), int(to_rating), loan))
        else:
            # Maybe one snapshot OR no snapshots, but loan still has a current
            # rating on the row itself. Track for the "no history" stat so the
            # template can show "12 loans rated, 0 history yet".
            current = getattr(loan, "internal_credit_rating", None)
            latest_snap = _latest_snapshot_value(
                snapshots_by_loan, loan.id, "internal_credit_rating"
            )
            if current is not None or latest_snap is not None:
                loans_with_any_rating += 1
                loans_without_history += 1
                if current is not None:
                    rating_universe.add(int(current))
                if latest_snap is not None:
                    rating_universe.add(int(latest_snap))

    # Always include the standard 1-5 range so the matrix doesn't look sparse
    # for small portfolios. Caller can crop visually if needed.
    rating_universe.update(DEFAULT_RATING_LABELS.keys())
    rating_order = sorted(rating_universe)
    rating_labels = [DEFAULT_RATING_LABELS.get(r, str(r)) for r in rating_order]
    rating_index = {r: i for i, r in enumerate(rating_order)}

    n = len(rating_order)
    matrix = [[0 for _ in range(n)] for _ in range(n)]

    upgrades = 0
    stable = 0
    downgrades = 0
    at_risk = []

    for from_r, to_r, loan in transitions:
        i = rating_index[from_r]
        j = rating_index[to_r]
        matrix[i][j] += 1

        # Lower integer = better rating, so to_r > from_r is a downgrade
        if to_r < from_r:
            upgrades += 1
        elif to_r == from_r:
            stable += 1
        else:
            downgrades += 1
            at_risk.append({
                "loan_id": loan.id,
                "company": getattr(loan, "company_name", None),
                "fund": getattr(loan, "fund_name", None),
                "sector": getattr(loan, "sector", None),
                "from_rating": from_r,
                "from_label": DEFAULT_RATING_LABELS.get(from_r, str(from_r)),
                "to_rating": to_r,
                "to_label": DEFAULT_RATING_LABELS.get(to_r, str(to_r)),
                "notches_down": to_r - from_r,
            })

    # Sort at_risk by severity (most notches down first), then by company name
    # for stable ordering.
    at_risk.sort(key=lambda r: (-r["notches_down"], r["company"] or ""))

    totals_from = [sum(matrix[i]) for i in range(n)]
    totals_to = [sum(matrix[i][j] for i in range(n)) for j in range(n)]

    coverage = compute_snapshot_coverage(
        loans, snapshots_by_loan, required_field="internal_credit_rating"
    )

    return {
        "rating_order": rating_order,
        "rating_labels": rating_labels,
        "matrix": matrix,
        "totals_from": totals_from,
        "totals_to": totals_to,
        "upgrades_count": upgrades,
        "stable_count": stable,
        "downgrades_count": downgrades,
        "at_risk_loans": at_risk,
        "loans_with_rating_data": loans_with_any_rating,
        "loans_with_migration_history": len(transitions),
        "loans_without_history": loans_without_history,
        "coverage": coverage,
    }


# ---------------------------------------------------------------------------
# Borrower fundamentals
# ---------------------------------------------------------------------------


FUNDAMENTAL_METRIC_DEFS = (
    {
        "key": "revenue",
        "label": "Revenue",
        "kind": "currency",
        "entry_fields": ("entry_revenue", "ttm_revenue_entry"),
        "current_fields": ("current_revenue", "ttm_revenue_current"),
        "snapshot_field": "current_revenue",
        "use_fx": True,
    },
    {
        "key": "ltv",
        "label": "LTV",
        "kind": "percent",
        "entry_fields": ("entry_ltv",),
        "current_fields": ("current_ltv",),
        "snapshot_field": "current_ltv",
        "use_fx": False,
    },
    {
        "key": "coverage_ratio",
        "label": "Coverage Ratio",
        "kind": "multiple",
        "entry_fields": ("entry_coverage_ratio",),
        "current_fields": ("current_coverage_ratio",),
        "snapshot_field": None,
        "use_fx": False,
    },
    {
        "key": "equity_cushion",
        "label": "Equity Cushion",
        "kind": "percent",
        "entry_fields": ("entry_equity_cushion",),
        "current_fields": ("current_equity_cushion",),
        "snapshot_field": None,
        "use_fx": False,
    },
)


def _credit_numeric(value):
    """Coerce a value to float when it is truly numeric, else return None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        coerced = float(value)
        return None if coerced != coerced else coerced
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    return None if coerced != coerced else coerced


def _credit_current_invested_weight(loan, fx=1.0):
    """Weight fundamentals strictly by current invested capital."""
    current_invested = _credit_numeric(getattr(loan, "current_invested_capital", None))
    if current_invested is None or current_invested <= 0:
        return 0.0
    return current_invested * fx


def _credit_metric_field_value(loan, field_names, *, fx=1.0, use_fx=False):
    """Return the first non-null numeric field from a candidate field list."""
    for field_name in field_names:
        value = _credit_numeric(getattr(loan, field_name, None))
        if value is not None:
            return value * fx if use_fx else value
    return None


def _credit_metric_current_value(loan, metric_def, snapshots_by_loan=None, *, fx=1.0):
    """Resolve the exit/current metric from the loan row, then snapshots if available."""
    current_value = _credit_metric_field_value(
        loan,
        metric_def["current_fields"],
        fx=fx,
        use_fx=metric_def["use_fx"],
    )
    if current_value is not None:
        return current_value

    snapshot_field = metric_def.get("snapshot_field")
    if snapshot_field:
        snapshot_value = _credit_numeric(
            _latest_snapshot_value(snapshots_by_loan, getattr(loan, "id", None), snapshot_field)
        )
        if snapshot_value is not None:
            return snapshot_value * fx if metric_def["use_fx"] else snapshot_value
    return None


def _credit_metric_summary(metric_def, records):
    """Aggregate entry/current/delta on weighted and simple-average bases."""
    entry_sum = 0.0
    entry_count = 0
    current_sum = 0.0
    current_count = 0
    delta_sum = 0.0
    delta_count = 0

    weighted_entry_num = 0.0
    weighted_entry_den = 0.0
    weighted_current_num = 0.0
    weighted_current_den = 0.0
    weighted_delta_num = 0.0
    weighted_delta_den = 0.0
    weighted_paired_count = 0

    for record in records:
        entry_value = record["entry"]
        current_value = record["current"]
        delta_value = record["delta"]
        weight = record["weight"]

        if entry_value is not None:
            entry_sum += entry_value
            entry_count += 1
            if weight > 0:
                weighted_entry_num += entry_value * weight
                weighted_entry_den += weight

        if current_value is not None:
            current_sum += current_value
            current_count += 1
            if weight > 0:
                weighted_current_num += current_value * weight
                weighted_current_den += weight

        if delta_value is not None:
            delta_sum += delta_value
            delta_count += 1
            if weight > 0:
                weighted_delta_num += delta_value * weight
                weighted_delta_den += weight
                weighted_paired_count += 1

    return {
        "key": metric_def["key"],
        "label": metric_def["label"],
        "kind": metric_def["kind"],
        "weighted_average_entry": safe_divide(weighted_entry_num, weighted_entry_den),
        "weighted_average_exit_current": safe_divide(weighted_current_num, weighted_current_den),
        "weighted_average_delta": safe_divide(weighted_delta_num, weighted_delta_den),
        "average_entry": safe_divide(entry_sum, entry_count),
        "average_exit_current": safe_divide(current_sum, current_count),
        "average_delta": safe_divide(delta_sum, delta_count),
        "entry_count": entry_count,
        "exit_current_count": current_count,
        "paired_count": delta_count,
        "weighted_paired_count": weighted_paired_count,
    }


def compute_credit_fundamentals(loans, metrics_by_id=None, *, snapshots_by_loan=None):
    """Borrower fundamentals plus entry vs exit/current analysis.

    Reads ``current_revenue`` and ``current_ebitda`` from the snapshot history
    via ``_snapshot_trend``, then aggregates a hold-weighted growth rate so the
    portfolio-level KPI matches what an analyst would compute by hand. Loans
    without two non-null observations for a field are excluded from that
    field's aggregate (silently dropping them would skew the average).

    The page also surfaces the "deteriorating" tail: every loan whose latest
    EBITDA or revenue is below its earliest. Sorted by absolute decline so the
    biggest drops are first, and capped to keep the table scannable.

    Returns the standard ``coverage`` block plus the field-level aggregates,
    grower / decliner lists, and entry vs exit/current summaries at the
    portfolio, fund, and deal level. Weighted fundamentals always use
    ``current_invested_capital`` only.
    """
    snapshots_by_loan = snapshots_by_loan or {}

    weighted_revenue_growth_num = 0.0
    weighted_revenue_growth_denom = 0.0
    weighted_ebitda_growth_num = 0.0
    weighted_ebitda_growth_denom = 0.0

    loans_with_revenue_trend = 0
    loans_with_ebitda_trend = 0

    rows = []
    revenue_decliners = []
    ebitda_decliners = []
    overall_metric_records = {metric["key"]: [] for metric in FUNDAMENTAL_METRIC_DEFS}
    fund_buckets = {}
    deal_rows = []
    total_current_invested_capital = 0.0
    weighted_loan_count = 0

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        weight = _credit_current_invested_weight(loan, fx)
        if weight > 0:
            total_current_invested_capital += weight
            weighted_loan_count += 1

        rev_trend = _snapshot_trend(snapshots_by_loan, loan.id, "current_revenue")
        ebitda_trend = _snapshot_trend(snapshots_by_loan, loan.id, "current_ebitda")

        rev_growth = None
        if rev_trend is not None and rev_trend["earliest"] not in (0, None):
            rev_growth = (rev_trend["latest"] - rev_trend["earliest"]) / rev_trend["earliest"]
            loans_with_revenue_trend += 1
            if weight > 0:
                weighted_revenue_growth_num += rev_growth * weight
                weighted_revenue_growth_denom += weight
            if rev_trend["delta"] < 0:
                revenue_decliners.append({
                    "loan_id": loan.id,
                    "company": getattr(loan, "company_name", None),
                    "fund": getattr(loan, "fund_name", None),
                    "sector": getattr(loan, "sector", None),
                    "earliest": rev_trend["earliest"],
                    "latest": rev_trend["latest"],
                    "delta": rev_trend["delta"],
                    "growth_pct": rev_growth,
                })

        ebitda_growth = None
        if ebitda_trend is not None and ebitda_trend["earliest"] not in (0, None):
            ebitda_growth = (
                ebitda_trend["latest"] - ebitda_trend["earliest"]
            ) / ebitda_trend["earliest"]
            loans_with_ebitda_trend += 1
            if weight > 0:
                weighted_ebitda_growth_num += ebitda_growth * weight
                weighted_ebitda_growth_denom += weight
            if ebitda_trend["delta"] < 0:
                ebitda_decliners.append({
                    "loan_id": loan.id,
                    "company": getattr(loan, "company_name", None),
                    "fund": getattr(loan, "fund_name", None),
                    "sector": getattr(loan, "sector", None),
                    "earliest": ebitda_trend["earliest"],
                    "latest": ebitda_trend["latest"],
                    "delta": ebitda_trend["delta"],
                    "growth_pct": ebitda_growth,
                })

        # Per-loan row for the main table — only include loans with at least
        # one fundamental trend so we don't fill the table with blanks.
        if rev_trend is not None or ebitda_trend is not None:
            rows.append({
                "loan_id": loan.id,
                "company": getattr(loan, "company_name", None),
                "fund": getattr(loan, "fund_name", None),
                "sector": getattr(loan, "sector", None),
                "revenue_earliest": rev_trend["earliest"] if rev_trend else None,
                "revenue_latest": rev_trend["latest"] if rev_trend else None,
                "revenue_growth_pct": rev_growth,
                "revenue_direction": rev_trend["direction"] if rev_trend else None,
                "ebitda_earliest": ebitda_trend["earliest"] if ebitda_trend else None,
                "ebitda_latest": ebitda_trend["latest"] if ebitda_trend else None,
                "ebitda_growth_pct": ebitda_growth,
                "ebitda_direction": ebitda_trend["direction"] if ebitda_trend else None,
            })

        fund_name = getattr(loan, "fund_name", None) or "Unassigned"
        if fund_name not in fund_buckets:
            fund_buckets[fund_name] = {
                "fund_name": fund_name,
                "loan_count": 0,
                "total_current_invested_capital": 0.0,
                "metric_records": {metric["key"]: [] for metric in FUNDAMENTAL_METRIC_DEFS},
            }
        fund_bucket = fund_buckets[fund_name]
        fund_bucket["loan_count"] += 1
        fund_bucket["total_current_invested_capital"] += weight

        deal_row = {
            "loan_id": loan.id,
            "company_name": getattr(loan, "company_name", None),
            "fund_name": fund_name,
            "status": getattr(loan, "status", None) or getattr(loan, "default_status", None),
            "exit_current_label": (
                "Exit"
                if getattr(loan, "exit_date", None) is not None
                or (getattr(loan, "status", "") or "").strip().lower() == "realized"
                else "Current"
            ),
            "current_invested_capital": weight if weight > 0 else None,
        }

        for metric_def in FUNDAMENTAL_METRIC_DEFS:
            entry_value = _credit_metric_field_value(
                loan,
                metric_def["entry_fields"],
                fx=fx,
                use_fx=metric_def["use_fx"],
            )
            current_value = _credit_metric_current_value(
                loan,
                metric_def,
                snapshots_by_loan=snapshots_by_loan,
                fx=fx,
            )
            delta_value = (
                current_value - entry_value
                if entry_value is not None and current_value is not None
                else None
            )

            record = {
                "entry": entry_value,
                "current": current_value,
                "delta": delta_value,
                "weight": weight,
            }
            overall_metric_records[metric_def["key"]].append(record)
            fund_bucket["metric_records"][metric_def["key"]].append(record)

            deal_row[f"{metric_def['key']}_entry"] = entry_value
            deal_row[f"{metric_def['key']}_exit_current"] = current_value
            deal_row[f"{metric_def['key']}_delta"] = delta_value

        revenue_entry = deal_row["revenue_entry"]
        revenue_exit_current = deal_row["revenue_exit_current"]
        deal_row["revenue_delta_pct"] = (
            safe_divide(revenue_exit_current - revenue_entry, revenue_entry)
            if revenue_entry not in (None, 0) and revenue_exit_current is not None
            else None
        )
        deal_rows.append(deal_row)

    # Sort decliners by absolute delta (worst first)
    revenue_decliners.sort(key=lambda r: r["delta"])
    ebitda_decliners.sort(key=lambda r: r["delta"])

    # Sort the main rows: ebitda decliners first, then by company
    rows.sort(key=lambda r: (
        0 if (r["ebitda_direction"] == "down" or r["revenue_direction"] == "down") else 1,
        r["company"] or "",
    ))

    # --- Loan-level fundamentals (no snapshots needed) ---
    # The loan model has entry_revenue, entry_ebitda, current_revenue,
    # current_ebitda fields that are uploaded directly. These surface even
    # when no snapshot history exists.
    loan_level_rows = []
    loans_with_loan_revenue = 0
    loans_with_loan_ebitda = 0
    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        e_rev = getattr(loan, "entry_revenue", None)
        c_rev = getattr(loan, "current_revenue", None)
        e_ebitda = getattr(loan, "entry_ebitda", None)
        c_ebitda = getattr(loan, "current_ebitda", None)
        ttm_rev_entry = getattr(loan, "ttm_revenue_entry", None)
        ttm_rev_current = getattr(loan, "ttm_revenue_current", None)

        # Use TTM revenue fields as fallback for entry/current revenue
        rev_entry = e_rev if e_rev is not None else ttm_rev_entry
        rev_current = c_rev if c_rev is not None else ttm_rev_current

        has_any = any(v is not None for v in (rev_entry, rev_current, e_ebitda, c_ebitda))
        if not has_any:
            continue

        if rev_entry is not None or rev_current is not None:
            loans_with_loan_revenue += 1
        if e_ebitda is not None or c_ebitda is not None:
            loans_with_loan_ebitda += 1

        rev_growth = None
        rev_dir = None
        if rev_entry is not None and rev_entry > 0 and rev_current is not None:
            rev_growth = (rev_current - rev_entry) / rev_entry
            rev_dir = "up" if rev_growth > 0.001 else ("down" if rev_growth < -0.001 else "flat")

        ebitda_growth_ll = None
        ebitda_dir = None
        if e_ebitda is not None and e_ebitda > 0 and c_ebitda is not None:
            ebitda_growth_ll = (c_ebitda - e_ebitda) / e_ebitda
            ebitda_dir = "up" if ebitda_growth_ll > 0.001 else ("down" if ebitda_growth_ll < -0.001 else "flat")

        loan_level_rows.append({
            "loan_id": loan.id,
            "company": loan.company_name,
            "fund": loan.fund_name,
            "sector": getattr(loan, "sector", None),
            "revenue_entry": (rev_entry * fx) if rev_entry is not None else None,
            "revenue_current": (rev_current * fx) if rev_current is not None else None,
            "revenue_growth_pct": rev_growth,
            "revenue_direction": rev_dir,
            "ebitda_entry": (e_ebitda * fx) if e_ebitda is not None else None,
            "ebitda_current": (c_ebitda * fx) if c_ebitda is not None else None,
            "ebitda_growth_pct": ebitda_growth_ll,
            "ebitda_direction": ebitda_dir,
        })

    loan_level_rows.sort(key=lambda r: (
        0 if (r["ebitda_direction"] == "down" or r["revenue_direction"] == "down") else 1,
        r["company"] or "",
    ))

    summary_metrics = [
        _credit_metric_summary(metric_def, overall_metric_records[metric_def["key"]])
        for metric_def in FUNDAMENTAL_METRIC_DEFS
    ]
    summary_by_key = {summary["key"]: summary for summary in summary_metrics}

    fund_rows = []
    for fund_name in sorted(fund_buckets):
        bucket = fund_buckets[fund_name]
        metric_summaries = {
            metric_def["key"]: _credit_metric_summary(
                metric_def, bucket["metric_records"][metric_def["key"]]
            )
            for metric_def in FUNDAMENTAL_METRIC_DEFS
        }
        fund_rows.append({
            "fund_name": bucket["fund_name"],
            "loan_count": bucket["loan_count"],
            "total_current_invested_capital": bucket["total_current_invested_capital"],
            "metrics": metric_summaries,
        })

    fund_metric_tables = []
    for metric_def in FUNDAMENTAL_METRIC_DEFS:
        table_rows = []
        for fund_row in fund_rows:
            summary = fund_row["metrics"][metric_def["key"]]
            table_rows.append({
                "fund_name": fund_row["fund_name"],
                "loan_count": fund_row["loan_count"],
                "total_current_invested_capital": fund_row["total_current_invested_capital"],
                **summary,
            })
        fund_metric_tables.append({
            "key": metric_def["key"],
            "label": metric_def["label"],
            "kind": metric_def["kind"],
            "rows": table_rows,
        })

    deal_rows.sort(key=lambda row: (row["fund_name"] or "", row["company_name"] or ""))

    coverage_revenue = compute_snapshot_coverage(
        loans, snapshots_by_loan, required_field="current_revenue"
    )
    coverage_ebitda = compute_snapshot_coverage(
        loans, snapshots_by_loan, required_field="current_ebitda"
    )
    # Top-level coverage banner uses ebitda since that's the primary fundamental
    coverage = compute_snapshot_coverage(loans, snapshots_by_loan)
    coverage["per_field_coverage"] = {
        "current_revenue": coverage_revenue["per_field_coverage"].get("current_revenue", 0.0),
        "current_ebitda": coverage_ebitda["per_field_coverage"].get("current_ebitda", 0.0),
    }

    return {
        "wavg_revenue_growth": safe_divide(
            weighted_revenue_growth_num, weighted_revenue_growth_denom
        ),
        "wavg_ebitda_growth": safe_divide(
            weighted_ebitda_growth_num, weighted_ebitda_growth_denom
        ),
        "loans_with_revenue_trend": loans_with_revenue_trend,
        "loans_with_ebitda_trend": loans_with_ebitda_trend,
        "rows": rows,
        "revenue_decliners": revenue_decliners,
        "ebitda_decliners": ebitda_decliners,
        "coverage": coverage,
        # Loan-level fundamentals (from upload, no snapshots needed)
        "loan_level_rows": loan_level_rows,
        "loans_with_loan_revenue": loans_with_loan_revenue,
        "loans_with_loan_ebitda": loans_with_loan_ebitda,
        "loan_count": len(loans),
        "fund_count": len(fund_rows),
        "weighted_loan_count": weighted_loan_count,
        "total_current_invested_capital": total_current_invested_capital,
        "summary_metrics": summary_metrics,
        "summary_by_key": summary_by_key,
        "fund_rows": fund_rows,
        "fund_metric_tables": fund_metric_tables,
        "deal_rows": deal_rows,
        "exit_current_label": "Exit / Current",
    }


# ---------------------------------------------------------------------------
# Watchlist scoring
# ---------------------------------------------------------------------------


def _score_loan(loan, snapshots_by_loan, metrics_by_id, sponsor_history):
    """Compute the watchlist score for a single loan with full breakdown.

    Returns a dict with ``score`` (int 0-100), ``bucket`` (urgent/attention/monitor/clear),
    and ``breakdown`` (list of {factor, points, reason}). The breakdown is the
    point of this function: every point added must be auditable so analysts can
    interrogate why a loan scored what it did.

    Hard triggers (status non-Performing, covenant breach) cap the score at the
    ``STATUS_AUTO`` weight regardless of trend signals. Trend signals stack but
    are individually capped to their weights.
    """
    breakdown = []
    score = 0

    # Hard trigger 1: status. "Default", "Restructured", "Watch", "Non-Accrual"
    # all jump to the auto threshold.
    status = (getattr(loan, "default_status", None) or "").strip()
    if status and status.lower() != "performing":
        breakdown.append({
            "factor": "status",
            "points": WATCHLIST_WEIGHT_STATUS_AUTO,
            "reason": f"Status: {status}",
        })
        score += WATCHLIST_WEIGHT_STATUS_AUTO

    # Hard trigger 2: covenant breach. Explicit False (not None).
    if getattr(loan, "covenant_compliant", None) is False:
        breakdown.append({
            "factor": "covenant",
            "points": WATCHLIST_WEIGHT_COVENANT,
            "reason": "Covenant non-compliant",
        })
        score += WATCHLIST_WEIGHT_COVENANT

    # Trend signal: LTV climbing across snapshots. Larger climb = more points,
    # capped at WATCHLIST_WEIGHT_LTV_TREND.
    ltv_trend = _snapshot_trend(snapshots_by_loan, loan.id, "current_ltv")
    if ltv_trend is not None and ltv_trend["delta"] > 0:
        # Linear scale: 0 -> 0 points, threshold -> full points, beyond -> capped
        scale = min(1.0, ltv_trend["delta"] / WATCHLIST_LTV_TREND_DELTA_THRESHOLD)
        points = int(round(WATCHLIST_WEIGHT_LTV_TREND * scale))
        if points > 0:
            breakdown.append({
                "factor": "ltv_trend",
                "points": points,
                "reason": (
                    f"LTV climbed {ltv_trend['earliest']:.2f} -> "
                    f"{ltv_trend['latest']:.2f}"
                ),
            })
            score += points

    # Trend signal: ICR (interest coverage) deteriorating across snapshots.
    icr_trend = _snapshot_trend(snapshots_by_loan, loan.id, "interest_coverage_ratio")
    if icr_trend is not None and icr_trend["delta"] < 0:
        scale = min(1.0, abs(icr_trend["delta"]) / WATCHLIST_ICR_TREND_DELTA_THRESHOLD)
        points = int(round(WATCHLIST_WEIGHT_ICR_TREND * scale))
        if points > 0:
            breakdown.append({
                "factor": "icr_trend",
                "points": points,
                "reason": (
                    f"ICR fell {icr_trend['earliest']:.2f}x -> "
                    f"{icr_trend['latest']:.2f}x"
                ),
            })
            score += points

    # Trend signal: EBITDA falling across snapshots.
    ebitda_trend = _snapshot_trend(snapshots_by_loan, loan.id, "current_ebitda")
    if ebitda_trend is not None and ebitda_trend["delta"] < 0:
        breakdown.append({
            "factor": "ebitda_trend",
            "points": WATCHLIST_WEIGHT_EBITDA_TREND,
            "reason": (
                f"EBITDA fell {ebitda_trend['earliest']:.1f} -> "
                f"{ebitda_trend['latest']:.1f}"
            ),
        })
        score += WATCHLIST_WEIGHT_EBITDA_TREND

    # Sponsor history: this sponsor has had at least one prior default or
    # restructuring in the same portfolio. sponsor_history is a dict
    # {sponsor_name: {"defaults": int, "restructured": int}} built once by
    # the caller from the same loan list (avoids per-loan recompute).
    sponsor = getattr(loan, "sponsor", None)
    if sponsor and sponsor_history:
        prior = sponsor_history.get(sponsor, {})
        prior_count = prior.get("defaults", 0) + prior.get("restructured", 0)
        if prior_count > 0:
            breakdown.append({
                "factor": "sponsor_history",
                "points": WATCHLIST_WEIGHT_SPONSOR_HISTORY,
                "reason": f"Sponsor {sponsor} has {prior_count} prior problem loan(s)",
            })
            score += WATCHLIST_WEIGHT_SPONSOR_HISTORY

    # Floor rolloff: if loan has a floor and it's below the typical rate cut
    # buffer, the loan is vulnerable to losing floor protection in a rate-cut
    # cycle. Heuristic-only, low weight.
    floor_rate = getattr(loan, "floor_rate", None)
    fixed_or_floating = (getattr(loan, "fixed_or_floating", None) or "").lower()
    if (
        fixed_or_floating == "floating"
        and floor_rate is not None
        and floor_rate < WATCHLIST_LOW_FLOOR_THRESHOLD
    ):
        breakdown.append({
            "factor": "floor_rolloff",
            "points": WATCHLIST_WEIGHT_FLOOR_ROLLOFF,
            "reason": f"Low floor ({floor_rate:.2%}) vulnerable to rate cuts",
        })
        score += WATCHLIST_WEIGHT_FLOOR_ROLLOFF

    # Cap at 100. The hard triggers already push above this individually, so
    # we cap after the fact rather than gating each adder.
    capped_score = min(score, WATCHLIST_SCORE_CAP)

    # Bucket assignment
    if capped_score >= WATCHLIST_URGENT_THRESHOLD:
        bucket = "urgent"
    elif capped_score >= WATCHLIST_ATTENTION_THRESHOLD:
        bucket = "attention"
    elif capped_score >= WATCHLIST_MONITOR_THRESHOLD:
        bucket = "monitor"
    else:
        bucket = "clear"

    return {
        "score": capped_score,
        "raw_score": score,  # Pre-cap, for debugging
        "bucket": bucket,
        "breakdown": breakdown,
    }


def _build_sponsor_history(loans):
    """Aggregate per-sponsor problem-loan counts for the watchlist scorer.

    Returns ``{sponsor: {"defaults": int, "restructured": int}}``. Built once
    per page render so the per-loan scorer can do an O(1) dict lookup.
    """
    history = defaultdict(lambda: {"defaults": 0, "restructured": 0})
    for loan in loans:
        sponsor = getattr(loan, "sponsor", None)
        if not sponsor:
            continue
        status = (getattr(loan, "default_status", None) or "").strip().lower()
        if status == "default":
            history[sponsor]["defaults"] += 1
        elif status == "restructured":
            history[sponsor]["restructured"] += 1
    return dict(history)


def compute_credit_watchlist(
    loans, metrics_by_id=None, *, snapshots_by_loan=None, sponsor_history=None
):
    """Score every loan against the watchlist rubric and bucket the results.

    Returns the per-loan rows sorted by score desc, plus bucket counts and a
    coverage block. The page template renders the rows in three sections
    (urgent, attention, monitor) and lets the user expand each row to see the
    full point-by-point breakdown — that's how an analyst defends "this is on
    the watchlist because..." in a quarterly review.
    """
    snapshots_by_loan = snapshots_by_loan or {}

    if sponsor_history is None:
        sponsor_history = _build_sponsor_history(loans)

    rows = []
    bucket_counts = {"urgent": 0, "attention": 0, "monitor": 0, "clear": 0}

    for loan in loans:
        scored = _score_loan(loan, snapshots_by_loan, metrics_by_id, sponsor_history)

        fx = loan.fx_rate_to_usd or 1.0
        hold = _credit_position_amount(loan, fx)

        rows.append({
            "loan_id": loan.id,
            "company": getattr(loan, "company_name", None),
            "fund": getattr(loan, "fund_name", None),
            "sector": getattr(loan, "sector", None),
            "sponsor": getattr(loan, "sponsor", None),
            "hold_size_usd": hold,
            "default_status": getattr(loan, "default_status", None),
            "score": scored["score"],
            "bucket": scored["bucket"],
            "breakdown": scored["breakdown"],
        })

        bucket_counts[scored["bucket"]] += 1

    # Sort by score desc, then hold size desc (bigger problems first), then
    # company name for stability.
    rows.sort(key=lambda r: (-r["score"], -r["hold_size_usd"], r["company"] or ""))

    coverage = compute_snapshot_coverage(loans, snapshots_by_loan)

    return {
        "rows": rows,
        "bucket_counts": bucket_counts,
        "urgent_count": bucket_counts["urgent"],
        "attention_count": bucket_counts["attention"],
        "monitor_count": bucket_counts["monitor"],
        "clear_count": bucket_counts["clear"],
        "total_loans": len(loans),
        "coverage": coverage,
        "weights": {
            "ltv_trend": WATCHLIST_WEIGHT_LTV_TREND,
            "icr_trend": WATCHLIST_WEIGHT_ICR_TREND,
            "ebitda_trend": WATCHLIST_WEIGHT_EBITDA_TREND,
            "status_auto": WATCHLIST_WEIGHT_STATUS_AUTO,
            "covenant": WATCHLIST_WEIGHT_COVENANT,
            "sponsor_history": WATCHLIST_WEIGHT_SPONSOR_HISTORY,
            "floor_rolloff": WATCHLIST_WEIGHT_FLOOR_ROLLOFF,
        },
        "thresholds": {
            "urgent": WATCHLIST_URGENT_THRESHOLD,
            "attention": WATCHLIST_ATTENTION_THRESHOLD,
            "monitor": WATCHLIST_MONITOR_THRESHOLD,
        },
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
        entry_amt = _credit_position_amount(loan, fx)
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

    # --- Rate mix & spread ---
    fixed_count = 0
    fixed_hold = 0.0
    pik_count = 0
    pik_hold = 0.0
    spread_num = 0.0
    spread_den = 0.0
    spread_count = 0
    yield_total_hold = 0.0

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        hold = _credit_position_amount(loan, fx)
        yield_total_hold += hold
        rate_type = (getattr(loan, "fixed_or_floating", None) or "").lower()
        if rate_type == "fixed":
            fixed_count += 1
            fixed_hold += hold
        if loan.pik_toggle:
            pik_count += 1
            pik_hold += hold
        if loan.spread_bps is not None and hold > 0:
            spread_num += loan.spread_bps * hold
            spread_den += hold
            spread_count += 1

    wavg_spread_bps = safe_divide(spread_num, spread_den)

    # --- Floor coverage ---
    # For floating-rate loans, the floor is the minimum reference rate. If the
    # reference rate (e.g. SOFR) drops below the floor, the loan still pays the
    # floor rate. This is yield protection in a rate-cut cycle. The page-level
    # question is: what share of the floating book has a floor set, and at
    # what hold-weighted rate?
    floating_loans_count = 0
    floating_loans_with_floor_count = 0
    floating_hold_total = 0.0
    floating_hold_with_floor = 0.0
    floor_rate_weighted_sum = 0.0
    floor_weight_total = 0.0

    for loan in loans:
        if (getattr(loan, "fixed_or_floating", None) or "").lower() != "floating":
            continue
        fx = loan.fx_rate_to_usd or 1.0
        hold = _credit_position_amount(loan, fx)
        floating_loans_count += 1
        floating_hold_total += hold

        floor = getattr(loan, "floor_rate", None)
        if floor is not None and floor > 0:
            floating_loans_with_floor_count += 1
            floating_hold_with_floor += hold
            if hold > 0:
                floor_rate_weighted_sum += floor * hold
                floor_weight_total += hold

    floor_coverage_pct = safe_divide(
        floating_loans_with_floor_count, floating_loans_count
    )
    wavg_floor_rate = safe_divide(floor_rate_weighted_sum, floor_weight_total)

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
        # --- Rate mix & spread ---
        "wavg_spread_bps": wavg_spread_bps,
        "fixed_count": fixed_count,
        "fixed_hold": fixed_hold,
        "pik_count": pik_count,
        "pik_hold": pik_hold,
        "floating_pct": safe_divide(floating_hold_total, yield_total_hold),
        "fixed_pct": safe_divide(fixed_hold, yield_total_hold),
        "pik_pct": safe_divide(pik_hold, yield_total_hold),
        "spread_coverage": spread_count,
        # --- Floor coverage block ---
        "floating_loans_count": floating_loans_count,
        "floating_loans_with_floor_count": floating_loans_with_floor_count,
        "floor_coverage_pct": floor_coverage_pct,
        "wavg_floor_rate": wavg_floor_rate,
        "floating_hold_total": floating_hold_total,
        "floating_hold_with_floor": floating_hold_with_floor,
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

    total_hold = sum(_credit_position_amount(l) for l in loans)
    target_default_amount = total_hold * default_shock

    base_nav = 0.0
    stressed_nav = 0.0
    defaulted_amount = 0.0
    impacted_loans = []

    for loan in sorted_loans:
        hold = _credit_position_amount(loan)
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
            # Rate shock: floating loans take a NAV haircut for spread compression.
            # Fixed loans are unaffected here (the fair_value mark already prices in
            # rate moves) — see test_stress_fixed_unaffected.
            if (loan.fixed_or_floating or "").lower() == "floating" and rate_shock != 0:
                haircut = (rate_shock / 100.0) * FLOATING_NAV_HAIRCUT_PER_100BPS
                stressed_nav += fv * (1.0 - haircut)
            else:
                stressed_nav += fv

    nav_impact_pct = safe_divide(stressed_nav - base_nav, base_nav) if base_nav > 0 else 0.0

    # Rate exposure breakdown for the stress page
    floating_count = 0
    floating_hold = 0.0
    fixed_count = 0
    fixed_hold = 0.0
    for loan in loans:
        hold = _credit_position_amount(loan)
        rate_type = (getattr(loan, "fixed_or_floating", None) or "").lower()
        if rate_type == "floating":
            floating_count += 1
            floating_hold += hold
        elif rate_type == "fixed":
            fixed_count += 1
            fixed_hold += hold

    return {
        "base_nav": base_nav,
        "stressed_nav": stressed_nav,
        "nav_impact_pct": nav_impact_pct,
        "nav_impact_dollars": stressed_nav - base_nav,
        "defaults_triggered": len(impacted_loans),
        "defaulted_amount": defaulted_amount,
        "impacted_loans": impacted_loans,
        "scenario": scenario,
        # Rate exposure
        "floating_count": floating_count,
        "floating_hold": floating_hold,
        "fixed_count": fixed_count,
        "fixed_hold": fixed_hold,
        "floating_pct": safe_divide(floating_hold, total_hold),
        "total_hold": total_hold,
    }


# ---------------------------------------------------------------------------
# Concentration
# ---------------------------------------------------------------------------


def compute_credit_concentration(loans, metrics_by_id=None):
    """Sector, geography, sponsor, security type breakdowns with HHI."""
    total_value = sum(_credit_track_value_components(l)["total_value"] for l in loans)

    def _build_breakdown(loans, attr):
        groups = defaultdict(lambda: {"value": 0.0, "count": 0})
        for loan in loans:
            key = getattr(loan, attr, None) or "Unknown"
            value = _credit_track_value_components(loan)["total_value"]
            groups[key]["value"] += value
            groups[key]["count"] += 1
        result = []
        for key, val in sorted(groups.items(), key=lambda x: -x[1]["value"]):
            pct = safe_divide(val["value"], total_value, 0.0)
            result.append(
                {
                    "name": key,
                    "value": val["value"],
                    "hold": val["value"],  # back-compat alias
                    "count": val["count"],
                    "pct": pct,
                }
            )
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
        [
            {
                "company": l.company_name,
                "value": _credit_track_value_components(l)["total_value"],
                "hold": _credit_track_value_components(l)["total_value"],  # back-compat alias
                "pct": safe_divide(_credit_track_value_components(l)["total_value"], total_value, 0.0),
            }
            for l in loans
        ],
        key=lambda x: -(x.get("value") or 0),
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
        groups = defaultdict(lambda: {"value": 0.0, "count": 0})
        for loan in loans:
            key = "Public" if getattr(loan, 'is_public', None) else "Private"
            value = _credit_track_value_components(loan)["total_value"]
            groups[key]["value"] += value
            groups[key]["count"] += 1
        for key, val in sorted(groups.items(), key=lambda x: -x[1]["value"]):
            pct = safe_divide(val["value"], total_value, 0.0)
            by_public.append(
                {
                    "name": key,
                    "value": val["value"],
                    "hold": val["value"],  # back-compat alias
                    "count": val["count"],
                    "pct": pct,
                }
            )

    return {
        "total_value": total_value,
        "total_hold": total_value,  # back-compat alias
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
        hold = _credit_position_amount(loan)
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
        # --- Term analysis ---
        "term_distribution": _compute_term_distribution(loans),
        "wavg_term_years": _wavg_term_years(loans),
    }


def _wavg_term_years(loans):
    """Hold-weighted average term in years."""
    num = 0.0
    den = 0.0
    count = 0
    for loan in loans:
        raw_tm = getattr(loan, "term_years", None)
        try:
            tm = float(raw_tm) if raw_tm is not None else None
        except (TypeError, ValueError):
            tm = None
        hold = _credit_position_amount(loan)
        if tm is not None and hold > 0:
            num += tm * hold
            den += hold
            count += 1
    return {
        "value": safe_divide(num, den),
        "coverage": count,
        "total": len(loans),
    }


def _compute_term_distribution(loans):
    """Bucket loans by term length: short (<2yr), medium (2-5yr), long (>5yr)."""
    buckets = {
        "Short (<2yr)": {"count": 0, "hold": 0.0},
        "Medium (2-5yr)": {"count": 0, "hold": 0.0},
        "Long (>5yr)": {"count": 0, "hold": 0.0},
        "Unknown": {"count": 0, "hold": 0.0},
    }
    total_hold = 0.0
    for loan in loans:
        hold = _credit_position_amount(loan)
        total_hold += hold
        raw_tm = getattr(loan, "term_years", None)
        try:
            tm = float(raw_tm) if raw_tm is not None else None
        except (TypeError, ValueError):
            tm = None
        if tm is None:
            buckets["Unknown"]["count"] += 1
            buckets["Unknown"]["hold"] += hold
        elif tm < 2:
            buckets["Short (<2yr)"]["count"] += 1
            buckets["Short (<2yr)"]["hold"] += hold
        elif tm <= 5:
            buckets["Medium (2-5yr)"]["count"] += 1
            buckets["Medium (2-5yr)"]["hold"] += hold
        else:
            buckets["Long (>5yr)"]["count"] += 1
            buckets["Long (>5yr)"]["hold"] += hold

    for b in buckets.values():
        b["pct"] = safe_divide(b["hold"], total_hold, 0.0)

    return {k: v for k, v in buckets.items() if v["count"] > 0}


# ---------------------------------------------------------------------------
# Credit Track Record (deal-level track record grouped by fund)
# ---------------------------------------------------------------------------
#
# Mirrors compute_deal_track_record in services/metrics/portfolio.py so the
# credit track record page can reuse the PE template structure almost verbatim.
#
# Key differences vs PE:
#   - Ownership % column becomes "% of Facility" (hold_size / issue_size)
#     which is the meaningful credit equivalent ("how much of the facility
#     does the manager hold").
#   - Gross IRR comes from loan.gross_irr (per-loan field), not derived.
#   - Net performance (net IRR / MOIC / DPI) comes from the new
#     CreditFundPerformance table (passed in as fund_performance dict keyed by
#     fund_name) instead of being duplicated on every deal row.
#   - hold_period is computed from close_date -> (exit_date or as_of_date),
#     expressed in years.

CREDIT_TRACK_RECORD_STATUS_ORDER = ("Fully Realized", "Partially Realized", "Unrealized", "Other")


def _credit_normalize_track_status(raw_status):
    """Normalize CreditLoan.status or default_status into track record buckets."""
    status = (raw_status or "").strip().lower()
    if "partial" in status and "realized" in status:
        return "Partially Realized"
    if "fully" in status and "realized" in status:
        return "Fully Realized"
    if status == "realized" or ("realized" in status and "unrealized" not in status):
        return "Fully Realized"
    if status in ("default", "defaulted", "restructured", "write-off", "write off", "writeoff"):
        # Treat credit losses as "Fully Realized" buckets for track record
        # purposes — the position is resolved, just at a loss.
        return "Fully Realized"
    if "performing" in status or "unrealized" in status or status == "":
        return "Unrealized"
    return "Other"


def _credit_loan_status(loan):
    """Prefer explicit `status` column; fall back to `default_status`."""
    raw = getattr(loan, "status", None) or getattr(loan, "default_status", None)
    return _credit_normalize_track_status(raw)


def _credit_hold_years(loan, as_of_date=None):
    """Hold period in years from close_date -> (exit_date or as_of)."""
    if loan.close_date is None:
        return None
    end_date = loan.exit_date or as_of_date or loan.as_of_date or date.today()
    delta_days = (end_date - loan.close_date).days
    if delta_days <= 0:
        return None
    return delta_days / 365.25


def _credit_track_value_components(loan, fx=1.0):
    """Loan-level value components used by track record and concentration."""
    realized_value = (
        getattr(loan, "realized_proceeds", None)
        if getattr(loan, "realized_proceeds", None) is not None
        else getattr(loan, "realized_value", None)
    )
    realized_value = (realized_value or 0.0) * fx

    ulv = (getattr(loan, "unrealized_loan_value", None) or 0.0) * fx
    uwev = (getattr(loan, "unrealized_warrant_equity_value", None) or 0.0) * fx
    fair_value = getattr(loan, "fair_value", None)
    tv_col = getattr(loan, "total_value", None)

    if ulv or uwev:
        unrealized_loan_value = ulv
    elif tv_col is not None:
        unrealized_loan_value = max((tv_col * fx) - realized_value - uwev, 0.0)
    elif fair_value is not None:
        unrealized_loan_value = fair_value * fx
    else:
        unrealized_loan_value = 0.0

    total_value = realized_value + unrealized_loan_value + uwev
    return {
        "realized_value": realized_value,
        "unrealized_value": unrealized_loan_value,
        "unrealized_warrant_equity_value": uwev,
        "total_value": total_value,
    }


def _credit_empty_track_totals():
    return {
        "deal_count": 0,
        "invested_equity": 0.0,
        "realized_value": 0.0,
        "unrealized_value": 0.0,
        "unrealized_warrant_equity_value": 0.0,
        "total_value": 0.0,
        "_gross_irr_weight_num": 0.0,
        "_gross_irr_weight_den": 0.0,
        "_hold_period_weight_num": 0.0,
        "_hold_period_weight_den": 0.0,
        "_facility_weight_num": 0.0,
        "_facility_weight_den": 0.0,
    }


def _credit_update_track_totals(totals, row):
    equity = row.get("invested_equity") or 0.0
    realized = row.get("realized_value") or 0.0
    unrealized = row.get("unrealized_value") or 0.0
    unrealized_warrant = row.get("unrealized_warrant_equity_value") or 0.0
    total_value = row.get("total_value") or 0.0
    gross_irr = row.get("gross_irr")
    hold_period = row.get("hold_period")
    facility_pct = row.get("facility_pct")

    totals["deal_count"] += 1
    totals["invested_equity"] += equity
    totals["realized_value"] += realized
    totals["unrealized_value"] += unrealized
    totals["unrealized_warrant_equity_value"] += unrealized_warrant
    totals["total_value"] += total_value

    if gross_irr is not None and equity > 0:
        totals["_gross_irr_weight_num"] += gross_irr * equity
        totals["_gross_irr_weight_den"] += equity
    if hold_period is not None and equity > 0:
        totals["_hold_period_weight_num"] += hold_period * equity
        totals["_hold_period_weight_den"] += equity
    if facility_pct is not None and equity > 0:
        totals["_facility_weight_num"] += facility_pct * equity
        totals["_facility_weight_den"] += equity


def _credit_merge_track_totals(lhs, rhs):
    merged = _credit_empty_track_totals()
    for key in merged:
        if key == "deal_count":
            merged[key] = int((lhs.get(key) or 0) + (rhs.get(key) or 0))
        else:
            merged[key] = (lhs.get(key) or 0.0) + (rhs.get(key) or 0.0)
    return merged


def _credit_finalize_track_totals(raw_totals, invested_total=None, fund_size=None):
    invested_equity = raw_totals["invested_equity"]
    total_value = raw_totals["total_value"]
    realized_value = raw_totals["realized_value"]
    unrealized_value = raw_totals["unrealized_value"]
    unrealized_warrant_equity_value = raw_totals["unrealized_warrant_equity_value"]
    unrealized_total_value = unrealized_value + unrealized_warrant_equity_value

    return {
        "deal_count": raw_totals["deal_count"],
        "invested_equity": invested_equity,
        "realized_value": realized_value,
        "unrealized_value": unrealized_value,
        "unrealized_warrant_equity_value": unrealized_warrant_equity_value,
        "unrealized_total_value": unrealized_total_value,
        "total_value": total_value,
        "hold_period": safe_divide(
            raw_totals["_hold_period_weight_num"], raw_totals["_hold_period_weight_den"]
        ),
        # "ownership_pct" key preserved so the shared template can render the
        # same field — value is facility %, which is the credit analog.
        "ownership_pct": safe_divide(
            raw_totals["_facility_weight_num"], raw_totals["_facility_weight_den"]
        ),
        "pct_total_invested": safe_divide(invested_equity, invested_total),
        "pct_fund_size": safe_divide(invested_equity, fund_size),
        "gross_irr": safe_divide(
            raw_totals["_gross_irr_weight_num"], raw_totals["_gross_irr_weight_den"]
        ),
        "gross_moic": safe_divide(total_value, invested_equity),
        "realized_gross_moic": safe_divide(realized_value, invested_equity),
        "unrealized_gross_moic": safe_divide(unrealized_total_value, invested_equity),
        # Backwards-compat aliases for downstream consumers.
        "moic": safe_divide(total_value, invested_equity),
        "irr": safe_divide(
            raw_totals["_gross_irr_weight_num"], raw_totals["_gross_irr_weight_den"]
        ),
    }


def _credit_fund_net_performance(fund_perf):
    """Extract net performance fields from a CreditFundPerformance row.

    Unlike PE (which reconciles net_irr across all deals in a fund and flags
    conflicts), credit stores fund-level net returns on a dedicated row. No
    reconciliation needed — if the row exists, use it; else emit N/A.
    """
    if fund_perf is None:
        return {
            "net_irr": None,
            "net_moic": None,
            "net_dpi": None,
            "net_tvpi": None,
            "net_rvpi": None,
            "called_capital": None,
            "distributed_capital": None,
            "nav": None,
            "fund_size": None,
            "has_data": False,
            # PE template uses .conflicts.* — keep shape compatible so the
            # shared template macros/logic don't need to branch on asset class.
            "conflicts": {"net_irr": False, "net_moic": False, "net_dpi": False},
        }
    net_moic = getattr(fund_perf, "net_moic", None)
    net_tvpi = getattr(fund_perf, "net_tvpi", None)
    if net_tvpi is None and net_moic is not None:
        net_tvpi = net_moic
    if net_moic is None and net_tvpi is not None:
        net_moic = net_tvpi
    return {
        "net_irr": getattr(fund_perf, "net_irr", None),
        "net_moic": net_moic,
        "net_dpi": getattr(fund_perf, "net_dpi", None),
        "net_tvpi": net_tvpi,
        "net_rvpi": getattr(fund_perf, "net_rvpi", None),
        "called_capital": getattr(fund_perf, "called_capital", None),
        "distributed_capital": getattr(fund_perf, "distributed_capital", None),
        "nav": getattr(fund_perf, "nav", None),
        "fund_size": getattr(fund_perf, "fund_size", None),
        "has_data": any(
            value is not None
            for value in (getattr(fund_perf, "net_irr", None), net_moic, getattr(fund_perf, "net_dpi", None), net_tvpi, getattr(fund_perf, "net_rvpi", None))
        ),
        "conflicts": {"net_irr": False, "net_moic": False, "net_dpi": False},
    }


def _credit_fund_sort_key(fund_name, fund_performance):
    """Sort funds by vintage year (if known), then alphabetically."""
    perf = (fund_performance or {}).get(fund_name) if fund_performance else None
    vintage = getattr(perf, "vintage_year", None) if perf is not None else None
    return (vintage if vintage is not None else 9999, (fund_name or "").lower())


def compute_credit_track_record(loans, metrics_by_id=None, *, fund_performance=None):
    """Build a deal-level track record for credit loans grouped by fund.

    Shape mirrors compute_deal_track_record so the credit track record page
    can reuse the same Jinja template with minimal changes.

    Args:
        loans: iterable of CreditLoan ORM rows.
        metrics_by_id: optional dict of loan_id -> compute_credit_loan_metrics
            output. Unused today (credit loans carry gross_irr/moic as direct
            columns) but accepted for API consistency with the PE function.
        fund_performance: optional dict of fund_name -> CreditFundPerformance
            ORM row. When provided, net performance rows under each fund
            show real net IRR / MOIC / DPI; otherwise they render N/A.

    Returns:
        {"funds": [...], "overall": {...}} with the same key shape as
        compute_deal_track_record.
    """
    fund_performance = fund_performance or {}

    grouped = defaultdict(list)
    status_index = {status: idx for idx, status in enumerate(CREDIT_TRACK_RECORD_STATUS_ORDER)}

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        hold_size = _credit_hold_amount(loan, fx)
        invested = _credit_position_amount(loan, fx)
        values = _credit_track_value_components(loan, fx)
        realized_value = values["realized_value"]
        unrealized_loan_value = values["unrealized_value"]
        uwev = values["unrealized_warrant_equity_value"]
        total_value = values["total_value"]
        unrealized_total_value = unrealized_loan_value + uwev

        issue_size = (loan.issue_size or 0.0) * fx
        facility_pct = safe_divide(hold_size, issue_size)

        gross_irr = getattr(loan, "gross_irr", None)
        gross_moic = safe_divide(total_value, invested) if invested > 0 else getattr(loan, "moic", None)
        realized_moic = safe_divide(realized_value, invested)
        unrealized_moic = safe_divide(unrealized_total_value, invested)

        status = _credit_loan_status(loan)
        fund = loan.fund_name or "Unknown Fund"

        row = {
            "loan_id": loan.id,
            # "deal_id" alias so any shared rendering code keeps working.
            "deal_id": loan.id,
            "company_name": loan.company_name or "Unknown Company",
            "sector": getattr(loan, "sector", None),
            "status": status,
            "investment_date": loan.close_date,
            "exit_date": loan.exit_date,
            "hold_period": _credit_hold_years(loan),
            # "ownership_pct" key is reused as % of facility so the shared
            # template doesn't need a separate column.
            "ownership_pct": facility_pct,
            "facility_pct": facility_pct,
            "invested_equity": invested,
            "current_invested_capital": invested,
            "realized_value": realized_value,
            "unrealized_value": unrealized_loan_value,
            "unrealized_warrant_equity_value": uwev,
            "unrealized_total_value": unrealized_total_value,
            "total_value": total_value,
            "gross_irr": gross_irr,
            "gross_moic": gross_moic,
            "realized_gross_moic": realized_moic,
            "unrealized_gross_moic": unrealized_moic,
            "fund_size": getattr(loan, "fund_size", None),
            "currency": getattr(loan, "currency", None),
            "fx_rate_to_usd": loan.fx_rate_to_usd,
            # Backward compatibility aliases mirroring PE rows.
            "moic": gross_moic,
            "irr": gross_irr,
            # PE template inspects these; credit keeps them None so the badge
            # only renders when real FX conversion happened.
            "performance_currency": getattr(loan, "currency", None),
            "financial_metric_currency": getattr(loan, "currency", None),
            "perf_fx_rate_to_usd": loan.fx_rate_to_usd,
            "fin_fx_rate_to_usd": loan.fx_rate_to_usd,
        }
        grouped[fund].append(row)

    fund_groups = []
    overall_status_totals = {
        status: _credit_empty_track_totals() for status in CREDIT_TRACK_RECORD_STATUS_ORDER
    }
    overall_totals = _credit_empty_track_totals()
    overall_realized_totals = _credit_empty_track_totals()
    overall_unrealized_totals = _credit_empty_track_totals()

    sorted_funds = sorted(
        grouped.keys(),
        key=lambda fn: _credit_fund_sort_key(fn, fund_performance),
    )

    overall_fund_size_total = 0.0

    for fund in sorted_funds:
        fund_rows = sorted(
            grouped[fund],
            key=lambda r: (
                status_index.get(r["status"], 99),
                r.get("investment_date") is None,
                r.get("investment_date").toordinal() if r.get("investment_date") is not None else 3652059,
                r["company_name"],
                r["loan_id"] or 0,
            ),
        )
        fund_totals = _credit_empty_track_totals()
        fund_status_totals = {
            status: _credit_empty_track_totals() for status in CREDIT_TRACK_RECORD_STATUS_ORDER
        }

        for row in fund_rows:
            _credit_update_track_totals(fund_totals, row)
            _credit_update_track_totals(fund_status_totals[row["status"]], row)
            _credit_update_track_totals(overall_totals, row)
            _credit_update_track_totals(overall_status_totals[row["status"]], row)
            if row["status"] in {"Fully Realized", "Partially Realized"}:
                _credit_update_track_totals(overall_realized_totals, row)
            if row["status"] == "Unrealized":
                _credit_update_track_totals(overall_unrealized_totals, row)

        # Fund size: prefer Fund Performance, but fall back to the loan sheet.
        fund_perf = fund_performance.get(fund)
        perf_fund_size = getattr(fund_perf, "fund_size", None) if fund_perf is not None else None
        fund_size_meta = _credit_resolve_scalar(
            [row.get("fund_size") for row in fund_rows] + [perf_fund_size]
        )
        fund_size = perf_fund_size if perf_fund_size is not None else fund_size_meta["value"]
        if fund_size is not None and fund_size > 0:
            overall_fund_size_total += fund_size
        fund_invested_total = fund_totals["invested_equity"]

        for idx, row in enumerate(fund_rows, start=1):
            row["row_num"] = idx
            row["pct_total_invested"] = safe_divide(row.get("invested_equity"), fund_invested_total)
            row["pct_fund_size"] = safe_divide(row.get("invested_equity"), fund_size)

        status_rollups = []
        for status in CREDIT_TRACK_RECORD_STATUS_ORDER:
            raw = fund_status_totals[status]
            if raw["deal_count"] == 0:
                continue
            status_rollups.append(
                {
                    "status": status,
                    "label": f"All {raw['deal_count']} {fund} {status} Loans",
                    "totals": _credit_finalize_track_totals(
                        raw, invested_total=fund_invested_total, fund_size=fund_size
                    ),
                }
            )

        realized_raw = _credit_merge_track_totals(
            fund_status_totals["Fully Realized"], fund_status_totals["Partially Realized"]
        )
        unrealized_raw = fund_status_totals["Unrealized"]
        fund_summary_rollups = []
        if realized_raw["deal_count"] > 0:
            fund_summary_rollups.append(
                {
                    "label": f"All {realized_raw['deal_count']} {fund} Fully and Partially Realized Loans",
                    "totals": _credit_finalize_track_totals(
                        realized_raw, invested_total=fund_invested_total, fund_size=fund_size
                    ),
                }
            )
        if unrealized_raw["deal_count"] > 0:
            fund_summary_rollups.append(
                {
                    "label": f"All {unrealized_raw['deal_count']} {fund} Unrealized Loans",
                    "totals": _credit_finalize_track_totals(
                        unrealized_raw, invested_total=fund_invested_total, fund_size=fund_size
                    ),
                }
            )
        fund_summary_rollups.append(
            {
                "label": f"All {fund_totals['deal_count']} {fund} Loans",
                "totals": _credit_finalize_track_totals(
                    fund_totals, invested_total=fund_invested_total, fund_size=fund_size
                ),
            }
        )

        fund_groups.append(
            {
                "fund_name": fund,
                "fund_size": fund_size,
                "fund_size_conflict": fund_size_meta["conflict"],
                "vintage_year": getattr(fund_perf, "vintage_year", None) if fund_perf else None,
                "rows": fund_rows,
                "status_rollups": status_rollups,
                "summary_rollups": fund_summary_rollups,
                "net_performance": _credit_fund_net_performance(fund_perf),
                "totals": _credit_finalize_track_totals(
                    fund_totals, invested_total=fund_invested_total, fund_size=fund_size
                ),
            }
        )

    overall_invested_total = overall_totals["invested_equity"]
    overall_status_rollups = []
    for status in CREDIT_TRACK_RECORD_STATUS_ORDER:
        totals = _credit_finalize_track_totals(
            overall_status_totals[status],
            invested_total=overall_invested_total,
            fund_size=overall_fund_size_total or None,
        )
        if totals["deal_count"] == 0:
            continue
        overall_status_rollups.append(
            {"status": status, "label": f"All {totals['deal_count']} {status} Loans", "totals": totals}
        )

    overall_summary_rollups = []
    if overall_realized_totals["deal_count"] > 0:
        overall_summary_rollups.append(
            {
                "label": f"All {overall_realized_totals['deal_count']} Fully and Partially Realized Loans",
                "totals": _credit_finalize_track_totals(
                    overall_realized_totals,
                    invested_total=overall_invested_total,
                    fund_size=overall_fund_size_total or None,
                ),
            }
        )
    if overall_unrealized_totals["deal_count"] > 0:
        overall_summary_rollups.append(
            {
                "label": f"All {overall_unrealized_totals['deal_count']} Unrealized Loans",
                "totals": _credit_finalize_track_totals(
                    overall_unrealized_totals,
                    invested_total=overall_invested_total,
                    fund_size=overall_fund_size_total or None,
                ),
            }
        )
    overall_summary_rollups.append(
        {
            "label": f"All {overall_totals['deal_count']} Loans",
            "totals": _credit_finalize_track_totals(
                overall_totals,
                invested_total=overall_invested_total,
                fund_size=overall_fund_size_total or None,
            ),
        }
    )

    return {
        "funds": fund_groups,
        "overall": {
            "status_rollups": overall_status_rollups,
            "summary_rollups": overall_summary_rollups,
            "status_groups": overall_status_rollups,  # back-compat alias
            "totals": _credit_finalize_track_totals(
                overall_totals,
                invested_total=overall_invested_total,
                fund_size=overall_fund_size_total or None,
            ),
        },
        "loan_count": overall_totals["deal_count"],
        "fund_count": len(fund_groups),
    }


# ---------------------------------------------------------------------------
# Credit Data Cuts — slice credit portfolio by qualitative dimensions
# ---------------------------------------------------------------------------
#
# Mirrors compute_data_cuts_analytics in services/metrics/data_cuts.py.
# Dimensions and metrics adapted for credit-specific fields.

import re as _re


def _term_bucket_label(term_years):
    """Classify term_years into a bucket label. None-safe."""
    try:
        tm = float(term_years) if term_years is not None else None
    except (TypeError, ValueError):
        return None
    if tm is None:
        return None
    if tm < 2:
        return "Short (<2yr)"
    if tm <= 5:
        return "Medium (2-5yr)"
    return "Long (>5yr)"


CREDIT_DIMENSIONS = {
    "fund_name": {"field": "fund_name", "fallback": "Unknown Fund"},
    "sector": {"field": "sector", "fallback": "Unknown"},
    "geography": {"field": "geography", "fallback": "Unknown"},
    "sponsor": {"field": "sponsor", "fallback": "Unknown"},
    "instrument": {"field": "instrument", "fallback": "Unknown"},
    "tranche": {"field": "tranche", "fallback": "Unknown"},
    "security_type": {"field": "security_type", "fallback": "Unknown"},
    "default_status": {"field": "default_status", "fallback": "Unknown"},
    "status": {"field": "status", "fallback": "Unknown"},
    "vintage_year": {"field": None, "resolver": lambda l: str(l.vintage_year) if l.vintage_year else None, "fallback": "Unknown"},
    "sourcing_channel": {"field": "sourcing_channel", "fallback": "Unknown"},
    "fixed_or_floating": {"field": "fixed_or_floating", "fallback": "Unknown"},
    "term_bucket": {"field": None, "resolver": lambda l: _term_bucket_label(getattr(l, "term_years", None)), "fallback": "Unknown"},
}

CREDIT_DIMENSION_LABELS = {
    "fund_name": "Fund",
    "sector": "Sector",
    "geography": "Geography",
    "sponsor": "Sponsor",
    "instrument": "Instrument",
    "tranche": "Tranche / Lien",
    "security_type": "Security Type",
    "default_status": "Default Status",
    "status": "Realization Status",
    "vintage_year": "Vintage Year",
    "sourcing_channel": "Sourcing Channel",
    "fixed_or_floating": "Rate Type",
    "term_bucket": "Term Bucket",
}

CREDIT_ALLOWED_METRICS = [
    "weighted_moic",
    "weighted_irr",
    "invested",
    "realized_value",
    "unrealized_value",
    "total_value",
    "weighted_yield",
    "weighted_ltv",
    "weighted_hold_years",
    "loss_ratio_count",
    "loss_ratio_capital",
    "pct_of_invested",
    "pct_of_total",
]

_CREDIT_MAX_SECONDARY_COLUMNS = 20
_CREDIT_MAX_CHART_SECONDARY = 8


def _credit_dc_new_bucket():
    return {
        "loan_count": 0,
        "invested": 0.0,
        "realized_value": 0.0,
        "unrealized_value": 0.0,
        "total_value": 0.0,
        "_irr_num": 0.0,
        "_irr_den": 0.0,
        "_yield_num": 0.0,
        "_yield_den": 0.0,
        "_ltv_num": 0.0,
        "_ltv_den": 0.0,
        "_hold_num": 0.0,
        "_hold_den": 0.0,
        "_loss_count": 0,
        "_loss_capital": 0.0,
        "_loans": [],
        "_loan_ids": [],
    }


def _credit_dc_add(bucket, loan, fx=1.0):
    hold = _credit_position_amount(loan, fx)
    invested = _credit_position_amount(loan, fx)

    rp = (getattr(loan, "realized_proceeds", None) or 0.0) * fx
    ulv = (getattr(loan, "unrealized_loan_value", None) or 0.0) * fx
    uwev = (getattr(loan, "unrealized_warrant_equity_value", None) or 0.0) * fx
    if ulv or uwev:
        unrealized = ulv + uwev
    elif getattr(loan, "fair_value", None) is not None:
        unrealized = loan.fair_value * fx
    else:
        unrealized = 0.0

    tv_col = getattr(loan, "total_value", None)
    total_val = (tv_col * fx) if tv_col is not None else (rp + unrealized)

    irr = getattr(loan, "gross_irr", None)
    moic_val = safe_divide(total_val, invested) if invested > 0 else getattr(loan, "moic", None)
    coupon = loan.coupon_rate
    ltv = loan.current_ltv

    hold_years = None
    if loan.close_date:
        end = loan.exit_date or getattr(loan, "as_of_date", None) or date.today()
        dd = (end - loan.close_date).days
        if dd > 0:
            hold_years = dd / 365.25

    bucket["loan_count"] += 1
    bucket["invested"] += invested
    bucket["realized_value"] += rp
    bucket["unrealized_value"] += unrealized
    bucket["total_value"] += total_val
    bucket["_loan_ids"].append(loan.id)

    if irr is not None and invested > 0:
        bucket["_irr_num"] += irr * invested
        bucket["_irr_den"] += invested
    if coupon is not None and hold > 0:
        bucket["_yield_num"] += coupon * hold
        bucket["_yield_den"] += hold
    if ltv is not None and hold > 0:
        bucket["_ltv_num"] += ltv * hold
        bucket["_ltv_den"] += hold
    if hold_years is not None and invested > 0:
        bucket["_hold_num"] += hold_years * invested
        bucket["_hold_den"] += invested

    if moic_val is not None and moic_val < 1.0 and invested > 0:
        bucket["_loss_count"] += 1
        bucket["_loss_capital"] += max(invested - total_val, 0.0)

    bucket["_loans"].append({
        "id": loan.id,
        "company_name": loan.company_name or "",
        "fund_name": loan.fund_name or "",
        "sector": getattr(loan, "sector", None) or "",
        "status": getattr(loan, "status", None) or getattr(loan, "default_status", None) or "",
        "invested": invested,
        "moic": moic_val,
        "irr": irr,
        "hold_years": hold_years,
        "coupon_rate": coupon,
        "current_ltv": ltv,
    })


def _credit_dc_finalize(label, bucket, portfolio_invested=None, portfolio_total=None):
    inv = bucket["invested"]
    cnt = bucket["loan_count"]
    return {
        "label": label,
        "loan_count": cnt,
        "deal_count": cnt,  # back-compat alias for template
        "invested": inv,
        "invested_equity": inv,  # alias for template reuse
        "realized_value": bucket["realized_value"],
        "unrealized_value": bucket["unrealized_value"],
        "total_value": bucket["total_value"],
        "weighted_moic": safe_divide(bucket["total_value"], inv),
        "weighted_irr": safe_divide(bucket["_irr_num"], bucket["_irr_den"]),
        "weighted_yield": safe_divide(bucket["_yield_num"], bucket["_yield_den"]),
        "weighted_ltv": safe_divide(bucket["_ltv_num"], bucket["_ltv_den"]),
        "weighted_hold_years": safe_divide(bucket["_hold_num"], bucket["_hold_den"]),
        "loss_ratio_count": safe_divide(bucket["_loss_count"], cnt) if cnt > 0 else None,
        "loss_ratio_capital": safe_divide(bucket["_loss_capital"], inv) if inv > 0 else None,
        "pct_of_invested": safe_divide(inv, portfolio_invested) if portfolio_invested else None,
        "pct_of_total": safe_divide(bucket["total_value"], portfolio_total) if portfolio_total else None,
        "loans": sorted(bucket["_loans"], key=lambda d: d.get("invested") or 0, reverse=True),
        "small_n": cnt < 3,
    }


def _credit_dc_resolve_dim(loan, dim_key):
    cfg = CREDIT_DIMENSIONS.get(dim_key)
    if cfg is None:
        return "Unknown"
    resolver = cfg.get("resolver")
    if resolver:
        val = resolver(loan)
        return val if val else cfg["fallback"]
    field = cfg["field"]
    val = getattr(loan, field, None)
    return val if val else cfg["fallback"]


def _credit_dc_validate_dim(dim, default="sector"):
    if dim and dim.lower() in CREDIT_DIMENSIONS:
        return dim.lower()
    return default


def _credit_dc_sort_groups(groups, dim_key):
    if dim_key == "vintage_year":
        def _key(g):
            try:
                return (0, int(g["label"]))
            except (ValueError, TypeError):
                return (1, 0)
        return sorted(groups, key=_key)
    elif dim_key == "fund_name":
        roman_map = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5,
                     "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10}
        def _fund_key(g):
            m = _re.search(r'\b([IVX]+)\b', g["label"] or "")
            if m and m.group(1) in roman_map:
                return (0, roman_map[m.group(1)], g["label"])
            return (1, 0, g["label"] or "")
        return sorted(groups, key=_fund_key)
    else:
        fb = CREDIT_DIMENSIONS.get(dim_key, {}).get("fallback", "Unknown")
        return sorted(groups, key=lambda g: (g["label"] == fb, g["label"] or ""))


def compute_credit_data_cuts(loans, metrics_by_id=None, primary_dim="sector", secondary_dim=None):
    """Slice credit portfolio performance by any qualitative dimension.

    Mirrors compute_data_cuts_analytics for PE deals.
    """
    primary_dim = _credit_dc_validate_dim(primary_dim, "sector")
    if secondary_dim:
        secondary_dim = _credit_dc_validate_dim(secondary_dim, None)
        if secondary_dim == primary_dim:
            secondary_dim = None

    groups = defaultdict(_credit_dc_new_bucket)
    cross_tab = defaultdict(lambda: defaultdict(_credit_dc_new_bucket)) if secondary_dim else None
    totals_bucket = _credit_dc_new_bucket()

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        primary_val = _credit_dc_resolve_dim(loan, primary_dim)
        _credit_dc_add(groups[primary_val], loan, fx)
        _credit_dc_add(totals_bucket, loan, fx)

        if cross_tab is not None:
            secondary_val = _credit_dc_resolve_dim(loan, secondary_dim)
            _credit_dc_add(cross_tab[primary_val][secondary_val], loan, fx)

    portfolio_invested = totals_bucket["invested"]
    portfolio_total = totals_bucket["total_value"]

    finalized_groups = [
        _credit_dc_finalize(label, bucket, portfolio_invested, portfolio_total)
        for label, bucket in groups.items()
    ]
    finalized_groups = _credit_dc_sort_groups(finalized_groups, primary_dim)
    totals = _credit_dc_finalize("Total", totals_bucket, portfolio_invested, portfolio_total)

    # Data quality warning
    total_loans = totals["loan_count"]
    fallback = CREDIT_DIMENSIONS.get(primary_dim, {}).get("fallback", "Unknown")
    unk = next((g for g in finalized_groups if g["label"] == fallback), None)
    unk_pct = safe_divide(unk["loan_count"], total_loans) if unk and total_loans > 0 else 0
    data_quality_warning = None
    if unk_pct and unk_pct > 0.2:
        data_quality_warning = {
            "dimension": CREDIT_DIMENSION_LABELS.get(primary_dim, primary_dim),
            "count": unk["loan_count"],
            "pct": unk_pct,
        }

    # Cross-tab finalization
    finalized_cross_tab = None
    secondary_labels = []
    cross_tab_truncated = False
    truncated_count = 0
    finalized_col_totals = None

    if cross_tab is not None:
        sec_totals = defaultdict(float)
        for _, sec_buckets in cross_tab.items():
            for sec_label, bucket in sec_buckets.items():
                sec_totals[sec_label] += bucket["invested"]

        sorted_sec = sorted(sec_totals.keys(), key=lambda k: sec_totals[k], reverse=True)

        if len(sorted_sec) > _CREDIT_MAX_SECONDARY_COLUMNS:
            cross_tab_truncated = True
            truncated_count = len(sorted_sec) - _CREDIT_MAX_SECONDARY_COLUMNS
            kept_labels = sorted_sec[:_CREDIT_MAX_SECONDARY_COLUMNS]
            overflow_labels = sorted_sec[_CREDIT_MAX_SECONDARY_COLUMNS:]
        else:
            kept_labels = sorted_sec
            overflow_labels = []

        secondary_labels = kept_labels + (["Other"] if overflow_labels else [])
        finalized_cross_tab = {}
        col_totals = {sl: _credit_dc_new_bucket() for sl in secondary_labels}

        for primary_label in [g["label"] for g in finalized_groups]:
            row = {}
            sec_buckets = cross_tab.get(primary_label, {})

            for sec_label in kept_labels:
                bucket = sec_buckets.get(sec_label)
                if bucket and bucket["loan_count"] > 0:
                    row[sec_label] = _credit_dc_finalize(sec_label, bucket)
                    for loan in loans:
                        if loan.id in bucket["_loan_ids"]:
                            _credit_dc_add(col_totals[sec_label], loan, loan.fx_rate_to_usd or 1.0)
                else:
                    row[sec_label] = None

            if overflow_labels:
                other_bucket = _credit_dc_new_bucket()
                for ov_sec in overflow_labels:
                    ov_bucket = sec_buckets.get(ov_sec)
                    if ov_bucket:
                        for loan in loans:
                            if loan.id in ov_bucket["_loan_ids"]:
                                fx = loan.fx_rate_to_usd or 1.0
                                _credit_dc_add(other_bucket, loan, fx)
                                _credit_dc_add(col_totals["Other"], loan, fx)
                if other_bucket["loan_count"] > 0:
                    row["Other"] = _credit_dc_finalize("Other", other_bucket)
                else:
                    row["Other"] = None

            finalized_cross_tab[primary_label] = row

        finalized_col_totals = {}
        for sl in secondary_labels:
            if col_totals[sl]["loan_count"] > 0:
                finalized_col_totals[sl] = _credit_dc_finalize(sl, col_totals[sl])
            else:
                finalized_col_totals[sl] = None

    # Chart payload
    chart_groups = finalized_groups[:50]
    chart_labels = [g["label"] for g in chart_groups]
    chart_datasets = {}
    for mk in CREDIT_ALLOWED_METRICS:
        chart_datasets[mk] = [g.get(mk) for g in chart_groups]

    chart_secondary = None
    if cross_tab is not None and finalized_cross_tab:
        chart_sec_labels = secondary_labels[:_CREDIT_MAX_CHART_SECONDARY]
        chart_secondary = {
            "secondary_labels": chart_sec_labels,
            "truncated": len(secondary_labels) > _CREDIT_MAX_CHART_SECONDARY,
            "total_secondary": len(secondary_labels),
        }

    return {
        "primary_dim": primary_dim,
        "primary_dim_label": CREDIT_DIMENSION_LABELS.get(primary_dim, primary_dim),
        "secondary_dim": secondary_dim,
        "secondary_dim_label": CREDIT_DIMENSION_LABELS.get(secondary_dim, secondary_dim) if secondary_dim else None,
        "groups": finalized_groups,
        "totals": totals,
        "cross_tab": finalized_cross_tab,
        "secondary_labels": secondary_labels if cross_tab is not None else [],
        "col_totals": finalized_col_totals,
        "cross_tab_truncated": cross_tab_truncated,
        "truncated_count": truncated_count,
        "chart_labels": chart_labels,
        "chart_datasets": chart_datasets,
        "chart_secondary": chart_secondary,
        "dimensions": CREDIT_DIMENSIONS,
        "dimension_labels": CREDIT_DIMENSION_LABELS,
        "allowed_metrics": CREDIT_ALLOWED_METRICS,
        "data_quality_warning": data_quality_warning,
        "deal_count": totals["loan_count"],
        "loan_count": totals["loan_count"],
    }


# ---------------------------------------------------------------------------
# Loan Structure & Terms analysis
# ---------------------------------------------------------------------------


def compute_credit_loan_structure(loans, metrics_by_id=None):
    """Aggregate and per-loan view of loan structural terms.

    Surfaces the ~25 fields that the parser ingests for loan economics,
    rate structure, fees, protections, amortization, par/outstanding,
    warrants, and equity, most of which were previously orphaned.
    """
    if metrics_by_id is None:
        metrics_by_id = {l.id: compute_credit_loan_metrics(l) for l in loans}

    # --- Rate mix ---
    floating_count = 0
    fixed_count = 0
    other_rate_count = 0
    floating_hold = 0.0
    fixed_hold = 0.0
    total_hold = 0.0

    # --- Weighted aggregates ---
    spread_num = 0.0
    spread_den = 0.0
    spread_count = 0
    coupon_num = 0.0
    coupon_den = 0.0
    coupon_count = 0
    ytm_num = 0.0
    ytm_den = 0.0
    ytm_count = 0
    floor_num = 0.0
    floor_den = 0.0
    floor_count = 0
    call_prot_num = 0.0
    call_prot_den = 0.0
    call_prot_count = 0

    # --- Fee totals ---
    total_fee_oid = 0.0
    total_fee_upfront = 0.0
    total_fee_exit = 0.0
    total_closing_fee = 0.0
    loans_with_fees = 0

    # --- PIK ---
    pik_count = 0
    pik_hold = 0.0

    # --- Amortization breakdown ---
    amort_dist = defaultdict(lambda: {"count": 0, "hold": 0.0})
    payment_freq_dist = defaultdict(lambda: {"count": 0, "hold": 0.0})

    # --- Par/Outstanding totals ---
    total_original_par = 0.0
    total_current_outstanding = 0.0
    total_issue_size = 0.0
    total_accrued_interest = 0.0

    # --- Warrant/Equity totals ---
    total_equity_investment = 0.0
    total_warrant_value = 0.0
    loans_with_warrants = 0
    loans_with_equity = 0

    # --- Per-loan detail rows ---
    rows = []

    for loan in loans:
        fx = loan.fx_rate_to_usd or 1.0
        hold = _credit_position_amount(loan, fx)
        total_hold += hold
        m = metrics_by_id.get(loan.id, {})

        # Rate mix
        rate_type = (getattr(loan, "fixed_or_floating", None) or "").strip()
        if rate_type.lower() == "floating":
            floating_count += 1
            floating_hold += hold
        elif rate_type.lower() == "fixed":
            fixed_count += 1
            fixed_hold += hold
        elif rate_type:
            other_rate_count += 1

        # Weighted aggregates
        spread = loan.spread_bps
        if spread is not None and hold > 0:
            spread_num += spread * hold
            spread_den += hold
            spread_count += 1

        coupon = loan.coupon_rate
        if coupon is not None and hold > 0:
            coupon_num += coupon * hold
            coupon_den += hold
            coupon_count += 1

        ytm = loan.yield_to_maturity
        if ytm is not None and hold > 0:
            ytm_num += ytm * hold
            ytm_den += hold
            ytm_count += 1

        floor = getattr(loan, "floor_rate", None)
        if floor is not None and floor > 0 and hold > 0:
            floor_num += floor * hold
            floor_den += hold
            floor_count += 1

        call_prot = loan.call_protection_months
        if call_prot is not None and hold > 0:
            call_prot_num += call_prot * hold
            call_prot_den += hold
            call_prot_count += 1

        # Fees
        oid = loan.fee_oid
        upfront = getattr(loan, "fee_upfront", None)
        exit_fee = getattr(loan, "fee_exit", None)
        closing = getattr(loan, "closing_fee", None)
        if any(v is not None for v in (oid, upfront, exit_fee, closing)):
            loans_with_fees += 1
            total_fee_oid += (oid or 0.0) * hold if hold > 0 else 0.0
            total_fee_upfront += (upfront or 0.0) * fx
            total_fee_exit += (exit_fee or 0.0) * fx
            total_closing_fee += (closing or 0.0) * fx

        # PIK
        if loan.pik_toggle:
            pik_count += 1
            pik_hold += hold

        # Amortization
        amort = getattr(loan, "amortization_type", None) or "Unknown"
        amort_dist[amort]["count"] += 1
        amort_dist[amort]["hold"] += hold

        # Payment frequency
        pf = getattr(loan, "payment_frequency", None) or "Unknown"
        payment_freq_dist[pf]["count"] += 1
        payment_freq_dist[pf]["hold"] += hold

        # Par/outstanding
        total_original_par += (getattr(loan, "original_par", None) or 0.0) * fx
        total_current_outstanding += (getattr(loan, "current_outstanding", None) or 0.0) * fx
        total_issue_size += (loan.issue_size or 0.0) * fx
        total_accrued_interest += (getattr(loan, "accrued_interest", None) or 0.0) * fx

        # Warrants/equity
        eq = getattr(loan, "equity_investment", None)
        if eq is not None and eq > 0:
            total_equity_investment += eq * fx
            loans_with_equity += 1
        uwev = getattr(loan, "unrealized_warrant_equity_value", None)
        warrants_cur = getattr(loan, "warrants_current", None)
        if (uwev is not None and uwev > 0) or (warrants_cur is not None and warrants_cur > 0):
            total_warrant_value += (uwev or 0.0) * fx
            loans_with_warrants += 1

        # Per-loan row
        rows.append({
            "loan_id": loan.id,
            "company": loan.company_name,
            "fund": loan.fund_name,
            "sector": getattr(loan, "sector", None),
            "hold_size": hold,
            "rate_type": rate_type or None,
            "reference_rate": getattr(loan, "reference_rate", None),
            "spread_bps": spread,
            "coupon_rate": coupon,
            "cash_margin": getattr(loan, "cash_margin", None),
            "floor_rate": floor,
            "pik_toggle": loan.pik_toggle,
            "pik_rate": getattr(loan, "pik_rate", None),
            "fee_oid": oid,
            "fee_upfront": upfront,
            "fee_exit": exit_fee,
            "closing_fee": closing,
            "call_protection_months": call_prot,
            "make_whole_premium": getattr(loan, "make_whole_premium", None),
            "prepayment_protection": getattr(loan, "prepayment_protection", None),
            "amortization_type": getattr(loan, "amortization_type", None),
            "payment_frequency": getattr(loan, "payment_frequency", None),
            "term_years": getattr(loan, "term_years", None),
            "original_par": (getattr(loan, "original_par", None) or 0.0) * fx if getattr(loan, "original_par", None) else None,
            "current_outstanding": (getattr(loan, "current_outstanding", None) or 0.0) * fx if getattr(loan, "current_outstanding", None) else None,
            "issue_size": (loan.issue_size or 0.0) * fx if loan.issue_size else None,
            "accrued_interest": (getattr(loan, "accrued_interest", None) or 0.0) * fx if getattr(loan, "accrued_interest", None) else None,
            "yield_to_maturity": ytm,
            "estimated_irr_at_entry": getattr(loan, "estimated_irr_at_entry", None),
            "equity_investment": (eq or 0.0) * fx if eq else None,
            "warrants_at_entry": getattr(loan, "warrants_at_entry", None),
            "warrant_strike_entry": getattr(loan, "warrant_strike_entry", None),
            "warrants_current": warrants_cur,
            "warrant_strike_current": getattr(loan, "warrant_strike_current", None),
            "warrant_term": getattr(loan, "warrant_term", None),
            "warrant_value": (uwev or 0.0) * fx if uwev else None,
            "business_description": getattr(loan, "business_description", None),
            "is_public": getattr(loan, "is_public", None),
            "recovery_rate": getattr(loan, "recovery_rate", None),
            "entry_collateral": (getattr(loan, "entry_collateral", None) or 0.0) * fx if getattr(loan, "entry_collateral", None) else None,
            "current_collateral": (getattr(loan, "current_collateral", None) or 0.0) * fx if getattr(loan, "current_collateral", None) else None,
            "entry_coverage_ratio": getattr(loan, "entry_coverage_ratio", None),
            "current_coverage_ratio": getattr(loan, "current_coverage_ratio", None),
            "entry_equity_cushion": getattr(loan, "entry_equity_cushion", None),
            "current_equity_cushion": getattr(loan, "current_equity_cushion", None),
        })

    return {
        # Rate mix
        "floating_count": floating_count,
        "fixed_count": fixed_count,
        "other_rate_count": other_rate_count,
        "floating_pct": safe_divide(floating_hold, total_hold),
        "fixed_pct": safe_divide(fixed_hold, total_hold),
        "total_hold": total_hold,
        # Weighted averages
        "wavg_spread_bps": safe_divide(spread_num, spread_den),
        "wavg_coupon": safe_divide(coupon_num, coupon_den),
        "wavg_ytm": safe_divide(ytm_num, ytm_den),
        "wavg_floor_rate": safe_divide(floor_num, floor_den),
        "wavg_call_protection_months": safe_divide(call_prot_num, call_prot_den),
        # Coverage counts
        "spread_coverage": spread_count,
        "coupon_coverage": coupon_count,
        "ytm_coverage": ytm_count,
        "floor_coverage": floor_count,
        "call_prot_coverage": call_prot_count,
        # Fees
        "loans_with_fees": loans_with_fees,
        "wavg_oid": safe_divide(total_fee_oid, spread_den) if spread_den > 0 else None,
        "total_fee_upfront": total_fee_upfront if total_fee_upfront > 0 else None,
        "total_fee_exit": total_fee_exit if total_fee_exit > 0 else None,
        "total_closing_fee": total_closing_fee if total_closing_fee > 0 else None,
        # PIK
        "pik_count": pik_count,
        "pik_pct": safe_divide(pik_hold, total_hold),
        # Amortization
        "amortization_distribution": {k: dict(v) for k, v in amort_dist.items()},
        "payment_frequency_distribution": {k: dict(v) for k, v in payment_freq_dist.items()},
        # Par/outstanding
        "total_original_par": total_original_par if total_original_par > 0 else None,
        "total_current_outstanding": total_current_outstanding if total_current_outstanding > 0 else None,
        "total_issue_size": total_issue_size if total_issue_size > 0 else None,
        "total_accrued_interest": total_accrued_interest if total_accrued_interest > 0 else None,
        # Warrants/equity
        "total_equity_investment": total_equity_investment if total_equity_investment > 0 else None,
        "total_warrant_value": total_warrant_value if total_warrant_value > 0 else None,
        "loans_with_warrants": loans_with_warrants,
        "loans_with_equity": loans_with_equity,
        # Per-loan detail
        "rows": rows,
    }

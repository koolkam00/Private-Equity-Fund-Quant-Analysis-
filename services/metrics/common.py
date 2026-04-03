"""Shared numerical helpers for deal-only portfolio analytics."""

from __future__ import annotations

import math
from datetime import date


EPS = 1e-9


def safe_divide(numerator, denominator, default=None):
    if numerator is None or denominator is None:
        return default
    try:
        # Guard against NaN/Inf inputs propagating through calculations.
        if isinstance(numerator, float) and (math.isnan(numerator) or math.isinf(numerator)):
            return default
        if isinstance(denominator, float) and (math.isnan(denominator) or math.isinf(denominator)):
            return default
        if denominator == 0:
            return default
        out = numerator / denominator
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        return default


def safe_power(base, exponent, default=None):
    if base is None or exponent is None:
        return default
    try:
        if isinstance(base, float) and (math.isnan(base) or math.isinf(base)):
            return default
        if isinstance(exponent, float) and (math.isnan(exponent) or math.isinf(exponent)):
            return default
        out = base ** exponent
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        return default


def safe_log(value, default=None):
    if value is None:
        return default
    try:
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return default
        if value <= 0:
            return default
        out = math.log(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except (TypeError, ValueError, OverflowError):
        return default


def effective_exit_date(deal, as_of_date=None):
    if deal.exit_date is not None:
        return deal.exit_date
    if as_of_date is not None:
        return as_of_date
    return date.today()


def deal_hold_years(deal, as_of_date=None):
    if deal.investment_date is None:
        return None
    end_date = effective_exit_date(deal, as_of_date=as_of_date)
    delta_days = (end_date - deal.investment_date).days
    if delta_days <= 0:
        return None
    return delta_days / 365.25


def percentile_rank(value, values):
    """Return the percentile rank (0.0-1.0) of *value* within *values*.

    Uses inclusive ranking: count(v <= value) / len(values).
    Returns 0.0 if values is empty or value is None.
    Filters out None values from the comparison set.
    """
    if value is None:
        return 0.0
    valid = [v for v in values if v is not None]
    if not valid:
        return 0.0
    count_le = sum(1 for v in valid if v <= value)
    return count_le / len(valid)


def resolve_analysis_as_of_date(deals):
    explicit_as_of_dates = [getattr(deal, "as_of_date", None) for deal in deals if getattr(deal, "as_of_date", None) is not None]
    if explicit_as_of_dates:
        return max(explicit_as_of_dates)

    exit_dates = [deal.exit_date for deal in deals if getattr(deal, "exit_date", None) is not None]
    if exit_dates:
        return max(exit_dates)

    investment_dates = [deal.investment_date for deal in deals if getattr(deal, "investment_date", None) is not None]
    if investment_dates:
        return max(investment_dates)

    return date.today()

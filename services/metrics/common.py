"""Shared numerical helpers for deal-only portfolio analytics."""

from __future__ import annotations

import math
from datetime import date


EPS = 1e-9


def safe_divide(numerator, denominator, default=None):
    if numerator is None or denominator is None:
        return default
    try:
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

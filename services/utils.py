"""Shared parsing and formatting utilities for deal-level ingestion."""

import re

import pandas as pd

EMPTY_STRINGS = {"", "nan", "none", "nat", "n/a", "na", "-", "--", "#n/a", "#ref!"}
DEFAULT_CURRENCY_CODE = "USD"
CURRENCY_CODE_RE = re.compile(r"^[A-Z]{3}$")

# Keep this compact and pragmatic. Unknown ISO codes still render via code fallback.
CURRENCY_SYMBOLS = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "CAD": "C$",
    "AUD": "A$",
    "JPY": "¥",
    "CHF": "CHF",
    "SEK": "kr",
    "NOK": "kr",
    "DKK": "kr",
    "NZD": "NZ$",
    "SGD": "S$",
    "HKD": "HK$",
    "INR": "₹",
    "CNY": "¥",
    "AED": "AED",
    "ZAR": "R",
    "BRL": "R$",
    "MXN": "MX$",
}


def clean_val(val):
    """Return None for NaN/NaT, otherwise the value as-is."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def clean_str(val):
    """Return None for empty/placeholder values, otherwise stripped string."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in EMPTY_STRINGS:
        return None
    return s


def normalize_currency_code(value, default=DEFAULT_CURRENCY_CODE):
    """Normalize ISO-3 currency code; blank values resolve to default."""
    s = clean_str(value)
    if s is None:
        return default
    code = s.upper()
    if not CURRENCY_CODE_RE.match(code):
        return None
    return code


def currency_symbol(currency_code):
    code = normalize_currency_code(currency_code, default=DEFAULT_CURRENCY_CODE) or DEFAULT_CURRENCY_CODE
    return CURRENCY_SYMBOLS.get(code)


def currency_unit_label(currency_code):
    code = normalize_currency_code(currency_code, default=DEFAULT_CURRENCY_CODE) or DEFAULT_CURRENCY_CODE
    symbol = currency_symbol(code)
    if symbol:
        return f"{code} {symbol}M"
    return f"{code} M"


def format_currency_millions(value, currency_code=DEFAULT_CURRENCY_CODE, show_code=True):
    if value is None:
        return "—"
    code = normalize_currency_code(currency_code, default=DEFAULT_CURRENCY_CODE) or DEFAULT_CURRENCY_CODE
    symbol = currency_symbol(code)
    amount = abs(float(value))
    sign = "-" if float(value) < 0 else ""
    if symbol:
        core = f"{sign}{symbol}{amount:.1f}M"
    else:
        core = f"{sign}{amount:.1f}M"
    return f"{code} {core}" if show_code else core

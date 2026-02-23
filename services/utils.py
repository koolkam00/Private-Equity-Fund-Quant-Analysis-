"""Shared parsing utilities for deal-level ingestion."""

import pandas as pd

EMPTY_STRINGS = {"", "nan", "none", "nat", "n/a", "na", "-", "--", "#n/a", "#ref!"}


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

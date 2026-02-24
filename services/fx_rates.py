"""Historical FX resolver utilities (ECB-backed) for upload-date USD conversion."""

from __future__ import annotations

from datetime import date, datetime
import json
import math
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from services.utils import DEFAULT_CURRENCY_CODE, normalize_currency_code

FX_SOURCE_LABEL = "Frankfurter (ECB)"
FRANKFURTER_BASE_URL = "https://api.frankfurter.app"


def _normalize_date(value):
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.today()


def _bad_result(currency_code, warning):
    return {
        "ok": False,
        "rate": None,
        "effective_date": None,
        "source": FX_SOURCE_LABEL,
        "warning": warning,
        "currency_code": currency_code,
    }


def resolve_rate_to_usd(currency_code, as_of_date):
    """Resolve historical FX rate to USD for a given date.

    Returns:
        {
          ok: bool,
          rate: float|None,
          effective_date: date|None,
          source: str,
          warning: str|None,
          currency_code: str
        }
    """
    code = normalize_currency_code(currency_code, default=DEFAULT_CURRENCY_CODE) or DEFAULT_CURRENCY_CODE
    ref_date = _normalize_date(as_of_date)

    if code == DEFAULT_CURRENCY_CODE:
        return {
            "ok": True,
            "rate": 1.0,
            "effective_date": ref_date,
            "source": "Identity",
            "warning": None,
            "currency_code": code,
        }

    query = urlencode({"from": code, "to": DEFAULT_CURRENCY_CODE})
    url = f"{FRANKFURTER_BASE_URL}/{ref_date.isoformat()}?{query}"

    try:
        with urlopen(url, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        return _bad_result(code, f"FX lookup failed for {code}->USD on {ref_date.isoformat()}: {exc}")

    rates = payload.get("rates") or {}
    rate = rates.get(DEFAULT_CURRENCY_CODE)
    try:
        rate = float(rate)
    except (TypeError, ValueError):
        rate = None
    if rate is None or not math.isfinite(rate) or rate <= 0:
        return _bad_result(code, f"FX rate missing/invalid for {code}->USD on {ref_date.isoformat()}.")

    raw_date = payload.get("date")
    try:
        effective_date = datetime.strptime(raw_date, "%Y-%m-%d").date() if raw_date else ref_date
    except ValueError:
        effective_date = ref_date

    return {
        "ok": True,
        "rate": rate,
        "effective_date": effective_date,
        "source": FX_SOURCE_LABEL,
        "warning": None,
        "currency_code": code,
    }

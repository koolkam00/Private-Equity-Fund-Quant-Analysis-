"""Historical FX resolver utilities (ECB-backed) for upload-date USD conversion."""

from __future__ import annotations

from datetime import date, datetime
import json
import math
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from services.utils import DEFAULT_CURRENCY_CODE, normalize_currency_code

FX_SOURCE_LABEL = "Frankfurter (ECB)"
FRANKFURTER_BASE_URL = "https://api.frankfurter.app"
FX_TIMEOUT_SECONDS = 8
FX_MAX_ATTEMPTS = 3
FX_BACKOFF_BASE_SECONDS = 0.4
FX_USER_AGENT = "PrivateEquityFundAnalyzer/1.0"


def _warning_text(currency_code, ref_date, category):
    return f"FX lookup failed [{category}] for {currency_code}->USD on {ref_date.isoformat()}."


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


def _http_error_category(exc):
    status = int(getattr(exc, "code", 0) or 0)
    if status == 403:
        return "403_forbidden"
    if status == 429:
        return "429_rate_limited"
    if 500 <= status <= 599:
        return "http_5xx"
    if 400 <= status <= 499:
        return f"http_{status}"
    return "http_error"


def _retryable_http(exc):
    status = int(getattr(exc, "code", 0) or 0)
    return status == 429 or (500 <= status <= 599)


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
    request = Request(
        url,
        headers={
            "User-Agent": FX_USER_AGENT,
            "Accept": "application/json",
        },
    )

    payload = None
    for attempt in range(FX_MAX_ATTEMPTS):
        try:
            with urlopen(request, timeout=FX_TIMEOUT_SECONDS) as response:
                payload = json.loads(response.read().decode("utf-8"))
            break
        except HTTPError as exc:
            category = _http_error_category(exc)
            if _retryable_http(exc) and attempt < FX_MAX_ATTEMPTS - 1:
                time.sleep(FX_BACKOFF_BASE_SECONDS * (2 ** attempt))
                continue
            return _bad_result(code, _warning_text(code, ref_date, category))
        except TimeoutError:
            if attempt < FX_MAX_ATTEMPTS - 1:
                time.sleep(FX_BACKOFF_BASE_SECONDS * (2 ** attempt))
                continue
            return _bad_result(code, _warning_text(code, ref_date, "timeout"))
        except URLError as exc:
            reason = getattr(exc, "reason", None)
            reason_text = str(reason or "").lower()
            category = "timeout" if "timed out" in reason_text else "network_error"
            if attempt < FX_MAX_ATTEMPTS - 1:
                time.sleep(FX_BACKOFF_BASE_SECONDS * (2 ** attempt))
                continue
            return _bad_result(code, _warning_text(code, ref_date, category))
        except ValueError:
            return _bad_result(code, _warning_text(code, ref_date, "invalid_response"))

    if payload is None:
        return _bad_result(code, _warning_text(code, ref_date, "lookup_failed"))

    rates = payload.get("rates") or {}
    rate = rates.get(DEFAULT_CURRENCY_CODE)
    try:
        rate = float(rate)
    except (TypeError, ValueError):
        rate = None
    if rate is None or not math.isfinite(rate) or rate <= 0:
        return _bad_result(code, _warning_text(code, ref_date, "rate_missing"))

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

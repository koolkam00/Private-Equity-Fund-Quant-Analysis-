from datetime import date
from io import BytesIO
from urllib.error import HTTPError, URLError

from services.fx_rates import resolve_rate_to_usd


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._payload


def test_fx_resolver_sends_headers_and_parses_success(monkeypatch):
    def _fake_urlopen(req, timeout):
        assert req.get_header("User-agent") == "PrivateEquityFundAnalyzer/1.0"
        assert req.headers.get("Accept") == "application/json"
        assert timeout == 8
        return _FakeResponse(b'{"date":"2026-02-24","rates":{"USD":1.1777}}')

    monkeypatch.setattr("services.fx_rates.urlopen", _fake_urlopen)

    out = resolve_rate_to_usd("eur", date(2026, 2, 24))
    assert out["ok"] is True
    assert abs(out["rate"] - 1.1777) < 1e-9
    assert out["effective_date"] == date(2026, 2, 24)
    assert out["warning"] is None


def test_fx_resolver_retries_on_5xx_then_succeeds(monkeypatch):
    calls = {"count": 0}
    sleeps = []

    def _fake_urlopen(req, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise HTTPError(req.full_url, 500, "server error", hdrs=None, fp=BytesIO(b""))
        return _FakeResponse(b'{"date":"2026-02-24","rates":{"USD":1.1800}}')

    monkeypatch.setattr("services.fx_rates.urlopen", _fake_urlopen)
    monkeypatch.setattr("services.fx_rates.time.sleep", lambda s: sleeps.append(round(float(s), 1)))

    out = resolve_rate_to_usd("EUR", date(2026, 2, 24))
    assert out["ok"] is True
    assert abs(out["rate"] - 1.18) < 1e-9
    assert calls["count"] == 2
    assert sleeps == [0.4]


def test_fx_resolver_does_not_retry_on_403(monkeypatch):
    calls = {"count": 0}
    sleeps = []

    def _fake_urlopen(req, timeout):
        calls["count"] += 1
        raise HTTPError(req.full_url, 403, "forbidden", hdrs=None, fp=BytesIO(b""))

    monkeypatch.setattr("services.fx_rates.urlopen", _fake_urlopen)
    monkeypatch.setattr("services.fx_rates.time.sleep", lambda s: sleeps.append(s))

    out = resolve_rate_to_usd("EUR", date(2026, 2, 24))
    assert out["ok"] is False
    assert "[403_forbidden]" in (out["warning"] or "")
    assert calls["count"] == 1
    assert sleeps == []


def test_fx_resolver_retries_on_timeout(monkeypatch):
    calls = {"count": 0}
    sleeps = []

    def _fake_urlopen(req, timeout):
        calls["count"] += 1
        raise TimeoutError("timed out")

    monkeypatch.setattr("services.fx_rates.urlopen", _fake_urlopen)
    monkeypatch.setattr("services.fx_rates.time.sleep", lambda s: sleeps.append(round(float(s), 1)))

    out = resolve_rate_to_usd("EUR", date(2026, 2, 24))
    assert out["ok"] is False
    assert "[timeout]" in (out["warning"] or "")
    assert calls["count"] == 3
    assert sleeps == [0.4, 0.8]


def test_fx_resolver_flags_rate_missing(monkeypatch):
    monkeypatch.setattr(
        "services.fx_rates.urlopen",
        lambda req, timeout: _FakeResponse(b'{"date":"2026-02-24","rates":{}}'),
    )

    out = resolve_rate_to_usd("EUR", date(2026, 2, 24))
    assert out["ok"] is False
    assert "[rate_missing]" in (out["warning"] or "")

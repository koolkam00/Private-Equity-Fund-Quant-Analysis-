from datetime import date, datetime

from app import app
from models import Deal, Firm, db


def test_fx_refresh_cli_failed_only_updates_failed_non_usd(app_context, monkeypatch):
    firm_eur = Firm(
        name="CLI EUR Firm",
        slug="cli-eur-firm",
        base_currency="EUR",
        fx_rate_to_usd=None,
        fx_rate_date=None,
        fx_rate_source="Frankfurter (ECB)",
        fx_last_status="lookup_failed",
    )
    firm_usd = Firm(
        name="CLI USD Firm",
        slug="cli-usd-firm",
        base_currency="USD",
        fx_rate_to_usd=None,
        fx_rate_date=None,
        fx_rate_source="Identity",
        fx_last_status="lookup_failed",
    )
    db.session.add_all([firm_eur, firm_usd])
    db.session.flush()

    db.session.add(
        Deal(
            company_name="CLI Deal",
            fund_number="Fund CLI",
            firm_id=firm_eur.id,
            entry_enterprise_value=100.0,
            entry_net_debt=40.0,
            exit_enterprise_value=140.0,
            exit_net_debt=30.0,
            equity_invested=60.0,
            realized_value=80.0,
            unrealized_value=20.0,
            created_at=datetime(2025, 5, 20, 10, 0, 0),
        )
    )
    db.session.commit()

    calls = []

    def _fake_resolve(code, as_of):
        calls.append((code, as_of))
        return {
            "ok": True,
            "rate": 1.15,
            "effective_date": as_of,
            "source": "Frankfurter (ECB)",
            "warning": None,
            "currency_code": code,
        }

    monkeypatch.setattr("app.resolve_rate_to_usd", _fake_resolve)

    runner = app.test_cli_runner()
    result = runner.invoke(args=["fx-refresh", "--failed-only"])

    assert result.exit_code == 0
    assert "Summary: scanned=2, updated_ok=1, still_failed=0, skipped_usd=1" in result.output

    refreshed_eur = db.session.get(Firm, firm_eur.id)
    refreshed_usd = db.session.get(Firm, firm_usd.id)
    assert refreshed_eur is not None
    assert refreshed_eur.fx_last_status == "ok"
    assert abs((refreshed_eur.fx_rate_to_usd or 0.0) - 1.15) < 1e-9
    assert refreshed_eur.fx_rate_date == date(2025, 5, 20)
    assert refreshed_usd is not None
    assert refreshed_usd.fx_last_status == "lookup_failed"
    assert refreshed_usd.fx_rate_to_usd is None
    assert calls == [("EUR", date(2025, 5, 20))]


def test_fx_refresh_cli_scopes_by_firm_id_and_as_of(app_context, monkeypatch):
    firm_a = Firm(
        name="CLI Firm A",
        slug="cli-firm-a",
        base_currency="EUR",
        fx_last_status="lookup_failed",
    )
    firm_b = Firm(
        name="CLI Firm B",
        slug="cli-firm-b",
        base_currency="GBP",
        fx_last_status="lookup_failed",
    )
    db.session.add_all([firm_a, firm_b])
    db.session.commit()

    calls = []

    def _fake_resolve(code, as_of):
        calls.append((code, as_of))
        return {
            "ok": False,
            "rate": None,
            "effective_date": None,
            "source": "Frankfurter (ECB)",
            "warning": f"FX lookup failed [network_error] for {code}->USD on {as_of.isoformat()}.",
            "currency_code": code,
        }

    monkeypatch.setattr("app.resolve_rate_to_usd", _fake_resolve)

    runner = app.test_cli_runner()
    result = runner.invoke(args=["fx-refresh", "--firm-id", str(firm_b.id), "--all", "--as-of", "2026-01-15"])

    assert result.exit_code == 0
    assert "Summary: scanned=1, updated_ok=0, still_failed=1, skipped_usd=0" in result.output
    assert calls == [("GBP", date(2026, 1, 15))]

    refreshed_a = db.session.get(Firm, firm_a.id)
    refreshed_b = db.session.get(Firm, firm_b.id)
    assert refreshed_a is not None and refreshed_a.fx_last_status == "lookup_failed"
    assert refreshed_b is not None and refreshed_b.fx_last_status == "lookup_failed"

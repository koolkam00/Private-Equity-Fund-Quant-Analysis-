"""Value creation bridge model for deal-only analytics."""

from __future__ import annotations

from services.metrics.common import EPS, safe_divide

DRIVERS = ("revenue", "margin", "multiple", "leverage", "other")


def _empty_bridge(unit="dollar", basis="fund"):
    return {
        "ready": False,
        "basis": basis,
        "unit": unit,
        "calculation_method": None,
        "fallback_reason": None,
        "ownership_pct": None,
        "drivers": {k: None for k in DRIVERS},
        "drivers_dollar": {k: None for k in DRIVERS},
        "value_created": None,
        "fund_value_created": None,
        "company_value_created": None,
        "fund_drivers_dollar": {k: None for k in DRIVERS},
        "company_drivers_dollar": {k: None for k in DRIVERS},
    }


def _normalize_unit(drivers_dollar, unit, equity, value_created):
    if unit == "dollar":
        return drivers_dollar
    if unit == "moic":
        if equity is None or equity <= 0:
            return {k: None for k in DRIVERS}
        return {k: safe_divide(v, equity) if v is not None else None for k, v in drivers_dollar.items()}
    if unit == "pct":
        if value_created is None or abs(value_created) < EPS:
            return {k: None for k in DRIVERS}
        return {k: safe_divide(v, value_created) if v is not None else None for k, v in drivers_dollar.items()}
    raise ValueError(f"Unsupported unit: {unit}")


def _derive_ownership(deal, warnings):
    if deal.ownership_pct is not None and deal.ownership_pct >= 0:
        return deal.ownership_pct

    if deal.entry_enterprise_value is None or deal.entry_net_debt is None:
        warnings.append("Ownership could not be derived from entry equity.")
        return None

    entry_equity = deal.entry_enterprise_value - deal.entry_net_debt
    if entry_equity is None or entry_equity <= 0:
        warnings.append("Entry equity <= 0; ownership fallback set to 100%.")
        return 1.0

    return safe_divide(deal.equity_invested, entry_equity, default=1.0)


def _select_basis(company_drivers, fund_drivers, basis):
    if basis == "company":
        return company_drivers
    if basis == "fund":
        return fund_drivers
    raise ValueError(f"Unsupported basis: {basis}")


def _required_bridge_inputs(deal):
    return all(
        v is not None
        for v in (
            deal.entry_revenue,
            deal.exit_revenue,
            deal.entry_enterprise_value,
            deal.exit_enterprise_value,
            deal.entry_net_debt,
            deal.exit_net_debt,
            deal.equity_invested,
        )
    )


def compute_additive_bridge(deal, warnings, basis="fund", unit="dollar"):
    if not _required_bridge_inputs(deal):
        warnings.append("Insufficient entry/exit data for additive bridge.")
        return _empty_bridge(unit=unit, basis=basis)

    eq = deal.equity_invested
    if eq is None or eq <= 0:
        warnings.append("Equity invested <= 0; additive bridge unavailable.")
        return _empty_bridge(unit=unit, basis=basis)

    r0, r1 = deal.entry_revenue, deal.exit_revenue
    # Treat missing EBITDA as zero so fallback bridge can still run from
    # revenue multiples + leverage even when EBITDA is not provided.
    e0 = 0.0 if deal.entry_ebitda is None else deal.entry_ebitda
    e1 = 0.0 if deal.exit_ebitda is None else deal.exit_ebitda
    ev0, ev1 = deal.entry_enterprise_value, deal.exit_enterprise_value
    nd0, nd1 = deal.entry_net_debt, deal.exit_net_debt

    if any(abs(v) < EPS for v in (r0, r1)):
        warnings.append("Near-zero revenue prevents robust additive bridge.")
        return _empty_bridge(unit=unit, basis=basis)

    if e0 <= 0 or e1 <= 0:
        rm0 = safe_divide(ev0, r0)
        rm1 = safe_divide(ev1, r1)
        if rm0 is None or rm1 is None:
            warnings.append("Invalid EV/Revenue prevents revenue-multiple fallback bridge.")
            return _empty_bridge(unit=unit, basis=basis)

        company = {
            "revenue": (r1 - r0) * rm0,
            "margin": 0.0,
            "multiple": (rm1 - rm0) * r1,
            "leverage": nd0 - nd1,
        }
        calculation_method = "revenue_multiple_fallback"
        fallback_reason = "negative_ebitda"
    else:
        if any(abs(v) < EPS for v in (e0, e1)):
            warnings.append("Near-zero EBITDA prevents robust additive bridge.")
            return _empty_bridge(unit=unit, basis=basis)

        m0 = e0 / r0
        m1 = e1 / r1
        x0 = ev0 / e0
        x1 = ev1 / e1

        if x0 < 0 or x1 < 0:
            warnings.append("Negative TEV/EBITDA multiple prevents additive bridge.")
            return _empty_bridge(unit=unit, basis=basis)

        company = {
            "revenue": (r1 - r0) * m0 * x0,
            "margin": r1 * (m1 - m0) * x0,
            "multiple": (x1 - x0) * e1,
            "leverage": nd0 - nd1,
        }
        calculation_method = "ebitda_additive"
        fallback_reason = None

    ownership = _derive_ownership(deal, warnings)
    if ownership is None:
        return _empty_bridge(unit=unit, basis=basis)

    fund = {k: v * ownership for k, v in company.items()}

    realized = 0.0 if deal.realized_value is None else deal.realized_value
    unrealized = 0.0 if deal.unrealized_value is None else deal.unrealized_value
    fund_value_created = (realized + unrealized) - eq
    company_value_created = safe_divide(fund_value_created, ownership) if ownership > EPS else None

    base_map = _select_basis(company, fund, basis)
    target_value_created = company_value_created if basis == "company" else fund_value_created

    subtotal = sum(base_map.values())
    other = target_value_created - subtotal if target_value_created is not None else None

    company_full = dict(company)
    fund_full = dict(fund)
    company_full["other"] = (
        company_value_created - sum(company.values()) if company_value_created is not None else None
    )
    fund_full["other"] = fund_value_created - sum(fund.values())

    selected_dollars = dict(base_map)
    selected_dollars["other"] = other

    return {
        "ready": True,
        "basis": basis,
        "unit": unit,
        "calculation_method": calculation_method,
        "fallback_reason": fallback_reason,
        "ownership_pct": ownership,
        "drivers": _normalize_unit(selected_dollars, unit, eq, target_value_created),
        "drivers_dollar": selected_dollars,
        "value_created": target_value_created,
        "fund_value_created": fund_value_created,
        "company_value_created": company_value_created,
        "fund_drivers_dollar": fund_full,
        "company_drivers_dollar": company_full,
    }


def compute_bridge_diagnostics(additive_bridge):
    out = {
        "ownership_sensitivity": {
            "ownership_base": additive_bridge.get("ownership_pct"),
            "driver_subtotal_base": None,
            "driver_subtotal_up_10": None,
            "driver_subtotal_down_10": None,
            "other_up_10": None,
            "other_down_10": None,
        }
    }

    if additive_bridge.get("ready"):
        ownership = additive_bridge.get("ownership_pct")
        fund_drivers = additive_bridge.get("fund_drivers_dollar", {})
        fund_value_created = additive_bridge.get("fund_value_created")
        if ownership is not None and ownership >= 0 and fund_value_created is not None:
            subtotal = sum((fund_drivers.get(k) or 0) for k in ("revenue", "margin", "multiple", "leverage"))
            up = subtotal * 1.10
            down = subtotal * 0.90
            out["ownership_sensitivity"] = {
                "ownership_base": ownership,
                "driver_subtotal_base": subtotal,
                "driver_subtotal_up_10": up,
                "driver_subtotal_down_10": down,
                "other_up_10": fund_value_created - up,
                "other_down_10": fund_value_created - down,
            }

    return out

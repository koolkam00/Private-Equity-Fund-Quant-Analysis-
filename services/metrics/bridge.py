"""Value creation bridge model for deal-only analytics."""

from __future__ import annotations

from services.metrics.common import EPS, safe_divide

DRIVERS = ("revenue", "margin", "multiple", "leverage", "other")
STANDARD_DISPLAY_DRIVER_KEYS = ("revenue", "margin", "multiple", "leverage", "other")
MISSING_REVENUE_DISPLAY_DRIVER_KEYS = ("ebitda_growth", "multiple", "leverage", "other")
AGGREGATE_DISPLAY_DRIVER_ORDER = ("revenue", "ebitda_growth", "margin", "multiple", "leverage", "other")
DISPLAY_DRIVER_LABELS = {
    "revenue": "Revenue Growth",
    "ebitda_growth": "EBITDA Growth",
    "margin": "Margin Expansion",
    "multiple": "Multiple Expansion",
    "leverage": "Leverage / Debt Paydown",
    "other": "Residual / Other",
}


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
        "display_drivers": [],
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
            return {k: None for k in drivers_dollar}
        return {k: safe_divide(v, equity) if v is not None else None for k, v in drivers_dollar.items()}
    if unit == "pct":
        if value_created is None or abs(value_created) < EPS:
            return {k: None for k in drivers_dollar}
        return {k: safe_divide(v, value_created) if v is not None else None for k, v in drivers_dollar.items()}
    raise ValueError(f"Unsupported unit: {unit}")


def _display_driver_order(calculation_method):
    if calculation_method == "ebitda_multiple_fallback":
        return MISSING_REVENUE_DISPLAY_DRIVER_KEYS
    return STANDARD_DISPLAY_DRIVER_KEYS


def _legacy_alias_map(canonical_drivers, calculation_method):
    if calculation_method == "ebitda_multiple_fallback":
        return {
            "revenue": canonical_drivers.get("ebitda_growth"),
            "margin": 0.0,
            "multiple": canonical_drivers.get("multiple"),
            "leverage": canonical_drivers.get("leverage"),
            "other": canonical_drivers.get("other"),
        }
    return {
        "revenue": canonical_drivers.get("revenue"),
        "margin": canonical_drivers.get("margin"),
        "multiple": canonical_drivers.get("multiple"),
        "leverage": canonical_drivers.get("leverage"),
        "other": canonical_drivers.get("other"),
    }


def _build_display_drivers(canonical_dollars, equity, value_created, calculation_method):
    rows = []
    for key in _display_driver_order(calculation_method):
        dollar = canonical_dollars.get(key)
        rows.append(
            {
                "key": key,
                "label": DISPLAY_DRIVER_LABELS.get(key, key),
                "dollar": dollar,
                "moic": safe_divide(dollar, equity) if dollar is not None else None,
                "pct": safe_divide(dollar, value_created) if dollar is not None else None,
            }
        )
    return rows


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

    revenue_available = all(v is not None and abs(v) >= EPS for v in (r0, r1))
    missing_revenue_both = all(v is None or abs(v) < EPS for v in (r0, r1))

    if revenue_available and (e0 <= 0 or e1 <= 0):
        rm0 = safe_divide(ev0, r0)
        rm1 = safe_divide(ev1, r1)
        if rm0 is None or rm1 is None:
            warnings.append("Invalid EV/Revenue prevents revenue-multiple fallback bridge.")
            return _empty_bridge(unit=unit, basis=basis)

        company_canonical = {
            "revenue": (r1 - r0) * rm0,
            "margin": 0.0,
            "multiple": (rm1 - rm0) * r1,
            "leverage": nd0 - nd1,
        }
        calculation_method = "revenue_multiple_fallback"
        fallback_reason = "negative_ebitda"
    elif revenue_available:
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

        company_canonical = {
            "revenue": (r1 - r0) * m0 * x0,
            "margin": r1 * (m1 - m0) * x0,
            "multiple": (x1 - x0) * e1,
            "leverage": nd0 - nd1,
        }
        calculation_method = "ebitda_additive"
        fallback_reason = None
    elif missing_revenue_both:
        if any(abs(v) < EPS for v in (e0, e1)):
            warnings.append("Insufficient revenue and non-positive EBITDA prevent additive bridge.")
            return _empty_bridge(unit=unit, basis=basis)

        x0 = safe_divide(ev0, e0)
        x1 = safe_divide(ev1, e1)
        if x0 is None or x1 is None:
            warnings.append("Invalid EV/EBITDA prevents EBITDA-multiple fallback bridge.")
            return _empty_bridge(unit=unit, basis=basis)
        if x0 < 0 or x1 < 0:
            warnings.append("Negative TEV/EBITDA multiple prevents EBITDA-multiple fallback bridge.")
            return _empty_bridge(unit=unit, basis=basis)

        company_canonical = {
            "ebitda_growth": (e1 - e0) * x0,
            "multiple": (x1 - x0) * e1,
            "leverage": nd0 - nd1,
        }
        calculation_method = "ebitda_multiple_fallback"
        fallback_reason = "missing_revenue"
    else:
        warnings.append("Partial revenue history prevents additive bridge fallback.")
        return _empty_bridge(unit=unit, basis=basis)

    ownership = _derive_ownership(deal, warnings)
    if ownership is None:
        return _empty_bridge(unit=unit, basis=basis)

    fund_canonical = {k: v * ownership for k, v in company_canonical.items()}

    realized = 0.0 if deal.realized_value is None else deal.realized_value
    unrealized = 0.0 if deal.unrealized_value is None else deal.unrealized_value
    fund_value_created = (realized + unrealized) - eq
    company_value_created = safe_divide(fund_value_created, ownership) if ownership > EPS else None

    base_map_canonical = _select_basis(company_canonical, fund_canonical, basis)
    target_value_created = company_value_created if basis == "company" else fund_value_created

    subtotal = sum(base_map_canonical.values())
    other = target_value_created - subtotal if target_value_created is not None else None

    # Flag large residual / "other" bucket — indicates bridge attribution
    # may not be capturing the dominant value drivers.
    if other is not None and target_value_created is not None and abs(target_value_created) > EPS:
        residual_pct = abs(other / target_value_created)
        if residual_pct > 0.25:
            warnings.append(
                f"Bridge residual is {residual_pct:.0%} of value created; "
                "attribution may be incomplete."
            )

    company_full_canonical = dict(company_canonical)
    fund_full_canonical = dict(fund_canonical)
    company_full_canonical["other"] = (
        company_value_created - sum(company_canonical.values()) if company_value_created is not None else None
    )
    fund_full_canonical["other"] = fund_value_created - sum(fund_canonical.values())

    selected_canonical_dollars = dict(base_map_canonical)
    selected_canonical_dollars["other"] = other
    selected_legacy_dollars = _legacy_alias_map(selected_canonical_dollars, calculation_method)
    company_legacy_dollars = _legacy_alias_map(company_full_canonical, calculation_method)
    fund_legacy_dollars = _legacy_alias_map(fund_full_canonical, calculation_method)

    return {
        "ready": True,
        "basis": basis,
        "unit": unit,
        "calculation_method": calculation_method,
        "fallback_reason": fallback_reason,
        "ownership_pct": ownership,
        "drivers": _normalize_unit(selected_legacy_dollars, unit, eq, target_value_created),
        "drivers_dollar": selected_legacy_dollars,
        "display_drivers": _build_display_drivers(
            selected_canonical_dollars,
            eq,
            target_value_created,
            calculation_method,
        ),
        "value_created": target_value_created,
        "fund_value_created": fund_value_created,
        "company_value_created": company_value_created,
        "fund_drivers_dollar": fund_legacy_dollars,
        "company_drivers_dollar": company_legacy_dollars,
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

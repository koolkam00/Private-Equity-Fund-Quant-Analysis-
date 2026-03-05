from __future__ import annotations


TRACK_RECORD_STATUS_ORDER = ("Fully Realized", "Partially Realized", "Unrealized", "Other")


def normalize_realization_status(raw_status):
    status = (raw_status or "").strip().lower()
    if "partial" in status and "realized" in status:
        return "Partially Realized"
    if "fully" in status and "realized" in status:
        return "Fully Realized"
    if status == "realized" or ("realized" in status and "unrealized" not in status):
        return "Fully Realized"
    if "unrealized" in status or status == "":
        return "Unrealized"
    return "Other"


def normalize_status_rollup(raw_status):
    normalized = normalize_realization_status(raw_status)
    if normalized in {"Fully Realized", "Partially Realized"}:
        return "Realized"
    return normalized


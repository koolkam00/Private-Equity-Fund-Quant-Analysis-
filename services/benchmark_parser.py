import uuid

import pandas as pd

from models import BenchmarkPoint, db
from services.utils import clean_str, clean_val


COLUMN_MAP = {
    "asset class": "asset_class",
    "asset": "asset_class",
    "assetclass": "asset_class",
    "vintage year": "vintage_year",
    "vintage": "vintage_year",
    "year": "vintage_year",
    "metric": "metric",
    "quartile": "quartile",
    "category": "quartile",
    "value": "value",
}

VALID_COLUMNS = {"asset_class", "vintage_year", "metric", "quartile", "value"}

METRIC_MAP = {
    "net irr": "net_irr",
    "irr": "net_irr",
    "net moic": "net_moic",
    "moic": "net_moic",
    "net tvpi": "net_moic",
    "tvpi": "net_moic",
    "net dpi": "net_dpi",
    "dpi": "net_dpi",
}

QUARTILE_MAP = {
    "lower quartile": "lower_quartile",
    "median": "median",
    "upper quartile": "upper_quartile",
    "top 5%": "top_5",
    "top 5": "top_5",
    "top5%": "top_5",
    "top5": "top_5",
}


def _normalize_metric(value):
    text = clean_str(value)
    if text is None:
        return None
    return METRIC_MAP.get(" ".join(text.lower().split()))


def _normalize_quartile(value):
    text = clean_str(value)
    if text is None:
        return None
    normalized = " ".join(text.lower().replace("percent", "%").split())
    return QUARTILE_MAP.get(normalized)


def parse_benchmarks(file_path, team_id, replace_mode="replace_all"):
    """Parse benchmark workbook and replace team-scoped benchmark points."""
    upload_batch = str(uuid.uuid4())[:8]

    if team_id is None:
        return {
            "success": False,
            "errors": ["Team scope is required for benchmark upload."],
            "upload_batch": upload_batch,
            "rows_loaded": 0,
            "asset_classes": [],
            "vintage_min": None,
            "vintage_max": None,
        }

    BenchmarkPoint.__table__.create(bind=db.engine, checkfirst=True)

    df = pd.read_excel(file_path, engine="openpyxl")
    if df.empty:
        return {
            "success": False,
            "errors": ["Benchmark workbook is empty."],
            "upload_batch": upload_batch,
            "rows_loaded": 0,
            "asset_classes": [],
            "vintage_min": None,
            "vintage_max": None,
        }

    df.columns = [str(col).strip().lower() for col in df.columns]
    rename_map = {col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP}
    df = df.rename(columns=rename_map)

    missing = [col for col in ("asset_class", "vintage_year", "metric", "quartile", "value") if col not in df.columns]
    if missing:
        return {
            "success": False,
            "errors": [
                "Benchmark file is missing required columns: " + ", ".join(missing)
            ],
            "upload_batch": upload_batch,
            "rows_loaded": 0,
            "asset_classes": [],
            "vintage_min": None,
            "vintage_max": None,
        }

    df = df[[col for col in df.columns if col in VALID_COLUMNS]]

    errors = []
    rows = []
    seen = {}

    for idx, row in df.iterrows():
        row_num = idx + 2
        if row.isna().all():
            continue

        raw = {k: clean_val(v) for k, v in row.to_dict().items()}
        asset_class = clean_str(raw.get("asset_class"))
        if asset_class is None:
            errors.append(f"Row {row_num}: Asset Class is required.")
            continue

        vintage_raw = clean_val(raw.get("vintage_year"))
        try:
            vintage_year = int(float(vintage_raw))
        except (TypeError, ValueError):
            vintage_year = None
        if vintage_year is None:
            errors.append(f"Row {row_num}: Vintage Year must be an integer year.")
            continue

        metric = _normalize_metric(raw.get("metric"))
        if metric is None:
            errors.append(
                f"Row {row_num}: Metric must be one of Net IRR, Net MOIC/MOIC, Net TVPI/TVPI, Net DPI/DPI."
            )
            continue

        quartile = _normalize_quartile(raw.get("quartile"))
        if quartile is None:
            errors.append(
                f"Row {row_num}: Quartile must be one of Lower Quartile, Median, Upper Quartile, Top 5%."
            )
            continue

        value_raw = clean_val(raw.get("value"))
        try:
            value = float(value_raw)
        except (TypeError, ValueError):
            value = None
        if value is None:
            errors.append(f"Row {row_num}: Value must be numeric.")
            continue

        key = (asset_class.strip().lower(), vintage_year, metric, quartile)
        if key in seen:
            first_row = seen[key]
            errors.append(
                f"Row {row_num}: Duplicate benchmark key for Asset Class '{asset_class}', Vintage {vintage_year}, "
                f"Metric {metric}, Quartile {quartile} (first seen on row {first_row})."
            )
            continue

        seen[key] = row_num
        rows.append(
            {
                "asset_class": asset_class,
                "vintage_year": vintage_year,
                "metric": metric,
                "quartile": quartile,
                "value": value,
            }
        )

    if errors:
        return {
            "success": False,
            "errors": errors,
            "upload_batch": upload_batch,
            "rows_loaded": 0,
            "asset_classes": sorted({row[0] for row in seen.keys()}),
            "vintage_min": min((row[1] for row in seen.keys()), default=None),
            "vintage_max": max((row[1] for row in seen.keys()), default=None),
        }

    if not rows:
        return {
            "success": False,
            "errors": ["No benchmark rows found."],
            "upload_batch": upload_batch,
            "rows_loaded": 0,
            "asset_classes": [],
            "vintage_min": None,
            "vintage_max": None,
        }

    if replace_mode == "replace_all":
        BenchmarkPoint.query.filter_by(team_id=team_id).delete(synchronize_session=False)

    for row in rows:
        db.session.add(
            BenchmarkPoint(
                team_id=team_id,
                asset_class=row["asset_class"],
                vintage_year=row["vintage_year"],
                metric=row["metric"],
                quartile=row["quartile"],
                value=row["value"],
                upload_batch=upload_batch,
            )
        )

    db.session.commit()

    vintage_values = [row["vintage_year"] for row in rows]
    return {
        "success": True,
        "errors": [],
        "upload_batch": upload_batch,
        "rows_loaded": len(rows),
        "asset_classes": sorted({row["asset_class"] for row in rows}),
        "vintage_min": min(vintage_values),
        "vintage_max": max(vintage_values),
    }

# AGENT_QUICKSTART

## What This Repo Is

- `Observed:` This is a single Flask web app for private-equity portfolio analytics, fund-level LP diligence views, Excel-based data ingestion, and PDF export.
- `Observed:` Production traffic enters through `wsgi.py`, which builds the app via `peqa/app_factory.py::create_app` and then binds almost all real routes from `legacy_app.py`.
- `Observed:` The repo has a partial package refactor (`peqa/`), but the route layer, upload flow, PDF generation, and most orchestration still live in `legacy_app.py`.

## Read These Files First

1. `peqa/app_factory.py:68` and `wsgi.py:1`
   Why: app bootstrap, extension init, logging, and route binding.
2. `legacy_app.py:2238`, `legacy_app.py:2551`, `legacy_app.py:2713`, `legacy_app.py:3102`
   Why: upload flow, dashboard payload, analysis dispatch, and health/readiness endpoints.
3. `services/deal_parser.py:1350`
   Why: main workbook ingestion path; many downstream bugs start here.
4. `models.py`
   Why: real persistence model and scoping model (`team_id`, `firm_id`, upload batches, supplemental tables).
5. `peqa/services/context.py` and `peqa/services/filtering.py`
   Why: active-team/active-firm scoping and page filters.
6. `services/metrics/deal.py`, `services/metrics/portfolio.py`, `services/metrics/analysis.py`, `services/metrics/lp.py`
   Why: almost all business logic is here.
7. `services/metrics/chart_builder.py`
   Why: dynamic catalog/query engine, separate from page-specific analyses.
8. `tests/`
   Why: current expected behavior is better documented by tests than by the older markdown files.

## How To Run

- Install deps:

```bash
python3 -m pip install -r requirements.txt
```

- Initialize schema:

```bash
python3 -m flask --app app db-upgrade
```

- Optional first-user bootstrap:

```bash
export BOOTSTRAP_ADMIN_EMAIL=admin@example.com
export BOOTSTRAP_ADMIN_PASSWORD=strong-password
export BOOTSTRAP_TEAM_NAME="Admin Team"
python3 -m flask --app app bootstrap-admin
```

- Run locally:

```bash
python3 -m flask --app app run
```

- Production entrypoint:

```bash
gunicorn wsgi:app
```

## How To Test

- Main test command:

```bash
pytest -q
```

- `Observed:` Repository scan run result: `187 passed` in `27.64s`.

## How To Trace A Bug

1. Confirm scope first.
   `legacy_app.py::_current_membership`, `_resolve_active_firm_for_team`, and `peqa/services/context.py::build_analysis_context` decide what rows are even visible.
2. Determine the path.
   Dashboard bugs usually go through `legacy_app.py::_build_dashboard_payload`; analysis-page bugs go through `legacy_app.py::_analysis_route_payload`; upload bugs go through `legacy_app.py::_handle_upload` into `services/deal_parser.py::parse_deals`.
3. Check whether the feature needs supplemental tables.
   Many LP pages need `fund_quarter_snapshots`, `fund_cashflows`, `fund_metadata`, `public_market_index_levels`, or `deal_underwrite_baselines`, not just `deals`.
4. Check scaling/currency after computation.
   `legacy_app.py::_reporting_currency_context` and `_scale_*` helpers can change payload values after the raw metric function returns.
5. Use tests as executable docs.
   `tests/test_parsers.py`, `tests/test_metrics.py`, `tests/test_routes.py`, `tests/test_security.py`.

## How To Safely Make A Change

- Add or update tests before touching `legacy_app.py` orchestration.
- Preserve team and firm scoping unless the task explicitly changes access semantics.
- If changing uploads, test both single-firm and multi-firm workbooks.
- If changing LP analyses, verify whether the page depends on optional sheets.
- Run `pytest -q` after changes.
- Do not trust `architecture_and_calculations.md` or `methodology.md` as the source of truth; they are partially stale.

## Common Traps

- `Observed:` `peqa/blueprints/*.py` is mostly scaffolding. Real view functions are still in `legacy_app.py`.
- `Observed:` Routes are registered twice: once directly on the Flask app and once through blueprints (`peqa/route_binding.py::AppBinder.register`). Runtime `app.url_map` contains duplicate rules such as `/dashboard` and `/analysis/<page>`.
- `Observed:` The app defaults to SQLite at `instance/deals.db` when no DB env var is set.
- `Observed:` Rate limiting uses in-memory storage (`peqa/extensions.py:19`), so it is process-local.
- `Observed:` Chart Builder and most analytics aggregate in Python per request; large datasets will stress request latency.

## Fastest Path To Productivity

- For upload/data bugs: read `services/deal_parser.py`, then `models.py`, then `tests/test_parsers.py`.
- For dashboard/report issues: read `legacy_app.py::_build_dashboard_payload`, `services/metrics/portfolio.py`, and `tests/test_routes.py`.
- For LP-analysis issues: read `legacy_app.py::_analysis_route_payload`, then `services/metrics/lp.py`, then `tests/test_metrics.py`.
- For chart-builder issues: read `services/metrics/chart_builder.py`, `legacy_app.py::chart_builder_*`, and `static/js/app.js`.

## Evidence

- `wsgi.py`
- `peqa/app_factory.py:68-91`
- `legacy_app.py:2238`, `legacy_app.py:2551`, `legacy_app.py:2713`, `legacy_app.py:3102-3114`
- `services/deal_parser.py:1350-1696`
- `peqa/route_binding.py:52-74`
- `peqa/extensions.py:19`
- `tests/`

# REPO_OVERVIEW

## Repo Purpose

- `Observed:` The system ingests Excel workbooks containing private-equity deal, fund, benchmark, and public-market data, stores them in SQLAlchemy models, and renders analytics pages for dashboarding, benchmarking, liquidity, risk, reporting quality, chart building, and print/PDF output.

## Primary Product / Runtime Behavior

- `Observed:` Authenticated users belong to teams and operate within an active firm scope stored in session.
- `Observed:` The dashboard and reports are server-rendered Jinja pages backed by JSON endpoints and Chart.js.
- `Observed:` The app supports both deal-only analytics and richer LP-oriented analytics that depend on supplemental uploaded tables.

## Tech Stack

- `Observed:` Backend: Flask, Flask-SQLAlchemy, Flask-Login, Flask-WTF, Flask-Migrate, Flask-Limiter.
- `Observed:` Data tooling: pandas, openpyxl.
- `Observed:` Reporting: ReportLab, browser print flows.
- `Observed:` Frontend: Jinja templates, Chart.js via CDN, vanilla JavaScript in `static/js/app.js`.
- `Observed:` Databases: SQLite by default; PostgreSQL via `DATABASE_URL` / `SQLALCHEMY_DATABASE_URI`.

## Major Architecture Pattern

- `Observed:` Hybrid architecture.
  - App factory and extensions live in `peqa/`.
  - Most runtime routes, orchestration, CLI commands, upload handlers, and PDF builders remain in `legacy_app.py`.
  - Metric computation is concentrated in `services/metrics/`.
- `Inferred:` The repo is in the middle of a gradual refactor from a flat Flask module into a packaged app, but the refactor is not yet complete.

## Key Entrypoints

- `Observed:` `wsgi.py` -> production Gunicorn entrypoint.
- `Observed:` `app.py` -> compatibility entrypoint used by Flask CLI and tests.
- `Observed:` `peqa/app_factory.py::create_app` -> bootstrap, extensions, request logging, route binding.
- `Observed:` `legacy_app.py` -> route handlers, CLI commands, analysis dispatch, uploads, PDFs.

## Main Subsystems

- `Observed:` Authentication and team invites
  - `legacy_app.py::login`, `logout`, `team`, `create_team_invite`, `accept_invite`
- `Observed:` Scope resolution
  - `legacy_app.py::_current_membership`, `_resolve_active_firm_for_team`
  - `peqa/services/context.py`
- `Observed:` Workbook ingestion
  - `services/deal_parser.py::parse_deals`
  - `services/benchmark_parser.py::parse_benchmarks`
- `Observed:` Core analytics
  - `services/metrics/deal.py`
  - `services/metrics/portfolio.py`
- `Observed:` Extended analysis pages
  - `services/metrics/analysis.py`
  - `services/metrics/lp.py`
  - `services/metrics/vca_ebitda.py`
  - `services/metrics/vca_revenue.py`
  - `services/metrics/benchmarking.py`
  - `services/metrics/ic_memo.py`
  - `services/metrics/methodology.py`
- `Observed:` Chart builder
  - `services/metrics/chart_builder.py`
  - `ChartBuilderTemplate` model
- `Observed:` Export / print
  - `legacy_app.py::_build_track_record_pdf`
  - `legacy_app.py::_build_vca_pdf`
  - `legacy_app.py::_build_benchmarking_pdf`
  - `/reports/ic-pdf-pack`
  - `services/excel_exporter.py::export_firm_to_excel` — reconstructs multi-sheet Excel from DB data for a firm

## Important Dependencies

- `Observed:` `pandas` and `openpyxl` are required for upload parsing.
- `Observed:` `reportlab` is required for server-side PDF generation.
- `Observed:` `psycopg[binary]` is the PostgreSQL driver for production.
- `Observed:` `Flask-Limiter` protects login, invite, upload, and chart-builder POST routes.
- `Observed:` `services/fx_rates.py` calls the Frankfurter/ECB API for FX metadata.

## Deployment Shape

- `Observed:` `render.yaml` defines one Render web service and one Render Postgres database.
- `Observed:` Build command: `pip install -r requirements.txt`
- `Observed:` Pre-deploy command: `python -m flask --app app db-upgrade`
- `Observed:` Start command: `gunicorn wsgi:app`
- `Observed:` Health endpoints: `/healthz` and `/readyz`

## Biggest Complexity Hotspots

- `Observed:` `legacy_app.py` is a 4k+ line orchestration file handling auth, scope, uploads, dashboards, analysis routing, CLI, scaling, and PDF output.
- `Observed:` `services/deal_parser.py` is the critical ingestion spine and supports eight workbook sheet types with replacement semantics and issue logging.
- `Observed:` `services/metrics/lp.py` contains the densest fund/LP logic and cross-table joins.
- `Observed:` `services/metrics/chart_builder.py` is a second analytics engine with its own field catalog, row loaders, filter logic, aggregation, and payload rendering.

## Evidence

- `requirements.txt`
- `render.yaml`
- `wsgi.py`
- `app.py`
- `peqa/app_factory.py:68-91`
- `legacy_app.py:1205-1227`, `legacy_app.py:2238-3114`, `legacy_app.py:3126-4399`
- `services/deal_parser.py:1350-1696`
- `services/metrics/*.py`

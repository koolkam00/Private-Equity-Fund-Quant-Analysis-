# EXECUTION_FLOW

## Startup / Bootstrap Flow

1. `Observed:` `wsgi.py` imports `peqa.app_factory.create_app()` and exposes `app`.
2. `Observed:` `peqa/app_factory.py::create_app`
   - creates the Flask app with root-level `templates/` and `static/`
   - loads `Config`
   - creates `UPLOAD_FOLDER`
   - initializes `db`, `login_manager`, `csrf`, `migrate`, `limiter`
   - registers request logging
   - calls `legacy_binder.register(app, get_blueprints(), ROUTE_BLUEPRINTS)`
3. `Observed:` Route definitions were recorded earlier on the `AppBinder` instance created in `legacy_app.py`.
4. `Observed:` The login manager is configured to redirect unauthenticated users to `login`.

## Request Lifecycle

1. `Observed:` `peqa/app_factory.py::_register_request_logging`
   - adds `g.request_id`
   - records request start time
   - logs JSON payload after each response
   - adds `X-Request-ID` response header
2. `Observed:` `legacy_app.py` view functions usually start by resolving user/team/firm scope.
3. `Observed:` `legacy_app.py::inject_global_scope_context` injects active firm, currency metadata, and team/admin state into every authenticated template render.

## Auth / Scope Flow

1. `Observed:` `legacy_app.py::login` validates email/password, finds first team membership, logs the user in, and sets `session["active_team_id"]`.
2. `Observed:` `legacy_app.py::_resolve_active_firm_for_team` picks the active firm from session or defaults to the first accessible firm with data.
3. `Observed:` Most data queries then rely on:
   - `peqa/services/filtering.py::build_deal_scope_query`
   - `peqa/services/context.py::build_analysis_context`

## Dashboard / Analysis Flow

1. `Observed:` `legacy_app.py::_build_filtered_deals_context`
   - reads request filters
   - resolves active firm and team
   - calls `build_analysis_context`
2. `Observed:` `build_analysis_context`
   - queries scoped deals
   - builds filter options
   - computes `metrics_by_id`
   - resolves selected benchmark asset class
3. `Observed:` Route-specific orchestration then branches:
   - dashboard -> `legacy_app.py::_build_dashboard_payload`
   - analysis pages -> `legacy_app.py::_analysis_route_payload`
   - deals page -> `services/metrics/portfolio.py::compute_deal_track_record` and `compute_deals_rollup_details`
4. `Observed:` After computation, `legacy_app.py::_reporting_currency_context` and `_scale_*` helpers may convert/scaled monetary values for display.

## Upload Flow

1. `Observed:` `/upload/deals` -> `legacy_app.py::upload_deals` -> `_handle_upload(parse_deals, "deals")`
2. `Observed:` `_handle_upload`
   - validates extension
   - saves upload to `UPLOAD_FOLDER`
   - calls `services/deal_parser.py::parse_deals`
   - flashes summary messages
   - removes the temp file
3. `Observed:` `parse_deals`
   - reads the workbook
   - validates required `Deals` columns (`Firm Name`, `As Of Date`, `Company Name`)
   - groups rows by firm
   - resolves/creates `Firm`
   - refreshes firm FX metadata
   - optionally replaces prior fund data
   - inserts `Deal` rows
   - parses optional sheets:
     - `Cashflows`
     - `Deal Quarterly`
     - `Fund Quarterly`
     - `Underwrite`
     - `Fund Metadata`
     - `Fund Cashflows`
     - `Public Market Benchmarks`
   - commits and returns counts/warnings
4. `Observed:` Upload problems are recorded to `upload_issues` via `_record_issue`.

## Benchmark Upload Flow

1. `Observed:` `/upload/benchmarks` -> `legacy_app.py::upload_benchmarks`
2. `Observed:` `services/benchmark_parser.py::parse_benchmarks`
   - normalizes column aliases
   - validates metric/quartile vocab
   - enforces duplicate-key rejection
   - replaces existing `BenchmarkPoint` rows at team scope when `replace_mode="replace_all"`

## Chart Builder Flow

1. `Observed:` `/analysis/chart-builder` and `/api/chart-builder/catalog`
   - call `services/metrics/chart_builder.py::build_chart_field_catalog`
2. `Observed:` `/api/chart-builder/query`
   - validates JSON spec
   - normalizes global filters
   - loads source rows into Python
   - applies local filters
   - aggregates into chart/table payload
3. `Observed:` Template CRUD routes persist chart configs in `chart_builder_templates`.

## PDF / Report Flow

- `Observed:` Track record PDF
  - `/track-record/pdf` -> `compute_deal_track_record` -> `_build_track_record_pdf`
- `Observed:` IC memo
  - `/ic-memo` renders a print-oriented HTML view; browser print is the export path
- `Observed:` Multi-file PDF pack
  - `/reports/ic-pdf-pack` computes track record, VCA EBITDA, VCA Revenue, and benchmarking payloads, generates PDFs with ReportLab, then zips them
- `Observed:` `/reports/ic-pdf-pack/live` links to the browser/native print versions

## Database / Cache / Queue Flow

- `Observed:` ORM: SQLAlchemy via `models.db`
- `Observed:` Default DB: SQLite at `instance/deals.db`
- `Observed:` Production DB: PostgreSQL via env var rewrite in `config.py`
- `Observed:` Cache: none
- `Observed:` Queue: none
- `Observed:` Rate-limit state: in-memory Flask-Limiter storage only

## Scheduled / Background Processes

- `Observed:` No background worker, queue consumer, or scheduler is defined in the repo.
- `Observed:` The only maintenance jobs are manual Flask CLI commands:
  - `db-upgrade`
  - `bootstrap-admin`
  - `fx-refresh`

## External Calls / Services

- `Observed:` FX lookup: `services/fx_rates.py` -> Frankfurter/ECB HTTP API
- `Observed:` Frontend CDNs in `templates/base.html`
  - Google Fonts
  - jsDelivr Bootstrap Icons
  - jsDelivr Chart.js

## Evidence

- `wsgi.py`
- `peqa/app_factory.py:68-91`
- `peqa/services/context.py`
- `peqa/services/filtering.py`
- `legacy_app.py:2238-2389`
- `services/deal_parser.py:1350-1696`
- `services/benchmark_parser.py:65-198`
- `services/metrics/chart_builder.py:1257-1335`
- `legacy_app.py:4173-4399`
- `services/fx_rates.py`

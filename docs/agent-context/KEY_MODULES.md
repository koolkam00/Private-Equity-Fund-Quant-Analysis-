# KEY_MODULES

## 1. App Bootstrap And Routing

### Purpose

- `Observed:` Build the Flask app, initialize extensions, and attach all routes.

### Major Files

- `wsgi.py`
- `app.py`
- `peqa/app_factory.py`
- `peqa/extensions.py`
- `peqa/route_binding.py`
- `legacy_app.py`

### Key Classes / Functions

- `peqa/app_factory.py::create_app`
- `peqa/route_binding.py::AppBinder`
- `legacy_app.py::app` (binder instance)
- `legacy_app.py::ROUTE_BLUEPRINTS`

### Inputs / Outputs

- `Input:` environment variables, Flask config, blueprints, bound route functions
- `Output:` a configured `Flask` app

### Side Effects

- Initializes DB/login/CSRF/migrations/rate limiter
- Creates upload/instance directories
- Registers before/after request logging
- Registers routes and CLI commands

### Dependencies

- `config.py`
- `models.py`
- `legacy_app.py`

### Why It Matters

- `Observed:` All runtime behavior depends on this layer; the app does not work without the legacy binder registration.

### Coupling / Fragility

- `Observed:` `AppBinder.register` registers routes both on the app and on blueprints, creating duplicate URL rules/endpoints.
- `Inferred:` Refactoring routes without understanding this dual registration can break endpoint names or auth redirects.

## 2. Data Model And Schema

### Purpose

- `Observed:` Define persistence for deals, supplemental analytics tables, auth/team state, benchmarks, and chart-builder templates.

### Major Files

- `models.py`
- `migrations/env.py`
- `migrations/versions/4a62775748c8_baseline_schema.py`

### Key Classes / Functions

- Models:
  - `Deal`
  - `DealCashflowEvent`
  - `DealQuarterSnapshot`
  - `FundQuarterSnapshot`
  - `FundMetadata`
  - `FundCashflow`
  - `PublicMarketIndexLevel`
  - `DealUnderwriteBaseline`
  - `UploadIssue`
  - `BenchmarkPoint`
  - `ChartBuilderTemplate`
  - `User`, `Team`, `TeamMembership`, `TeamFirmAccess`, `TeamInvite`
- Schema helper:
  - `models.py::ensure_schema_updates`

### Inputs / Outputs

- `Input:` ORM writes from uploads, auth actions, template saves
- `Output:` normalized relational tables used by every page

### Side Effects

- `Observed:` `ensure_schema_updates` can perform additive DDL and index creation against live DBs.

### Dependencies

- SQLAlchemy
- Flask-Migrate/Alembic

### Why It Matters

- `Observed:` The distinction between team-scoped, firm-scoped, and globally named entities is a central correctness concern.

### Coupling / Fragility

- `Observed:` Many delete/replace helpers filter by `firm_id` but not always by `team_id`.

## 3. Scope And Context Services

### Purpose

- `Observed:` Resolve active analysis scope and derive filter options, benchmark context, and precomputed deal metrics.

### Major Files

- `peqa/services/context.py`
- `peqa/services/filtering.py`

### Key Classes / Functions

- `AnalysisContext`
- `build_analysis_context`
- `build_deal_scope_query`
- `apply_deal_filters`
- `build_filter_options`
- `build_fund_vintage_lookup`

### Inputs / Outputs

- `Input:` current membership, active firm, request values, session
- `Output:` scoped deals, filter options, benchmark asset-class selection, `metrics_by_id`

### Side Effects

- `Observed:` Reads and updates session benchmark selection.

### Dependencies

- `models.py`
- `services/metrics/deal.py`

### Why It Matters

- `Observed:` Most pages do not query deals directly; they trust this layer.

### Coupling / Fragility

- `Observed:` Deal scope currently allows rows where `Deal.team_id` is `NULL` or matches the active team.

## 4. Ingestion Layer

### Purpose

- `Observed:` Parse uploaded Excel workbooks, validate rows, create firms if needed, write deals and supplemental tables, and log upload issues.

### Major Files

- `services/deal_parser.py`
- `services/benchmark_parser.py`
- `services/fx_rates.py`
- `services/utils.py`

### Key Functions

- `parse_deals`
- `_parse_optional_sheets`
- `_replace_existing_fund_data`
- `_refresh_firm_fx_metadata`
- `parse_benchmarks`
- `resolve_rate_to_usd`

### Inputs / Outputs

- `Input:` workbook file path, `team_id`, optional uploader ID
- `Output:` counts, warnings, `batch_id`, issue report ID, per-firm metadata

### Side Effects

- Inserts and deletes DB rows
- Creates or updates `Firm`
- Writes `UploadIssue`
- Calls external FX API

### Dependencies

- `models.py`
- pandas/openpyxl
- `services/fx_rates.py`

### Why It Matters

- `Observed:` Upload semantics determine whether downstream pages have enough data to work.

### Coupling / Fragility

- `Observed:` Replacement and deletion semantics are implemented here, not in a separate service layer.
- `Observed:` Optional-sheet parsing depends on matching newly inserted deals by company/fund.

## 5. Core Metrics Engine

### Purpose

- `Observed:` Compute deal-level returns and bridge math, then aggregate up to portfolio and track-record outputs.

### Major Files

- `services/metrics/common.py`
- `services/metrics/bridge.py`
- `services/metrics/deal.py`
- `services/metrics/portfolio.py`
- `services/metrics/risk.py`
- `services/metrics/quality.py`

### Key Functions

- `compute_deal_metrics`
- `compute_bridge_view`
- `compute_portfolio_analytics`
- `compute_bridge_aggregate`
- `compute_deal_track_record`
- `compute_deals_rollup_details`
- `compute_loss_and_distribution`
- `compute_data_quality`

### Inputs / Outputs

- `Input:` `Deal` rows and sometimes precomputed `metrics_by_id`
- `Output:` dictionaries consumed directly by templates, APIs, PDFs, and chart builder

### Side Effects

- `Observed:` None outside Python memory

### Dependencies

- `services/metrics/common.py`
- `peqa/services/filtering.py`

### Why It Matters

- `Observed:` This is the base layer reused almost everywhere.

### Coupling / Fragility

- `Observed:` Many higher-level modules rebuild `metrics_by_id` independently, so metric shape changes ripple widely.

## 6. Extended Analysis Modules

### Purpose

- `Observed:` Compute page-specific payloads beyond the core dashboard.

### Major Files

- `services/metrics/analysis.py`
- `services/metrics/benchmarking.py`
- `services/metrics/lp.py`
- `services/metrics/vca_ebitda.py`
- `services/metrics/vca_revenue.py`
- `services/metrics/ic_memo.py`
- `services/metrics/methodology.py`

### Key Functions

- `compute_fund_liquidity_analysis`
- `compute_underwrite_outcome_analysis`
- `compute_valuation_quality_analysis`
- `compute_exit_readiness_analysis`
- `compute_stress_lab_analysis`
- `compute_deal_trajectory_analysis`
- `compute_benchmarking_analysis`
- `compute_lp_liquidity_quality_analysis`
- `compute_manager_consistency_analysis`
- `compute_public_market_comparison_analysis`
- `compute_reporting_quality_analysis`
- `compute_nav_at_risk_analysis`
- `compute_benchmark_confidence_analysis`
- `compute_liquidity_forecast_analysis`
- `compute_fee_drag_analysis`
- `compute_lp_due_diligence_memo`
- `compute_ic_memo_payload`
- `build_methodology_payload`

### Inputs / Outputs

- `Input:` scoped deals plus supplemental tables queried inside the functions
- `Output:` page payload dicts rendered by `analysis_page.html` variants or returned via `/api/analysis/.../series`

### Side Effects

- `Observed:` Many functions hit the database for supplemental rows.

### Dependencies

- Core metrics engine
- `models.py`
- `peqa/services/filtering.py`

### Why It Matters

- `Observed:` These modules define the app's differentiated product surface.

### Coupling / Fragility

- `Observed:` `services/metrics/lp.py` has dense cross-table logic and repeated calls into other LP-analysis functions.
- `Observed:` Several modules reimplement status normalization instead of centralizing it.

## 7. Chart Builder

### Purpose

- `Observed:` Provide an internal ad hoc analytics/query builder across deals, quarterly tables, cashflows, underwrite, and benchmarks.

### Major Files

- `services/metrics/chart_builder.py`
- `legacy_app.py` chart-builder routes
- `models.py::ChartBuilderTemplate`

### Key Functions

- `build_chart_field_catalog`
- `run_chart_query`
- `_rows_for_source`
- `_aggregate`

### Inputs / Outputs

- `Input:` JSON chart spec, active team/firm, global filters
- `Output:` chart/table payloads and saved team templates

### Side Effects

- Template CRUD writes `chart_builder_templates`

### Dependencies

- `models.py`
- core metrics
- filter/context helpers

### Why It Matters

- `Observed:` This is effectively a second query layer, separate from the fixed analysis pages.

### Coupling / Fragility

- `Observed:` Row loading and aggregation are Python-side, not DB-side, so scale depends on dataset size.

## 8. Frontend Layer

### Purpose

- `Observed:` Render server-side pages and execute client-side chart/detail interactions.

### Major Files

- `templates/base.html`
- `templates/*.html`
- `static/js/app.js`
- `static/css/style.css`

### Key Functions / Behaviors

- Firm picker modal
- density toggle
- bridge waterfall rendering
- Chart.js dashboard rendering
- chart-builder client interactions

### Inputs / Outputs

- `Input:` server-rendered JSON/script payloads and page data attributes
- `Output:` interactive charts, tables, print views

### Side Effects

- Browser localStorage for density mode
- network calls to JSON endpoints

### Dependencies

- Chart.js CDN
- Bootstrap Icons CDN
- Google Fonts CDN

### Why It Matters

- `Observed:` Many "bugs" users see are in template wiring or `static/js/app.js`, not in metric math.

### Coupling / Fragility

- `Observed:` The frontend assumes specific payload shapes from `legacy_app.py` and metric modules; payload drift can break pages without type safety.

## Evidence

- `peqa/app_factory.py:68-91`
- `peqa/route_binding.py:52-74`
- `models.py`
- `services/deal_parser.py`
- `services/metrics/*.py`
- `legacy_app.py`
- `templates/base.html`
- `static/js/app.js`

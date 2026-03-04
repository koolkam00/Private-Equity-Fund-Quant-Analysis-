# Deal-Only PE Portfolio Analytics: Architecture and Calculations

## 1. System Architecture
- Backend: Flask (`app.py`) + SQLAlchemy (`models.py`)
- Ingestion: Deal-level Excel parser (`services/deal_parser.py`)
- Metrics Engine: Deal-only analytics modules (`services/metrics/*`)
- Frontend: Jinja + Chart.js (`templates/*.html`, `static/js/app.js`)
- Storage: SQLite (`instance/deals.db`)

## 2. Data Flow
1. User uploads a deal-level workbook at `/upload`.
2. Parser maps headers, coerces types, validates rows.
3. Invalid rows are quarantined in `upload_issues` with `issue_report_id`.
4. Valid rows are stored in `deals`.
5. Dashboard and deal routes compute analytics from deals only.
6. Chart payload endpoints:
- `GET /api/dashboard/series`
- `GET /api/deals/<id>/bridge`

## 3. Deal Data Model
Primary fields used in analytics:
- Qualitative: company, fund, sector, geography, status, year invested, dates
- Investment values: equity invested, realized value, unrealized value
- Entry/Exit financials: revenue, EBITDA, TEV, net debt
- Optional ownership override: `ownership_pct`
- Firm metadata: `firms.base_currency` (ISO-3, default `USD`)

Currency policy:
- No FX conversion is applied in analytics.
- All monetary values are interpreted and displayed in the active firm's base currency.
- Dashboard/deals/track-record/UI defaults use `CODE + symbol` (for example, `USD $123.4M`); `/analysis/*` pages use symbol-only display (for example, `$123.4M`).

## 4. Calculation Framework

### 4.1 Core Returns
- Gross MOIC = `(realized_value + unrealized_value) / equity_invested`
- Value Created = `realized_value + unrealized_value - equity_invested`
- Implied IRR = `moic^(1/hold_years) - 1` when valid

### 4.2 Loss Ratios
- Count-based loss ratio: `% deals with MOIC < 1.0x`
- Capital value loss ratio: `% of total invested equity lost on impaired deals (MOIC < 1.0x), where loss per impaired deal = max(equity invested - total value, 0)`

### 4.3 Entry/Exit Metrics
For both entry and exit:
- TEV / EBITDA
- TEV / Revenue
- Net Debt / EBITDA
- Net Debt / TEV
- EBITDA Margin

Growth metrics:
- Revenue Growth, EBITDA Growth
- Revenue CAGR, EBITDA CAGR

All portfolio statistics include both:
- Simple averages
- Equity-weighted averages

Signed-ratio policy:
- Negative `TEV / EBITDA` is treated as unavailable (`None`).
- `TEV / Revenue`, `Net Debt / EBITDA`, and `Net Debt / TEV` remain signed when computable.

### 4.4 Value Creation Bridge (Additive)
Definitions:
- `m0 = EBITDA0 / Revenue0`, `m1 = EBITDA1 / Revenue1`
- `x0 = TEV0 / EBITDA0`, `x1 = TEV1 / EBITDA1`

Company-level additive drivers:
- Revenue: `(Revenue1 - Revenue0) * m0 * x0`
- Margin: `Revenue1 * (m1 - m0) * x0`
- Multiple: `(x1 - x0) * EBITDA1`
- Leverage: `NetDebt0 - NetDebt1`

Ownership scaling:
- Use explicit `ownership_pct` when provided
- Else derive from `equity_invested / (Entry TEV - Entry Net Debt)` when valid
- Else fallback to 100% with warning

Fund residual closure:
- `Other = ValueCreated_fund - (Revenue + Margin + Multiple + Leverage)`

Negative-EBITDA fallback bridge:
- Trigger: `Entry EBITDA <= 0` or `Exit EBITDA <= 0`
- Definitions: `rm0 = TEV0 / Revenue0`, `rm1 = TEV1 / Revenue1`
- Drivers (company basis):
  - Revenue: `(Revenue1 - Revenue0) * rm0`
  - Margin: `0`
  - Multiple: `(rm1 - rm0) * Revenue1`
  - Leverage: `NetDebt0 - NetDebt1`
- Residual still closes to observed value created, and ownership scaling remains unchanged.

Missing-revenue fallback bridge:
- Trigger: both `Entry Revenue` and `Exit Revenue` are missing/near-zero
- Definitions: `x0 = TEV0 / EBITDA0`, `x1 = TEV1 / EBITDA1`
- Drivers (company basis):
  - EBITDA Growth: `(EBITDA1 - EBITDA0) * x0`
  - Multiple: `(x1 - x0) * EBITDA1`
  - Leverage: `NetDebt0 - NetDebt1`
- Display policy:
  - Margin row is hidden for this fallback.
  - Legacy payload compatibility keeps `revenue` alias equal to EBITDA Growth and `margin = 0`.

### 4.5 Bridge Views
All bridge drivers are available in:
- Dollar ($M)
- MOIC contribution (`driver_$ / equity_invested`)
- Percent of value created (`driver_$ / value_created`)

## 5. Filters
Global filters drive every KPI, table, and chart:
- Fund
- Status
- Sector
- Geography
- Vintage year
- Exit type
- Lead partner
- Security type
- Deal type
- Entry channel

### 5.1 Team-Scoped Benchmark Selector
- Dashboard includes a manual `Benchmark Asset Class` selector.
- Benchmarks are team-scoped and loaded from `benchmark_points`.
- Matching policy is strict exact match on:
  - `asset_class`
  - `vintage_year`
  - `metric` (`net_irr`, `net_moic`, `net_dpi`)
- No interpolation or nearest-year fallback is used.

Benchmark ranking algorithm (higher-is-better):
1. `value >= top_5` -> `Top 5%`
2. else if `value >= upper_quartile` -> `1st Quartile`
3. else if `value >= median` -> `2nd Quartile`
4. else if `value >= lower_quartile` -> `3rd Quartile`
5. else -> `4th Quartile`

If required thresholds are missing, or if fund vintage/metric is unavailable, dashboard shows `N/A`.

### 5.2 Benchmarking Analysis (IC PDF)
- Routes:
  - `GET /analysis/benchmarking`
  - `GET /api/analysis/benchmarking/series`
- Scope:
  - Uses the selected `benchmark_asset_class` only.
  - Applies exact vintage-year matching to benchmark thresholds.
  - Produces fund-level benchmarking rows for `net_irr`, `net_moic`, and `net_dpi`.
- Ranking:
  - `Top 5%` when value `>= top_5` (when present)
  - else quartiles via `upper_quartile`, `median`, `lower_quartile`
  - else `N/A` when metric/vintage/thresholds are missing
- Composite score:
  - Rank-score mapping: `Top5=5`, `Q1=4`, `Q2=3`, `Q3=2`, `Q4=1`, `N/A=0`
  - Composite score = average of available (non-`N/A`) metric scores
  - Composite rank bands:
    - `>=4.75` -> `Top 5%`
    - `>=3.75` -> `1st Quartile`
    - `>=2.75` -> `2nd Quartile`
    - `>=1.75` -> `3rd Quartile`
    - otherwise `4th Quartile`
- Print/PDF behavior:
  - Browser-native print (`window.print()`), not server-side PDF generation.
  - Print profile: `Letter landscape`.
  - Report structure: fund benchmarking table (page 1) + executive summary + threshold appendix.
  - Includes print metadata (as-of date, unit label, filters, benchmark asset class).
  - Fund benchmarking table format matches the dashboard `Fund Summary` benchmark columns.

### 5.3 IC Analysis PDF Pack Export
- Route:
  - `GET /reports/ic-pdf-pack`
  - `GET /reports/ic-pdf-pack/live`
- Output:
  - Returns a ZIP archive containing 4 separate PDFs:
    - Deal Level Track Record
    - Value Creation Analysis by EBITDA
    - Value Creation Analysis by Revenue
    - Benchmarking Analysis
- Filename/title convention:
  - `{Firm Name} {Analysis Name} As Of {YYYY-MM-DD}.pdf`
  - ZIP filename: `{Firm Name} Analysis PDF Pack As Of {YYYY-MM-DD}.zip`
- Scope behavior:
  - Export always uses all visible data for the active firm (full-portfolio scope).
  - Route intentionally ignores page-level filter parameters for fund/status/sector/geography/vintage/exit type.
  - Benchmarking PDF uses the current selected benchmark asset class from session/query context.
- Rendering engine:
  - Server-side ReportLab generation (not browser print CSS) for deterministic one-click multi-file download.
- Live-layout export helper:
  - `/reports/ic-pdf-pack/live` opens the same existing page-level print/download flows used by:
    - `/track-record/pdf`
    - `/analysis/vca-ebitda` print layout
    - `/analysis/vca-revenue` print layout
    - `/analysis/benchmarking` print layout
  - This preserves the exact browser print CSS layout for those analysis tabs.

## 6. Migration and Compatibility
- Additive schema updates for `geography`, `year_invested`, `ownership_pct`
- Legacy templates supported with fallbacks:
  - Missing geography -> `Unknown`
  - Missing year invested -> derived from investment date
- If a legacy time-series table exists from earlier versions, it is archived and ignored by runtime analytics

## 7. IC Memo Presentation
- Routes:
  - `GET /ic-memo`
  - `GET /ic-memo/<fund_name>`
- Route behavior:
  - Supports global filter semantics from dashboard/query parameters.
  - Path `fund_name` is authoritative when present; other query filters still apply.
- Payload contract:
  - Built by `compute_ic_memo_payload(...)` in `services/metrics/ic_memo.py`.
  - Contains `meta`, `executive`, `bridge`, `risk`, `operating`, `slicing`, and `team` sections.

### 7.1 Decile Ranking Method
- Dimension groups are ranked by `weighted_moic` (default).
- Group metrics include:
  - `deal_count`
  - `invested_equity`
  - `total_value`
  - `value_created`
  - `weighted_moic`
  - `weighted_implied_irr`
- Deciles use strict sizing:
  - `n = max(1, ceil(group_count * 0.10))`
  - `top_decile = top n groups`
  - `bottom_decile = bottom n groups`

### 7.2 PDF / Print Behavior
- Export mechanism is browser-native print-to-PDF (`window.print()`), not server-side PDF generation.
- IC memo print defaults:
  - US Letter landscape layout.
  - Multi-page sections with explicit page breaks.
  - Non-report UI elements are hidden in print.
  - Charts and tables use print-safe sizing and color-adjust settings.

## 8. Methodology & Audit Page
- Routes:
  - `GET /methodology` (canonical)
  - `GET /audit` (alias redirect to `/methodology`)
- Payload contract:
  - Built by `build_methodology_payload()` in `services/metrics/methodology.py`.
  - Includes `meta`, `sections`, `glossary`, and `rules`.
  - Sections contain metric-level fields:
    - `id`, `name`, `formula`, `code_formula`, `variables`, `interpretation`, `edge_cases`, `units`, `source_refs`.
- Scope:
  - Mirrors backend formula and rule behavior across:
    - shared numeric/date helpers
    - deal metrics
    - portfolio aggregation
    - bridge attribution
    - loss/risk analytics
    - track record logic
    - analysis modules
    - IC memo grouping/deciles
- Maintenance contract:
  - Any formula or threshold change in `services/metrics/*` should be reflected in `services/metrics/methodology.py` and covered by tests.
- Print/PDF behavior:
  - Browser-native print (`window.print()`), with page classes and print CSS tuned for audit readability.

## 9. Tenancy, Authentication, and Scope
- Access model: invite-only accounts (email/password).
- Team and Firm are separate domains:
  - Team routes are for collaboration/admin membership.
  - Analytics data scope is controlled by active firm selection.
- Firm visibility is team-scoped via `team_firm_access`.
- Join model:
  - `team_firm_access(team_id, firm_id, created_by_user_id, created_at)`
- Upload behavior:
  - Uploading a new firm auto-grants access to the uploader’s team only.
- Analytics-scoped entities (all include `firm_id`):
  - `deals`
  - `deal_cashflow_events`
  - `deal_quarter_snapshots`
  - `fund_quarter_snapshots`
  - `deal_underwrite_baselines`
  - `upload_issues`
- Team collaboration routes:
  - `GET /team`
  - `POST /team/invites`
  - `GET/POST /auth/accept-invite/<token>`
- Firm management/scope routes:
  - `GET /firms`
  - `POST /firms/<id>/select`

### 9.1 Global Fund Scope Precedence
Fund selection precedence is deterministic:
1. path fund override (e.g. `/ic-memo/<fund_name>`)
2. query `fund` parameter
3. all funds in active firm

Applied consistently through `_build_filtered_deals_context(...)`.

Active firm precedence:
1. session `active_firm_id`
2. first accessible firm with data for current team (fallback)

### 9.2 Upload Replacement Rule
- Upload parser default mode is `replace_fund`.
- Deals sheet requires `Firm Name` and workbook must contain exactly one distinct firm.
- Deals sheet requires `As Of Date` and workbook must contain exactly one distinct `As Of Date` across all non-empty rows.
- Deals sheet optional `Firm Currency` must be a valid ISO-3 code when present.
- If `Firm Currency` is missing/blank, firm currency defaults to `USD`.
- Each workbook must resolve to exactly one currency value after normalization.
- Unknown firm names are auto-created.
- Existing firm currency is updated from the uploaded workbook when a different valid `Firm Currency` is provided.
- For each fund found in uploaded Deals sheet:
  - delete prior firm-scoped deals for that fund
  - delete dependent supplemental rows for those deals
  - delete firm-scoped fund-quarter snapshots for that fund
  - insert newly uploaded rows atomically in one transaction
- `upload_batch` and `upload_issues` history are preserved for auditability.

### 9.3 As Of Date Display Metadata
- `deals.as_of_date` is an additive nullable date field populated by new uploads.
- Display scope:
  - Dashboard metadata
  - Analysis pages (`/analysis/*`) metadata
- Resolution precedence for displayed as-of date in filtered views:
  1. max visible `deals.as_of_date`
  2. max visible `deals.exit_date`
  3. max visible `deals.investment_date`
  4. system date (`today`)
- Mixed upload snapshots in one filtered view are represented by showing the latest visible as-of date.
- This is display-only metadata; time-based metric calculations are unchanged.

### 9.4 Benchmark Upload Contract
- Benchmark ingestion route: `POST /upload/benchmarks`
- Optional benchmark template route: `GET /upload/benchmarks/template`
- Required benchmark columns:
  - `Asset Class`
  - `Vintage Year`
  - `Metric`
  - `Quartile`
  - `Value`
- Metric aliases:
  - `Net IRR` -> `net_irr`
  - `Net MOIC` / `MOIC` / `Net TVPI` / `TVPI` -> `net_moic`
  - `Net DPI` / `DPI` -> `net_dpi`
- Quartile aliases:
  - `Lower Quartile` -> `lower_quartile`
  - `Median` -> `median`
  - `Upper Quartile` -> `upper_quartile`
  - `Top 5%` -> `top_5`
- Upload mode is full replace per team:
  - existing `benchmark_points` rows for that team are deleted before insert.

### 9.5 Firm Currency USD Reporting Conversion
- Each firm stores native uploaded currency in `firms.base_currency`.
- Additional firm FX metadata is stored in:
  - `firms.fx_rate_to_usd`
  - `firms.fx_rate_date`
  - `firms.fx_rate_source`
  - `firms.fx_last_status`
- Conversion policy:
  - If native currency is `USD`, values remain USD and `fx_rate_to_usd=1.0`.
  - If native currency is non-USD and upload-date FX lookup succeeds, analytics are reported in USD using firm-level `fx_rate_to_usd`.
  - If FX lookup fails, upload still succeeds and analytics remain in native currency.
- FX timestamp policy:
  - FX is refreshed on each upload using upload date.
  - Effective date returned by provider is stored and shown in disclosure.
- Display policy:
  - Dashboard shows conversion disclosure when active:
    - source currency, USD rate, effective date, source.
  - If conversion is unavailable, dashboard shows native-currency warning.
- Math policy:
  - Only monetary values are scaled by FX factor.
  - Ratios/multiples/percentages (MOIC, IRR, CAGR, margins, leverage ratios) are not scaled.

## 10. Deployment Runbook (Render + Postgres)
1. Provision with `render.yaml`:
  - Web service + managed Postgres
2. Required env vars:
  - `SECRET_KEY`
  - `BOOTSTRAP_ADMIN_EMAIL`
  - `BOOTSTRAP_ADMIN_PASSWORD`
  - `FLASK_ENV=production`
3. Startup behavior:
  - schema bootstrap via `db.create_all()` + `ensure_schema_updates()`
  - identity bootstrap creates initial admin user/team when no users exist
  - legacy rows without `team_id` are backfilled to seeded Admin Team (compatibility)
  - legacy rows without `firm_id` are backfilled via team->firm mapping (fallback `Admin Firm`)
4. First-run steps:
  - sign in with bootstrap admin
  - open `/firms`, confirm active firm selection
  - open `/team`, generate invite link(s) for collaborators
  - upload first fund workbook at `/upload`
  - switch active firm using global header selector
5. Operations:
  - use managed Postgres snapshots/backups from Render dashboard
  - use `/healthz` for uptime checks

## 11. Chart Builder (Analysis Tab)
- Route:
  - `GET /analysis/chart-builder`
- API endpoints:
  - `GET /api/chart-builder/catalog`
  - `POST /api/chart-builder/query`
  - `GET /api/chart-builder/templates`
  - `POST /api/chart-builder/templates`
  - `PUT /api/chart-builder/templates/<id>`
  - `DELETE /api/chart-builder/templates/<id>`

### 11.1 Data Scope and Sources
- Team and firm enforcement:
  - firm-scoped sources (`deals`, `deal_quarterly`, `fund_quarterly`, `cashflows`, `underwrite`) execute in active firm context.
  - benchmarks remain team-scoped.
- Supported sources (single source per chart):
  - `deals`
  - `deal_quarterly`
  - `fund_quarterly`
  - `cashflows`
  - `underwrite`
  - `benchmarks`

### 11.2 Query Spec and Auto Chart Rules
- Query spec includes:
  - `source`, `chart_type`, `x`, `y[]`, optional `series`, optional `size`, `filters[]`, `sort`, `limit`.
- Auto chart resolution:
  1. manual chart type is always honored when not `auto`.
  2. date/year x + numeric y -> `line`.
  3. numeric x + numeric y -> `scatter`, or `bubble` when `size` is set.
  4. categorical x + y -> `bar`.
  5. categorical x + single y + low cardinality (`<=8`) + no series -> `donut`.
  6. fallback -> `bar`.

### 11.3 Aggregation and Guardrails
- Supported aggregations:
  - `count`, `count_distinct`, `sum`, `avg`, `wavg`, `min`, `max`
- Weighted average defaults:
  - `deals`, `deal_quarterly`, `underwrite`: `equity_invested`
  - `fund_quarterly`: `paid_in_capital`
  - disabled for `cashflows` and `benchmarks`
- Guardrails:
  - grouped category default cap: `200`
  - scatter/bubble point default cap: `1500`
  - table row hard cap: `5000`
  - truncation sets `meta.truncated=true` with warning text.

### 11.4 Template Persistence
- Team-scoped template model:
  - `chart_builder_templates(team_id, name, source, config_json, created_by_user_id, timestamps)`
- `config_json` stores configuration only (`config_version=1`), no data snapshots.
- Templates run live against current active firm and current global filters.

## 12. Value Creation Analysis - by EBITDA (PDF-Style Tab)
- Route:
  - `GET /analysis/vca-ebitda`
- API:
  - `GET /api/analysis/vca-ebitda/series`
- Scope:
  - Uses the same active-firm + page filter context as other analysis pages.
- Table shape:
  - 40 fixed columns with grouped multi-row headers and formula legend row.
  - Fund sections include deal rows, subtotal rows, and deterministic summary rows:
    - `Average`
    - `Median`
    - `Weighted Average` (equity-weighted where denominator is valid)
  - Overall block includes:
    - realized-only, partial-only, realized+partial, unrealized-only, and grand total rows.
- Core mapping rules:
  - `Fund Total Cost = equity`
  - `Total Value = realized + unrealized`
  - `Gross Profit = Total Value - Fund Total Cost`
  - `Gross IRR = uploaded deal IRR`
  - `Gross Profit % of Total` uses fund total gross profit for fund sections and grand total gross profit for overall rows.
  - `EBITDA Growth ($)` in this table maps to `bridge.revenue + bridge.margin`.
  - `Value Creation %` columns are each driver `$` divided by row gross profit.
- Entry/exit block:
  - Uses weighted entry/exit operating aggregates and reports delta as `exit - entry`.
- Currency:
  - Monetary fields are run through the standard reporting-currency scaling pipeline.
- Print/PDF:
  - Browser-native print (`window.print()`), with Legal landscape print CSS and compact non-wrapping rows.
  - Print layout renders a single full-width 40-column table per fund block; fund blocks can share pages when they fit, and oversize blocks may continue naturally across pages.
  - Print controls support fund-block ordering (`Fund Name`, `Gross Profit`, `Gross MOIC`, `Gross IRR`, `Status`), density (`Readable`/`Compact`), and mode (`Detailed`/`Executive PDF`).
  - A compact legend strip is rendered in the print table header and repeats on each printed page via `thead` repetition.
  - Footer labels use section indexing (`Fund X of N`) per fund block and an explicit final overall-block footer label; each fund’s net-performance summary is printed at the bottom of that fund block.
  - Executive mode prepends a net-performance summary section (per fund `Net IRR`, `Net MOIC`, `Net DPI` plus overall gross summary), then keeps full detail tables as an appendix.
  - Fund-level net conflicts are rendered as `N/A` for the conflicting metric(s).
  - Overall portfolio block prints on a dedicated final page.
  - Formula legend row remains on-screen but is suppressed in print for density/readability.
  - Subtotal/summary/overall rows use stronger contrast and accent borders than deal rows; negative delta/value-creation cells are highlighted while positive values use the default text color.
  - Analysis pages render symbol-only money display (`$`, `€`, etc.) with currency code suppressed in table values and unit labels.

## 13. Value Creation Analysis - by Revenue (PDF-Style Tab)
- Route:
  - `GET /analysis/vca-revenue`
- API:
  - `GET /api/analysis/vca-revenue/series`
- Scope:
  - Uses the same active-firm + page filter context as other analysis pages.
- Table shape:
  - 40 fixed columns with grouped multi-row headers and formula legend row.
  - Fund and overall block row ordering matches the EBITDA VCA tab.
- Core mapping rules:
  - `Fund Total Cost = equity`
  - `Total Value = realized + unrealized`
  - `Gross Profit = Total Value - Fund Total Cost`
  - `Gross IRR = uploaded deal IRR`
  - `Revenue Growth ($) = bridge.revenue` (growth lever is revenue-only).
  - `Debt ($) = bridge.leverage`.
  - `Total ($) = Gross Profit`.
  - `Multiple ($) = Total - Revenue Growth - Debt` (residualized, so margin/other effects are captured and decomposition fully reconciles).
  - `Value Creation %` columns are each driver `$` divided by row gross profit.
- Entry/exit block:
  - Uses revenue/TEV/EV-to-revenue/net-debt-to-revenue/net-debt-to-EV weighted aggregates at entry and exit, with delta as `exit - entry`.
- Currency:
  - Monetary fields are run through the standard reporting-currency scaling pipeline.
- Print/PDF:
  - Shares the same print DOM/CSS/JS behavior as the EBITDA VCA tab, including legal-landscape formatting, fund packing, executive summary mode, and black major-section separators.

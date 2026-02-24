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

## 4. Calculation Framework

### 4.1 Core Returns
- Gross MOIC = `(realized_value + unrealized_value) / equity_invested`
- Value Created = `realized_value + unrealized_value - equity_invested`
- Implied IRR = `moic^(1/hold_years) - 1` when valid

### 4.2 Loss Ratios
- Count-based loss ratio: `% deals with MOIC < 1.0x`
- Capital-based loss ratio: `% invested equity in deals with MOIC < 1.0x`

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
- Access model: invite-only, team-scoped workspaces.
- Authentication: email/password login.
- Team isolation: all core tables are scoped by `team_id`.
- Team-scoped entities:
  - `deals`
  - `deal_cashflow_events`
  - `deal_quarter_snapshots`
  - `fund_quarter_snapshots`
  - `deal_underwrite_baselines`
  - `upload_issues`
- Team management routes:
  - `GET /team`
  - `POST /team/invites`
  - `GET/POST /auth/accept-invite/<token>`

### 9.1 Global Fund Scope Precedence
Fund selection precedence is deterministic:
1. path fund override (e.g. `/ic-memo/<fund_name>`)
2. query `fund` parameter
3. session `active_fund` selected from global selector
4. all funds in current team

Applied consistently through `_build_filtered_deals_context(...)`.

### 9.2 Upload Replacement Rule
- Upload parser default mode is `replace_fund`.
- For each fund found in uploaded Deals sheet:
  - delete prior team-scoped deals for that fund
  - delete dependent supplemental rows for those deals
  - delete team-scoped fund-quarter snapshots for that fund
  - insert newly uploaded rows atomically in one transaction
- `upload_batch` and `upload_issues` history are preserved for auditability.

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
  - legacy rows without `team_id` are backfilled to seeded Admin Team
4. First-run steps:
  - sign in with bootstrap admin
  - open `/team`, generate invite link(s)
  - upload first fund workbook at `/upload`
  - switch active fund using global header selector
5. Operations:
  - use managed Postgres snapshots/backups from Render dashboard
  - use `/healthz` for uptime checks

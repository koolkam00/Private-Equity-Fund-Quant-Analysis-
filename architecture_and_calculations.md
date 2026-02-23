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

### 4.5 Value Creation Bridge (Multiplicative)
When values are positive, use log decomposition:
- `ln(TEV1/TEV0) = ln(R1/R0) + ln(m1/m0) + ln(x1/x0)`

Allocate EV change by factor shares, then add leverage and residual to reconcile exactly.
Low-confidence flag is raised when log decomposition is numerically invalid.

### 4.6 Bridge Views
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

## 6. Migration and Compatibility
- Additive schema updates for `geography`, `year_invested`, `ownership_pct`
- Legacy templates supported with fallbacks:
  - Missing geography -> `Unknown`
  - Missing year invested -> derived from investment date
- If a legacy time-series table exists from earlier versions, it is archived and ignored by runtime analytics

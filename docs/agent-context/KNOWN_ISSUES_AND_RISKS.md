# KNOWN_ISSUES_AND_RISKS

## 1. Replace / Delete Paths Are Not Fully Team-Scoped

- `Severity:` High
- `Why It Matters:` If more than one team can access the same `Firm`, one team's upload replacement or upload-batch deletion can remove another team's rows. The same issue affects team-scoped public-market data during firm-scoped cleanup.
- `Observed Evidence:`
  - `services/deal_parser.py:1198 (_replace_existing_fund_data)` deletes `Deal`, `DealCashflowEvent`, `DealQuarterSnapshot`, `DealUnderwriteBaseline`, `FundQuarterSnapshot`, `FundMetadata`, and `FundCashflow` by `firm_id` and fund only.
  - `legacy_app.py:2995 (_delete_upload_batch_for_firm)` deletes most rows by `firm_id` and `upload_batch`, but deletes `PublicMarketIndexLevel` only by `upload_batch`.
  - `models.py` shows several affected tables carry both `team_id` and `firm_id`.
- `Recommended Follow-Up:`
  - Decide whether firms are truly shareable across teams.
  - If yes, add `team_id` filters to replace/delete flows where the table has `team_id`.
  - Split team-scoped public-market cleanup from firm-scoped upload-batch cleanup.

## 2. Upload FX Metadata Uses Current Date, Not Workbook As-Of Date

- `Severity:` High
- `Why It Matters:` The UI and launch checklist imply upload-date/as-of-date FX semantics, but upload processing currently records FX using today's date. That can make displayed reporting currency conversion inconsistent with the uploaded dataset date.
- `Observed Evidence:`
  - `services/deal_parser.py:1350 (parse_deals)` validates one `As Of Date` per firm.
  - `services/deal_parser.py:1582` calls `_refresh_firm_fx_metadata(firm, upload_date=date.today())`.
  - `PRE_LAUNCH_CHECKLIST.md:55-56` expects "upload-date FX".
- `Recommended Follow-Up:`
  - Decide the intended FX reference date.
  - If the intended date is workbook `As Of Date`, pass `context["as_of_date"]` instead of `date.today()`.
  - Add tests for FX date semantics.

## 3. Route Registration Is Duplicated

- `Severity:` Medium
- `Why It Matters:` The app serves the same URLs through both legacy endpoint names and blueprint-prefixed endpoint names. That increases cognitive load and makes endpoint naming/refactoring riskier.
- `Observed Evidence:`
  - `peqa/route_binding.py:52-74` adds each route to `flask_app` and also to a blueprint.
  - Runtime `app.url_map` inspection showed duplicate rules for `/dashboard`, `/analysis/<page>`, `/upload/deals`, `/track-record/pdf`, `/healthz`, and `/readyz`.
- `Recommended Follow-Up:`
  - Document whether this is temporary compatibility behavior.
  - If not intentional, simplify route registration to one path/endpoints strategy.

## 4. Production Rate Limits Use In-Memory Storage

- `Severity:` Medium
- `Why It Matters:` Limits reset on deploy/restart and are not shared across multiple processes or instances.
- `Observed Evidence:`
  - `peqa/extensions.py:19` sets `Limiter(... storage_uri="memory://")`.
- `Recommended Follow-Up:`
  - Move to a shared backend such as Redis if rate limiting must hold across workers/instances.

## 5. Existing Markdown Docs Are Partially Stale Or Internally Inconsistent

- `Severity:` Medium
- `Why It Matters:` Future agents or humans can be misled into debugging the wrong runtime path or assuming the wrong data model.
- `Observed Evidence:`
  - `methodology.md:5` says no time-series contribution/distribution data is used.
  - `architecture_and_calculations.md:15` says dashboard/deal routes compute analytics from deals only.
  - `architecture_and_calculations.md:29` says no FX conversion is applied in analytics.
  - Current code clearly uses `FundQuarterSnapshot`, `FundCashflow`, `PublicMarketIndexLevel`, and firm FX metadata in LP analyses and reporting.
- `Recommended Follow-Up:`
  - Treat `docs/agent-context/` as the preferred handoff set.
  - Either update or explicitly deprecate the older markdown docs.

## 6. Analytics And Chart Builder Are Python-Side, Request-Time Computations

- `Severity:` Medium
- `Why It Matters:` Large datasets will increase page/API latency because the app repeatedly loads rows and aggregates them in memory without caching.
- `Observed Evidence:`
  - `peqa/services/context.py::build_analysis_context` eagerly computes `metrics_by_id` for all scoped deals.
  - `legacy_app.py::_build_dashboard_payload` and `_analysis_route_payload` compute payloads per request.
  - `services/metrics/chart_builder.py::run_chart_query` loads source rows into Python and aggregates there.
- `Recommended Follow-Up:`
  - Profile large datasets.
  - Cache expensive payloads or push more aggregation to SQL if needed.

## 7. Status Normalization Logic Is Duplicated Across Modules

- `Severity:` Medium
- `Why It Matters:` A future rule change can easily be applied in one module and missed in others, producing inconsistent rollups.
- `Observed Evidence:`
  - Status normalization appears in:
    - `peqa/services/metrics/status.py`
    - `services/metrics/analysis.py`
    - `services/metrics/portfolio.py`
    - `services/metrics/vca_ebitda.py`
    - `services/metrics/vca_revenue.py`
    - `services/metrics/ic_memo.py`
- `Recommended Follow-Up:`
  - Consolidate callers on `peqa/services/metrics/status.py`.

## 8. Error Mapping For Chart-Builder Template Writes Is Overly Broad

- `Severity:` Low
- `Why It Matters:` A generic DB failure can be surfaced as "Template name already exists", which can slow debugging.
- `Observed Evidence:`
  - `legacy_app.py::chart_builder_template_create_api`
  - `legacy_app.py::chart_builder_template_update_api`
  - both catch broad `Exception` around `db.session.commit()`.
- `Recommended Follow-Up:`
  - Narrow exception handling to uniqueness/integrity errors and log unexpected DB failures separately.

## 9. Observability Is Basic

- `Severity:` Low to Medium
- `Why It Matters:` The app has health checks and request logs, but no evidence of centralized error tracking, metrics, tracing, or CI enforcement.
- `Observed Evidence:`
  - Request logging exists in `peqa/app_factory.py`.
  - Health/readiness routes exist in `legacy_app.py`.
  - No CI config or observability library/config was found in the repo.
- `Recommended Follow-Up:`
  - Add CI, structured error tracking, and minimal performance instrumentation if operating this beyond small internal usage.

## Evidence

- `services/deal_parser.py:1198-1696`
- `legacy_app.py:2995-3073`
- `peqa/extensions.py:19`
- `peqa/route_binding.py:52-74`
- `methodology.md`
- `architecture_and_calculations.md`
- `peqa/services/context.py`
- `services/metrics/chart_builder.py`

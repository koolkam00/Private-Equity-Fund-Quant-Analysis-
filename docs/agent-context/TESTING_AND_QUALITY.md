# TESTING_AND_QUALITY

## Test Frameworks

- `Observed:` Pytest is the only automated test framework present.
- `Observed:` Tests use Flask test clients, SQLite test databases, monkeypatching, and direct ORM setup.

## Test Structure

- `tests/conftest.py`
  - shared app/client fixtures
- `tests/test_parsers.py`
  - workbook ingestion, supplemental sheets, FX stubbing
- `tests/test_benchmarks.py`
  - benchmark upload parsing
- `tests/test_metrics.py`
  - core and LP analysis payloads
- `tests/test_routes.py`
  - dashboard/upload/report routes and some print/export behavior
- `tests/test_chart_builder.py`
  - chart-builder catalog and query logic
- `tests/test_security.py`
  - CSRF and rate limits
- `tests/test_cli.py`
  - migration/bootstrap/FX refresh CLI commands

## Commands

- Tests:

```bash
pytest -q
```

- `Observed:` No lint config was found.
- `Observed:` No type-checker config was found.
- `Observed:` No frontend build command exists; assets are static files plus CDN imports.

## Latest Observed Result

- `Observed:` `pytest -q` passed with:
  - `187 passed`
  - `2 warnings`
- `Observed:` Warnings came from third-party dependencies (`dateutil`, `reportlab`), not from repo code.

## What Is Tested Well

- `Observed:` Deal workbook parsing, including supplemental sheets and multi-firm behavior
- `Observed:` Benchmark upload parsing and normalization
- `Observed:` Core deal/portfolio metrics
- `Observed:` LP analysis payload builders in `services/metrics/lp.py`
- `Observed:` Route-level auth, health/readiness, upload, and export behavior
- `Observed:` CLI migration/bootstrap/FX refresh commands
- `Observed:` CSRF and request-rate limits

## What Is Tested Poorly

- `Observed:` Multi-team sharing edge cases where two teams can access the same `Firm`
- `Observed:` Delete/replace semantics for team-scoped tables during firm-scoped operations
- `Observed:` Frontend JavaScript behavior beyond basic HTML presence
- `Observed:` Performance/regression behavior on large datasets
- `Observed:` Duplicate route registration implications from `AppBinder.register`
- `Observed:` Existing markdown documentation accuracy is not tested and is currently stale

## Brittle / Flaky Areas

- `Observed:` PDF behavior depends on ReportLab and print-layout assumptions; coverage is mostly smoke-level.
- `Observed:` Tests use a shared imported app from `app.py`, which mirrors production routing but can hide some factory-isolation problems.

## Highest-Value Tests To Add Next

1. Multi-team same-firm isolation tests around `_replace_existing_fund_data` and `_delete_upload_batch_for_firm`.
2. Upload FX-date tests proving whether workbook `As Of Date` or current date should drive firm FX metadata.
3. Upload-batch deletion tests covering `PublicMarketIndexLevel` and multi-firm workbook side effects.
4. Route-map tests that assert whether duplicate route registration is intentional.
5. Larger-data performance smoke tests for chart-builder queries and LP analysis endpoints.

## Quality Notes

- `Observed:` There is no CI config in the repo root.
- `Observed:` There is no static typing layer protecting payload shapes between backend and `static/js/app.js`.
- `Observed:` Existing tests are the most reliable source of intended behavior.

## Evidence

- `tests/`
- `tests/conftest.py`
- `tests/test_parsers.py`
- `tests/test_metrics.py`
- `tests/test_routes.py`
- `tests/test_security.py`
- `tests/test_cli.py`
- `pytest -q` scan result

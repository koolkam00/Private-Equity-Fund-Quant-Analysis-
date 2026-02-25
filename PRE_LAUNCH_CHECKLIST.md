# Production Pre-Launch Checklist

Use this checklist before opening the app to external users.

## 1) Security Hardening

- [ ] `SECRET_KEY` is set in production and is not the default value.
- [ ] `FLASK_ENV=production` is set.
- [ ] `DATABASE_URL` points to managed Postgres (not local SQLite).
- [ ] HTTPS is enabled on the public domain (Render TLS active).
- [ ] Login is required on protected routes (`/dashboard`, `/deals`, `/track-record`, `/analysis/*`, `/ic-memo`, `/methodology`).
- [ ] Invite-only onboarding is enforced (`/team` invite flow only).
- [ ] At least one owner/admin exists in each active team.
- [ ] At least one firm exists in `/firms` and active firm selector is visible in header.
- [ ] Bootstrap credentials are rotated after first admin login, or removed from env when no longer needed.
- [ ] Session cookie settings are confirmed in production:
- [ ] `SESSION_COOKIE_SECURE=True`
- [ ] `SESSION_COOKIE_HTTPONLY=True`
- [ ] `SESSION_COOKIE_SAMESITE=Lax`
- [ ] Dependencies are pinned and installed from `requirements.txt`.
- [ ] No test/debug credentials are present in deployed environment variables.
- [ ] Upload size limits are acceptable (`MAX_CONTENT_LENGTH` policy reviewed).

## 2) Data Backup and Recovery

- [ ] Render Postgres automated snapshots/backups are enabled.
- [ ] Backup retention policy is documented and accepted by stakeholders.
- [ ] Manual backup test executed with `pg_dump`:

```bash
pg_dump "$DATABASE_URL" > prelaunch_backup.sql
```

- [ ] Restore drill completed at least once in a non-production database.
- [ ] Verified team-firm access behavior after restore (team sees only mapped firms).
- [ ] Verified firm scope behavior after restore (switching active firm changes visible analytics dataset).
- [ ] Confirmed re-upload behavior (`replace_fund`) only replaces the same fund within the same firm.
- [ ] Incident recovery owner is assigned (who performs restore if needed).

## 3) Smoke Tests (Pre-Go-Live)

Run these in a fresh browser session on the deployed URL.

- [ ] `GET /healthz` returns HTTP `200` and `{"status":"ok"}`.
- [ ] Login works with a valid user.
- [ ] Invalid login is rejected.
- [ ] Owner/admin can create an invite link on `/team`.
- [ ] Invite acceptance flow creates a user and team membership.
- [ ] Upload a workbook for `Firm A / Fund A` and confirm data appears on dashboard.
- [ ] Confirm `Firm Currency` upload behavior:
- [ ] Missing/blank `Firm Currency` defaults to `USD`.
- [ ] Valid ISO-3 values (for example `EUR`) are accepted and reflected in UI.
- [ ] Mixed currencies in one workbook are rejected.
- [ ] Non-USD uploads resolve upload-date FX and dashboard shows:
  - `Converted from <CCY> to USD at <rate> (effective <date>, source <source>)`.
- [ ] When FX lookup fails for non-USD upload:
  - upload still succeeds,
  - dashboard shows native-currency warning,
  - reporting currency metadata remains native (not USD).
- [ ] Post-deploy FX refresh command executed for failed firms:
  - `flask fx-refresh --failed-only`
  - verify updated non-USD firms now show conversion banner when rates resolve.
- [ ] Upload revised workbook for `Fund A` and confirm old `Fund A` rows are replaced.
- [ ] Upload `Firm A / Fund B` and confirm both funds coexist in the same firm.
- [ ] Upload a workbook for `Firm B / Fund X` and confirm firm auto-creation or selection works.
- [ ] Upload benchmark workbook (`Asset Class`, `Vintage Year`, `Metric`, `Quartile`, `Value`) and confirm success flash.
- [ ] Verify benchmark upload is full-replace at team scope (old benchmark rows are replaced).
- [ ] On dashboard, select `Benchmark Asset Class` and confirm Fund Summary shows:
  - `Net IRR Benchmark`, `Net MOIC Benchmark`, `Net DPI Benchmark`.
- [ ] Confirm benchmark ranking labels render (`Top 5%`, `1st Quartile`, `2nd Quartile`, `3rd Quartile`, `4th Quartile`, `N/A`).
- [ ] Confirm exact vintage-year matching behavior (non-matching vintage renders `N/A`).
- [ ] Global firm selector switches scope across pages (`/dashboard`, `/deals`, `/track-record`, `/analysis/*`, `/ic-memo`).
- [ ] Currency labels/values switch with active firm across dashboard, deals, track record, and bridge charts.
- [ ] Query fund filter still works as a request-level override for that page view.
- [ ] `/api/deals/<id>/bridge` returns `404` when deal belongs to a non-active firm.
- [ ] Track record PDF export works (`/track-record/pdf`).
- [ ] IC memo print-to-PDF works via `window.print()`.
- [ ] `GET /analysis/chart-builder` loads field catalog and renders board.
- [ ] Chart Builder query run succeeds (`/api/chart-builder/query`) and chart/table render with active firm scope.
- [ ] Chart Builder template CRUD works (`create`, `load`, `update`, `delete`) and remains team-scoped.

## 4) Performance and Stability Checks

- [ ] `pytest -q` passes on the release commit.
- [ ] App boots with gunicorn:

```bash
gunicorn wsgi:app
```

- [ ] No startup errors in Render logs.
- [ ] P95 page load time is acceptable for primary pages.
- [ ] No repeated 5xx errors in first-hour monitoring after deploy.

## 5) Launch Gate and Rollback

- [ ] Release commit hash is recorded.
- [ ] Deployment timestamp and operator are recorded.
- [ ] Rollback plan is documented (previous Render deploy + DB restore point).
- [ ] Final sign-off from product/ops/security stakeholders is captured.

## 6) Day-1 Operations

- [ ] Create initial team invites for pilot users.
- [ ] Create/confirm initial firm list and active-firm ownership expectations.
- [ ] Confirm support channel and owner for launch-day issues.
- [ ] Schedule first post-launch backup verification.
- [ ] Schedule first post-launch security review (credentials, access list, logs).

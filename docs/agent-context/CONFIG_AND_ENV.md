# CONFIG_AND_ENV

## Environment Variables

| Key | Required | Observed Effect | Evidence |
|---|---|---|---|
| `FLASK_ENV` | No | Sets production mode check used for cookies and secret enforcement | `config.py:8-27` |
| `SECRET_KEY` | Yes in production | Used for Flask session signing; production boot fails if missing/default | `config.py:11-14` |
| `DATABASE_URL` | No locally / Yes in production | Primary DB URL if `SQLALCHEMY_DATABASE_URI` absent; rewritten to `postgresql+psycopg://` | `config.py:16-21` |
| `SQLALCHEMY_DATABASE_URI` | Optional override | Same purpose as `DATABASE_URL`, checked first | `config.py:16-21` |
| `BOOTSTRAP_ADMIN_EMAIL` | Optional | Used only when no users exist and `bootstrap-admin` runs | `legacy_app.py:1024-1115`, `.env.example` |
| `BOOTSTRAP_ADMIN_PASSWORD` | Optional | Same as above | `legacy_app.py:1024-1115`, `.env.example` |
| `BOOTSTRAP_TEAM_NAME` | Optional | Default bootstrap team name | `legacy_app.py:1027`, `.env.example` |
| `PORT` | Optional | Used only by `legacy_app.py` when run as `__main__` | `legacy_app.py:4403-4405`, `.env.example` |

## Config Files

- `Observed:` `config.py`
  - central Flask config class
  - DB URL rewrite logic
  - cookie/security defaults
  - upload size/type policy
- `Observed:` `.env.example`
  - expected local/prod env keys
- `Observed:` `render.yaml`
  - deployment topology and env injection on Render
- `Observed:` `migrations/alembic.ini` and `migrations/env.py`
  - Alembic runtime config

## Environment-Specific Behavior

- `Observed:` Production (`FLASK_ENV=production`)
  - requires non-default `SECRET_KEY`
  - enables `SESSION_COOKIE_SECURE`
- `Observed:` Local / missing DB env
  - falls back to SQLite `instance/deals.db`
- `Observed:` Rate limits
  - use in-memory storage regardless of environment

## Feature Flags

- `Observed:` No dedicated feature-flag framework exists.
- `Observed:` The main runtime branch is `Config.IS_PRODUCTION`, derived from `FLASK_ENV`.
- `Observed:` Tests sometimes override `WTF_CSRF_ENABLED` via `create_app(config_override=...)`.

## Secrets Handling Patterns

- `Observed:` Secrets are expected from environment variables, not checked-in config.
- `Observed:` User passwords are stored as Werkzeug password hashes.
- `Observed:` Team invite tokens are hashed before storage (`legacy_app.py::_hash_invite_token`).
- `Observed:` Render marks `SECRET_KEY` and bootstrap admin creds as unsynced secrets in `render.yaml`.

## Dangerous Defaults

- `Observed:` Default dev secret key remains `"dev-secret-key-change-in-prod"` outside production.
- `Observed:` Default DB is local SQLite, which is convenient but easy to use accidentally if env is misconfigured.
- `Observed:` `WTF_CSRF_TIME_LIMIT = None`, so CSRF tokens do not expire by time.
- `Observed:` `UPLOAD_FOLDER` defaults to a local `uploads/` directory inside the repo.
- `Observed:` `Flask-Limiter` uses `memory://`, so limits are not shared across processes.

## Missing Validation / Config Risks

- `Observed:` `parse_deals` validates firm currency codes, but FX refresh during upload uses `date.today()` rather than the workbook `As Of Date`.
- `Observed:` No startup-time schema migration runs automatically; operators must run `db-upgrade`.
- `Observed:` No explicit validation exists for multi-process-safe rate-limit storage in production.
- `Observed:` No typed settings layer or schema exists beyond the `Config` class.

## Unconfirmed / Human-Decision Areas

- `Unconfirmed:` Whether firms are intended to be globally shared across teams or practically isolated by access mapping.
- `Unconfirmed:` Whether FX conversion should always use upload `As Of Date`, current date, or per-row historical dates.

## Evidence

- `config.py`
- `.env.example`
- `render.yaml`
- `peqa/extensions.py:19`
- `legacy_app.py:1024-1115`
- `services/deal_parser.py:1277-1299`
- `services/deal_parser.py:1582`

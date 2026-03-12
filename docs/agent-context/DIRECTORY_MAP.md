# DIRECTORY_MAP

## Top-Level Tree

```text
.
|-- app.py
|-- wsgi.py
|-- legacy_app.py
|-- config.py
|-- models.py
|-- requirements.txt
|-- render.yaml
|-- .env.example
|-- services/
|   |-- deal_parser.py
|   |-- benchmark_parser.py
|   |-- fx_rates.py
|   |-- utils.py
|   `-- metrics/
|-- peqa/
|   |-- app_factory.py
|   |-- extensions.py
|   |-- route_binding.py
|   |-- services/
|   |-- blueprints/
|   `-- presenters/
|-- templates/
|-- static/
|   |-- css/
|   `-- js/
|-- migrations/
|   `-- versions/
|-- tests/
|-- uploads/
|-- instance/
|-- architecture_and_calculations.md
|-- methodology.md
|-- PRE_LAUNCH_CHECKLIST.md
|-- debug_bridge.py
|-- debug_wavg.py
|-- test_app.py
`-- *.xlsx sample/template files
```

## Directory Purposes

- `Observed:` Root Python modules
  - Core runtime files: `app.py`, `wsgi.py`, `legacy_app.py`, `config.py`, `models.py`
  - `Core`
- `Observed:` `services/`
  - Primary business logic for ingestion and analytics
  - `Core`
- `Observed:` `peqa/`
  - App factory, extension wiring, route-binding helper, scope/context services, partial refactor scaffolding
  - `Core`, but unevenly mature
- `Observed:` `templates/`
  - Server-rendered Jinja views for dashboard, uploads, analyses, team/auth, and PDF-print layouts
  - `Core`
- `Observed:` `static/`
  - Shared JS and CSS; `static/js/app.js` contains most client-side behavior
  - `Core`
- `Observed:` `migrations/`
  - Alembic environment and one baseline migration
  - `Core` for deployment and schema lifecycle
- `Observed:` `tests/`
  - Pytest suite for parsers, metrics, routes, security, CLI, and chart builder
  - `Core`
- `Observed:` `uploads/`
  - Runtime upload staging directory created/used by the app
  - `Peripheral runtime state`
- `Observed:` `instance/`
  - Default SQLite DB location (`instance/deals.db`)
  - `Peripheral runtime state`

## Core vs Peripheral Areas

### Core

- `legacy_app.py`
- `services/deal_parser.py`
- `services/benchmark_parser.py`
- `services/metrics/`
- `models.py`
- `config.py`
- `peqa/app_factory.py`
- `peqa/services/context.py`
- `peqa/services/filtering.py`
- `templates/`
- `static/js/app.js`
- `tests/`

### Peripheral / Utility / Historical

- `peqa/blueprints/`
  - Mostly placeholder blueprints; real handlers are not here yet.
- `peqa/presenters/`
  - Stubs only.
- `debug_bridge.py`, `debug_wavg.py`
  - Manual debugging helpers.
- `test_app.py`
  - Manual smoke script, not part of the main pytest suite.
- `architecture_and_calculations.md`, `methodology.md`
  - Existing docs, but partially stale.
- `*.xlsx`
  - Templates / samples, not application logic.

## Where Key Concerns Live

- `Business logic`
  - `services/metrics/`
  - `services/deal_parser.py`
- `Configuration`
  - `config.py`
  - `.env.example`
  - `render.yaml`
- `Tests`
  - `tests/`
- `Deployment / infrastructure`
  - `render.yaml`
  - `wsgi.py`
  - `migrations/`
- `Assets / UI`
  - `templates/`
  - `static/css/style.css`
  - `static/js/app.js`
- `Auth / team management`
  - `legacy_app.py`
  - `models.py`
- `Scope filtering`
  - `peqa/services/context.py`
  - `peqa/services/filtering.py`

## Evidence

- `find . -maxdepth 2 ...`
- `peqa/blueprints/`
- `peqa/presenters/`
- `services/`
- `templates/`
- `static/`
- `tests/`

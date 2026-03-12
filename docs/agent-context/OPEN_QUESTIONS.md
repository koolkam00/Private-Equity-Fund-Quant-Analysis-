# OPEN_QUESTIONS

## Architecture / Ownership

- `Unconfirmed:` Is `peqa/` the intended long-term home for all routes, or is the current hybrid layout expected to remain?
  - Why it matters: future refactors could either migrate out of `legacy_app.py` or accidentally deepen the split.

- `Unconfirmed:` Are firms meant to be globally shared business entities across teams, or effectively team-owned records linked through `TeamFirmAccess`?
  - Why it matters: current delete/replace logic assumes `firm_id` can define safe cleanup scope.

## Data Semantics

- `Unconfirmed:` Should firm FX metadata use workbook `As Of Date`, upload timestamp, or latest deal creation timestamp?
  - Why it matters: code, checklist language, and likely user expectation are not aligned.

- `Unconfirmed:` Should `PublicMarketIndexLevel` be team-scoped only, or should there also be firm-level isolation?
  - Why it matters: upload deletion is triggered at firm scope but public-market rows are not.

- `Unconfirmed:` Is it acceptable that many LP pages degrade silently when optional sheets are absent, or should the UI block or warn more aggressively?
  - Why it matters: current payloads often return "low confidence" rather than hard failures.

## Runtime / Operations

- `Unconfirmed:` What dataset size is the app expected to support?
  - Why it matters: the current design is synchronous and Python-aggregation-heavy.

- `Unconfirmed:` Is in-memory rate limiting acceptable for the intended deployment shape?
  - Why it matters: Render may run a single process today, but the code does not enforce that assumption.

- `Unconfirmed:` Is automatic schema migration at startup intentionally avoided?
  - Why it matters: operators must remember `db-upgrade`; missing schema currently surfaces as runtime 503s / readiness failures.

## Product / UX

- `Unconfirmed:` Should the benchmark asset class be a session-wide selector, a per-page selector, or attached to persisted team preferences?
  - Why it matters: current selection is stored in session and reused across multiple pages/export flows.

- `Unconfirmed:` Is flashing raw invite URLs in the team UI a temporary admin convenience or the intended long-term invite-delivery mechanism?
  - Why it matters: link distribution/security expectations are not documented.

## Existing Docs To Treat Carefully

- `Observed:` `methodology.md` and `architecture_and_calculations.md` should not be assumed current without code verification.
- `Observed:` There is no top-level README describing the current production runtime path.

## Evidence

- `legacy_app.py`
- `services/deal_parser.py`
- `models.py`
- `peqa/services/context.py`
- `architecture_and_calculations.md`
- `methodology.md`

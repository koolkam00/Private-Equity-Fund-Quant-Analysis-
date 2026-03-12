# AI Memo Generation Blueprint

## Implementation status

- A working first-pass memo engine has been added in the repo under `peqa/services/memos/`.
- The app now includes memo ORM tables, a migration, memo routes in `legacy_app.py`, server-rendered memo pages, a DB-backed memo job queue, and inline job execution for dev/test.
- The current implementation supports local/S3 document storage, TXT/PDF/DOCX/PPTX extraction when the optional parsers are installed, heuristic style profiling, evidence bundling from existing analytics, section-by-section drafting, validation, approval, and markdown/HTML export.
- OCR, semantic vector retrieval, and richer LLM-driven drafting remain follow-on hardening items rather than complete in this first implementation slice.

## 1. Executive recommendation

Use a hybrid architecture:

- Treat existing analytics code as the quantitative system of record.
- Add a new memo subsystem for document ingestion, style learning, retrieval, drafting, validation, and assembly.
- Use structured style extraction plus section-level exemplars plus retrieval, not fine-tuning first.
- Use a bounded multi-stage pipeline, not a free-form agent swarm.
- Run memo ingestion and generation asynchronously in a worker, because the current app is synchronous Flask with no background processing.

The best fit for this repo is:

- thin new routes in [legacy_app.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/legacy_app.py)
- new greenfield services under `peqa/services/memos/`
- new ORM tables in [models.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/models.py)
- a new migration in [migrations/versions](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/migrations/versions)
- a new server-rendered memo workflow page plus JSON APIs, following the chart-builder pattern
- a dedicated worker process, backed by Postgres rows, with inline fallback for local SQLite dev

## 2. What the current app appears to do

- The app is a single Flask application bootstrapped by [peqa/app_factory.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/peqa/app_factory.py) and wired mostly through [legacy_app.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/legacy_app.py).
- It ingests Excel workbooks only, via [services/deal_parser.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/deal_parser.py) and [services/benchmark_parser.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/benchmark_parser.py).
- It stores structured deal, fund, benchmark, and upload-issue data in SQLAlchemy models in [models.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/models.py).
- It computes analytics at request time in Python from structured data in [services/metrics](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics).
- It renders server-side pages in [templates](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/templates) and uses lightweight JS in [static/js/app.js](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/static/js/app.js).
- It already has two memo-like outputs:
  - deterministic IC memo from [services/metrics/ic_memo.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/ic_memo.py) rendered by [templates/ic_memo.html](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/templates/ic_memo.html)
  - deterministic LP diligence memo from [services/metrics/lp.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/lp.py) rendered by [templates/analysis_lp_due_diligence_memo.html](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/templates/analysis_lp_due_diligence_memo.html)

## 3. Relevant existing components to reuse

- Scope and filter resolution: [peqa/services/context.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/peqa/services/context.py), [peqa/services/filtering.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/peqa/services/filtering.py)
- Quantitative facts and calculations: [services/metrics/deal.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/deal.py), [services/metrics/portfolio.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/portfolio.py), [services/metrics/analysis.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/analysis.py), [services/metrics/lp.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/lp.py)
- Existing memo surfaces and section patterns: [services/metrics/ic_memo.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/ic_memo.py), [templates/ic_memo.html](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/templates/ic_memo.html), [templates/analysis_lp_due_diligence_memo.html](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/templates/analysis_lp_due_diligence_memo.html)
- Structured cross-source row loading: [services/metrics/chart_builder.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/chart_builder.py)
- Team and firm scoping model: [models.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/models.py)
- JSON API + server-rendered interaction pattern: chart builder routes in [legacy_app.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/legacy_app.py)

## 4. Gaps that must be added

- No PDF, DOCX, PPTX, or general diligence-document ingestion exists.
- No durable source-document storage exists. Current uploads are temporary files and are deleted after parsing.
- No queue or worker exists.
- No LLM abstraction, prompt stack, embedding flow, or retrieval layer exists.
- No claim-level citation or validation layer exists.
- No style-profile or prior-memo corpus model exists.
- No typed API contract or OpenAPI spec exists.

## 5. Recommended architecture for memo generation

- Add a new subsystem under `peqa/services/memos/`.
- Keep memo routes in [legacy_app.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/legacy_app.py) for now, because real routing still lives there.
- Build a new `MemoEvidenceBuilder` that pulls structured facts from existing analytics code and raw qualitative evidence from memo/diligence document chunks.
- Build a DB-backed job table and worker loop. In local SQLite, support inline execution for tests and development. In production Postgres, run a separate worker service.
- Persist all intermediate artifacts: extracted text, chunks, embeddings, style profiles, outlines, section drafts, validations, final memo.

## 6. Best approach for learning my memo style

Use style extraction plus exemplars plus delexicalization:

- Ingest prior memos as documents.
- Split by heading structure and section order.
- Delexicalize entities, numbers, and names before style analysis so the model learns voice and structure without copying old deal-specific content.
- Extract a `MemoStyleProfile` JSON object:
  - canonical section order
  - heading variants
  - opening and closing patterns
  - tone rules
  - reasoning rules
  - risk-questioning habits
  - preferred use of bullets vs prose
- Store section-level exemplars separately for retrieval during drafting.

Do not fine-tune first. Fine-tuning is a poor first move here because:

- the hard problem is grounding, not fluency
- the factual substrate changes every memo
- the likely corpus size is small
- retrieval plus structure is easier to audit and update

## 7. Recommended end-to-end memo pipeline

1. Upload prior memos and supporting diligence files.
2. Persist original files in durable storage, not `uploads/`.
3. Extract text, section boundaries, page references, and chunk records.
4. Build or refresh style profile from prior memos.
5. User creates a memo run for a firm, fund scope, and benchmark scope.
6. Build structured evidence bundle from app analytics plus retrieved document chunks.
7. Generate outline in the user’s learned section order.
8. Draft each section from section-specific evidence plus style exemplars.
9. Validate each section for unsupported claims, numeric mismatches, missing citations, and conflicts.
10. Assemble final memo with explicit fact/calc/synthesis tagging and an open-questions block.
11. Present section review UI with citations and validation flags before export.

## 8. Grounding and anti-hallucination design

- App-calculated numbers are primary. Uploaded DDQ/PPM/deck numbers are secondary evidence and conflict checks.
- Every claim must carry provenance:
  - `app_metric`
  - `app_analysis`
  - `document_chunk`
  - `derived_calculation`
  - `ai_synthesis`
- Drafting should return structured JSON, not raw prose only.
- Validation should reject or downgrade any uncited numeric claim.
- Missing or conflicting inputs should produce:
  - explicit `missing_data`
  - explicit `conflicts`
  - explicit `open_diligence_questions`
- Add a copy-similarity guard against reproducing old memo passages verbatim.

## 9. UX / product flow

- Add a new sidebar item: `AI Investment Memos`.
- New flow:
  - `Style Library`: upload/select prior memos, inspect extracted section map, approve style profile
  - `Source Library`: upload DDQ/PPM/decks and see extraction status
  - `Generate Memo`: choose scope, memo type, style profile, and optional user notes
  - `Review Memo`: section-by-section draft, evidence drawer, validation flags, open questions, export
- Keep the app server-rendered. Use a small polling UI in [static/js/app.js](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/static/js/app.js), similar to chart builder, instead of adding a frontend framework.

## 10. Data model / schema changes

Add new tables in [models.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/models.py):

- `memo_documents`
- `memo_document_chunks`
- `memo_style_profiles`
- `memo_style_exemplars`
- `memo_generation_runs`
- `memo_generation_sections`
- `memo_generation_claims`

Use `team_id` on every table and `firm_id` where scope matters. For style profiles, also store `created_by_user_id`, because style is likely user-specific even if shared within a team.

## 11. API / backend changes

Add JSON routes in [legacy_app.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/legacy_app.py):

- `POST /memo-documents` upload prior memos and diligence docs
- `GET /api/memo-documents`
- `POST /api/memo-style-profiles/rebuild`
- `GET /api/memo-style-profiles`
- `POST /api/memo-runs`
- `GET /api/memo-runs/<id>`
- `GET /api/memo-runs/<id>/sections`
- `POST /api/memo-runs/<id>/rerun-section`
- `POST /api/memo-runs/<id>/approve`
- `GET /memo-runs/<id>` server-rendered review page

Add a worker CLI command:

- `flask memo-worker`

## 12. Frontend changes

Create new templates:

- `templates/memo_studio.html`
- `templates/memo_run.html`

Add JS to [static/js/app.js](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/static/js/app.js) for:

- upload progress
- polling run status
- loading section evidence drawers
- rerun/approve actions
- displaying unsupported claims and conflicts

Reuse the existing page shell and density toggle from [templates/base.html](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/templates/base.html).

## 13. Validation / QA design

- Unit tests for document parsers, chunking, style extraction normalization, retrieval, and claim validation.
- Integration tests for memo run orchestration and scoping.
- Snapshot tests for outline JSON and section JSON contracts.
- Regression fixtures with known conflicting figures and missing data.
- Route tests following the current suite style in [tests](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/tests).
- Add local inline worker mode so tests stay simple and deterministic.

## 14. Phased implementation roadmap

### MVP

- prior-memo ingestion
- PDF/DOCX/PPTX text extraction
- style profile extraction
- structured evidence builder from existing app analytics
- async memo generation run with outline plus draft plus validation
- review UI with citations and open questions

### V2

- numeric fact extraction from DDQ/PPM/decks for conflict detection
- richer section editing and rerun controls
- export to DOCX/PDF
- team-shared style profiles and memo templates
- better OCR for scanned PDFs

### Ideal end state

- pgvector or equivalent indexed retrieval in Postgres
- reusable memo templates by memo type
- automated delta memos for updated diligence packets
- richer conflict resolution UI
- continuous style-profile retraining from approved memos

## 15. Biggest risks and failure modes

- Wrong numbers due to currency scaling, stale as-of logic, or gross/net conflicts.
- Hallucinated qualitative claims from weak retrieval.
- Over-copying prior memos instead of matching style.
- Bad PDF/deck extraction, especially for scanned documents.
- Scope leakage across teams or firms.
- Long-running jobs timing out on the web process.
- Existing docs misleading implementers.

Mitigations:

- build facts from deterministic Python first
- validate every numeric claim
- use delexicalized style exemplars
- persist provenance and validation output
- add team/firm scoping tests for every new table
- move work to a worker

## 16. Exact next engineering steps

1. Add durable document storage abstraction and new memo tables.
2. Add memo document upload routes and extraction pipeline.
3. Add style-profile builder from prior memos.
4. Add memo evidence builder from existing analytics plus doc chunks.
5. Add DB-backed memo job runner and worker CLI.
6. Add outline, section draft, validation, and final assembly services.
7. Add memo studio UI and review page.
8. Add test coverage before tuning prompts.

## A. Proposed system diagram

```text
Browser
  -> Flask route in legacy_app.py
    -> Memo run row created in DB
      -> Worker claims job
        -> Document storage fetch
        -> Text extraction / chunking
        -> Style profile loader
        -> Existing analytics services
        -> Memo evidence builder
        -> LLM outline stage
        -> LLM section drafting stage
        -> Validation stage
        -> Final assembly
      -> DB stores sections, claims, validations, final memo
  -> Review UI polls run status and renders evidence-backed sections
```

## B. Proposed memo-generation pipeline

```text
Upload docs
-> extract text
-> split into sections/chunks
-> embed chunks
-> rebuild style profile
-> create memo run
-> gather app facts
-> retrieve relevant memo exemplars
-> retrieve relevant diligence chunks
-> generate outline
-> draft section JSON
-> validate citations and numbers
-> assemble memo
-> human review
-> export
```

## C. Proposed data structures / schema examples

```json
{
  "memo_style_profile": {
    "id": 12,
    "team_id": 3,
    "created_by_user_id": 8,
    "name": "Andrew Default Memo Style",
    "section_order": ["executive_summary", "fund_overview", "team", "performance", "risks", "questions", "recommendation"],
    "voice_rules": {
      "tone": "direct, analytical, skeptical",
      "preferred_opening": "state thesis early",
      "hedging": "surface uncertainty explicitly"
    },
    "reasoning_rules": [
      "separate facts from judgment",
      "highlight missing data before concluding",
      "state downside cases explicitly"
    ]
  }
}
```

```json
{
  "memo_generation_run": {
    "id": 44,
    "team_id": 3,
    "firm_id": 9,
    "fund_scope": "Fund IV",
    "benchmark_asset_class": "Buyout",
    "style_profile_id": 12,
    "status": "review_required",
    "missing_data": ["no fund cashflow history for Fund IV"],
    "conflicts": ["DDQ states 1.9x gross MOIC, app dataset computes 1.7x"]
  }
}
```

```json
{
  "memo_generation_section": {
    "run_id": 44,
    "section_key": "risks",
    "title": "Key Risks and Diligence Gaps",
    "draft_text": "...",
    "validation": {
      "unsupported_claims": [],
      "numeric_mismatches": [],
      "missing_citations": 1
    }
  }
}
```

## D. Proposed prompt stack

### Style extraction

```text
Input: delexicalized prior memo sections
Output JSON:
- canonical_sections
- heading_aliases
- tone_rules
- reasoning_rules
- opening_patterns
- closing_patterns
- section_specific_guidelines
Constraint: describe style only; do not preserve deal-specific facts
```

### Outline generation

```text
Input:
- style profile
- memo type
- app evidence summary
- missing data
- conflicts
Output JSON:
- ordered sections
- per-section objective
- required evidence ids
- required open questions
Constraint: include missing-data and conflict sections when evidence is incomplete
```

### Section drafting

```text
Input:
- one section spec
- app facts
- retrieved diligence chunks
- section exemplars
- rules for fact/calc/synthesis separation
Output JSON:
- paragraphs
- claim list
- citation ids
- open questions
Constraint: do not invent numbers; if support is missing, write a question instead
```

### Validation

```text
Input:
- drafted section JSON
- evidence bundle
- claim provenance
Output JSON:
- unsupported_claims
- citation_gaps
- numeric_mismatches
- conflict_flags
- suggested_revision
Constraint: fail closed on unsupported numeric claims
```

### Final assembly

```text
Input:
- validated sections
- style profile
- memo metadata
Output:
- markdown
- html
- fact/calc/synthesis footnotes
- open diligence questions appendix
```

## E. Proposed agent workflow

Use staged agents only if implemented as explicit services:

- `StyleProfiler`
- `EvidencePlanner`
- `SectionDrafter`
- `SectionValidator`
- `MemoAssembler`

This should be orchestrated by code, not by autonomous agent-to-agent planning.

## F. Implementation checklist

- Add new models and migration
- Add durable document storage abstraction
- Add parsers for PDF, DOCX, PPTX
- Add chunking and embedding pipeline
- Add style profile extraction
- Add memo evidence builder
- Add memo generation job runner
- Add memo prompts and validation contracts
- Add memo studio templates and JS
- Add worker CLI and Render worker service
- Add tests for ingestion, orchestration, validation, and scoping

## Exact files to modify or create

Modify:

- [legacy_app.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/legacy_app.py): add memo routes, APIs, and worker CLI registration
- [models.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/models.py): add memo ORM models
- [templates/base.html](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/templates/base.html): add navigation entry
- [static/js/app.js](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/static/js/app.js): add upload/poll/review behavior
- [render.yaml](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/render.yaml): add worker service
- [requirements.txt](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/requirements.txt): add document extraction and embedding dependencies
- [services/metrics/chart_builder.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/chart_builder.py): extract reusable source-row loading into a shared evidence loader

Create:

- `peqa/services/memos/__init__.py`
- `peqa/services/memos/types.py`
- `peqa/services/memos/storage.py`
- `peqa/services/memos/document_ingestion.py`
- `peqa/services/memos/style_profiles.py`
- `peqa/services/memos/evidence_builder.py`
- `peqa/services/memos/retrieval.py`
- `peqa/services/memos/prompts.py`
- `peqa/services/memos/orchestrator.py`
- `peqa/services/memos/validation.py`
- `peqa/services/memos/worker.py`
- `templates/memo_studio.html`
- `templates/memo_run.html`
- `tests/test_memo_ingestion.py`
- `tests/test_memo_generation.py`
- `tests/test_memo_routes.py`
- `migrations/versions/<new_revision>_memo_generation_tables.py`

## Orchestration pseudocode

```python
def create_memo_run(team_id, firm_id, style_profile_id, document_ids, filters, benchmark_asset_class, created_by_user_id):
    run = MemoGenerationRun(...)
    db.session.add(run)
    db.session.commit()
    enqueue_run(run.id)
    return run


def process_memo_run(run_id):
    run = claim_run(run_id)
    style = load_style_profile(run.style_profile_id)
    evidence = build_memo_evidence(
        team_id=run.team_id,
        firm_id=run.firm_id,
        filters=run.filters_json,
        benchmark_asset_class=run.benchmark_asset_class,
        document_ids=run.document_ids,
    )
    outline = generate_outline(style, evidence)
    save_outline(run, outline)

    for section in outline["sections"]:
        ctx = build_section_context(style, evidence, section)
        draft = draft_section(ctx)
        validation = validate_section(draft, evidence)
        save_section(run, section, draft, validation)

    final_doc = assemble_memo(run.id)
    save_final_doc(run, final_doc)
    mark_run_review_required(run.id)
```

## Recommended representation for prior memo style patterns and exemplars

- `memo_style_profiles.profile_json`
  - normalized voice rules
  - canonical section order
  - reasoning heuristics
  - transition patterns
- `memo_style_exemplars`
  - `style_profile_id`
  - `section_key`
  - `heading_text`
  - `exemplar_text_delexicalized`
  - `source_document_id`
  - `similarity_embedding_json`
  - `rank`

The prompts should consume delexicalized exemplars by default, and link back to raw source only for audit.

## Repo-specific constraints and mismatches to account for

- [architecture_and_calculations.md](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/architecture_and_calculations.md) and [methodology.md](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/methodology.md) are stale; the app is not deal-only anymore.
- The upload template [PE_Fund_Data_Template.xlsx](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/PE_Fund_Data_Template.xlsx) exposes fewer optional sheets than the parser currently supports.
- [templates/upload.html](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/templates/upload.html) says each workbook must contain exactly one firm in the Deals sheet, but [services/deal_parser.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/deal_parser.py) and tests support multi-firm uploads.
- [services/metrics/ic_memo.py](/Users/andrewkam/private fund analysis/Private-Equity-Fund-Quant-Analysis-/services/metrics/ic_memo.py) stamps memo `meta.as_of` with `date.today()`, so its metadata is not reliable enough to reuse blindly for AI memo generation.
- Current document uploads are temporary and deleted, so memo-source files cannot rely on the existing upload mechanism.

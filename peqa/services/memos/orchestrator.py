from __future__ import annotations

import json
import logging
import os
import re

from flask import current_app

from models import MemoGenerationClaim, MemoGenerationRun, MemoGenerationSection, db
from peqa.services.memos.assembly import assemble_memo
from peqa.services.memos.evidence_builder import build_memo_evidence_bundle
from peqa.services.memos.prompts import (
    DEFAULT_MEMO_SECTIONS,
    build_outline_prompt,
    build_section_drafting_prompt,
)
from peqa.services.memos.retrieval import retrieve_section_evidence
from peqa.services.memos.style_profiles import load_style_profile
from peqa.services.memos.types import DraftSection, dataclass_to_dict
from peqa.services.memos.validation import extract_claims, validate_section


logger = logging.getLogger(__name__)


def _json_dumps(value):
    return json.dumps(value, sort_keys=True, default=str)


def _json_loads(value, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _humanize_section_key(section_key: str) -> str:
    return section_key.replace("_", " ").strip().title()


def _style_voice(style_profile: dict) -> dict:
    tone = style_profile.get("tone") or {}
    first_person_ratio = tone.get("first_person_chunk_ratio") or 0
    return {
        "recommend_prefix": "We recommend" if first_person_ratio >= 0.2 else "Recommendation:",
        "observation_prefix": "We note" if first_person_ratio >= 0.2 else "Observation:",
    }


def _safe_top_snippets(retrieval_pack, limit: int = 2) -> list[str]:
    snippets = []
    for item in retrieval_pack.items[:limit]:
        text = re.sub(r"\s+", " ", item.text or "").strip()
        if text:
            snippets.append(text[:280])
    return snippets


def _outline_from_style(style_profile: dict) -> list[dict]:
    defaults = {section.key: section for section in DEFAULT_MEMO_SECTIONS}
    ordered_keys = style_profile.get("section_order") or [section.key for section in DEFAULT_MEMO_SECTIONS]
    seen = set()
    sections = []
    primary_headings = style_profile.get("primary_headings") or {}

    for key in ordered_keys:
        if key in seen:
            continue
        seen.add(key)
        default = defaults.get(key)
        sections.append(
            {
                "key": key,
                "title": primary_headings.get(key) or (default.title if default else _humanize_section_key(key)),
                "objective": default.objective if default else "Draft this memo section using grounded evidence only.",
                "required_evidence": default.required_evidence if default else ["structured_facts", "analysis_summaries"],
            }
        )

    for default in DEFAULT_MEMO_SECTIONS:
        if default.key in seen:
            continue
        sections.append(
            {
                "key": default.key,
                "title": default.title,
                "objective": default.objective,
                "required_evidence": default.required_evidence,
            }
        )
    return sections


def _provider_enabled() -> bool:
    provider = (current_app.config.get("MEMO_LLM_PROVIDER") or "disabled").strip().lower()
    return provider == "openai" and bool(os.environ.get("OPENAI_API_KEY"))


def _call_openai_json(model: str, prompt: dict) -> dict | None:
    if not _provider_enabled():
        return None
    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("OpenAI SDK not installed; falling back to deterministic memo generation.")
        return None

    client = OpenAI()
    try:
        input_payload = json.dumps(prompt["input"], default=str)
        if hasattr(client, "responses"):
            response = client.responses.create(
                model=model,
                temperature=0,
                input=[
                    {"role": "system", "content": [{"type": "input_text", "text": prompt["system"]}]},
                    {"role": "user", "content": [{"type": "input_text", "text": input_payload}]},
                ],
            )
            output_text = getattr(response, "output_text", None)
            if output_text:
                return json.loads(output_text)
        response = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": prompt["system"]},
                {"role": "user", "content": input_payload},
            ],
        )
        output_text = response.choices[0].message.content
        return json.loads(output_text)
    except Exception:
        logger.exception("LLM memo generation failed; falling back to deterministic generation.")
        return None


def _build_outline(style_profile: dict, evidence_bundle) -> list[dict]:
    prompt = build_outline_prompt(
        style_profile,
        {
            "structured_facts": evidence_bundle.structured_facts,
            "missing_data": evidence_bundle.missing_data,
            "conflicts": evidence_bundle.conflicts,
            "open_questions": evidence_bundle.open_questions,
        },
    )
    ai_result = _call_openai_json(current_app.config["MEMO_LLM_MODEL_OUTLINE"], prompt)
    if isinstance(ai_result, dict) and isinstance(ai_result.get("sections"), list):
        sections = []
        for row in ai_result["sections"]:
            if not row.get("key"):
                continue
            sections.append(
                {
                    "key": row["key"],
                    "title": row.get("title") or _humanize_section_key(row["key"]),
                    "objective": row.get("objective") or "",
                    "required_evidence": row.get("required_evidence") or [],
                }
            )
        if sections:
            return sections
    return _outline_from_style(style_profile)


def _draft_text_for_section(section_spec: dict, style_profile: dict, evidence_bundle, retrieval_pack) -> DraftSection:
    voice = _style_voice(style_profile)
    facts = evidence_bundle.structured_facts or {}
    lp_payload = (evidence_bundle.analysis_summaries or {}).get("lp_due_diligence_memo") or {}
    snippets = _safe_top_snippets(retrieval_pack)
    citations = [
        {
            "id": "fact:structured_facts",
            "source_type": "app_fact",
            "label": "Structured app facts",
            "excerpt": "Derived from existing portfolio and LP diligence analytics.",
        }
    ]
    for item in retrieval_pack.items[:3]:
        citations.append(
            {
                "id": item.source_id,
                "source_type": item.source_type,
                "label": item.label,
                "excerpt": item.text[:220],
                "page_start": item.page_start,
                "page_end": item.page_end,
            }
        )

    paragraphs = []
    section_key = section_spec["key"]
    if section_key == "executive_summary":
        recommendation = "proceed with diligence" if len(evidence_bundle.open_questions) <= 3 else "hold pending resolution of the open diligence items"
        paragraphs.append(
            f"{voice['recommend_prefix']} {recommendation}. The current app evidence covers {facts.get('fund_count') or 0} funds, "
            f"with {(facts.get('funds_with_decision_ready_reporting') or 0)} funds showing decision-ready reporting quality and "
            f"{(facts.get('pme_complete_funds') or 0)} funds with complete PME coverage."
        )
        if snippets:
            paragraphs.append(f"{voice['observation_prefix']} the source materials emphasize: {snippets[0]}")
    elif section_key == "fund_overview":
        fund_rows = facts.get("fund_metadata") or []
        managers = sorted({row.get("manager_name") for row in fund_rows if row.get("manager_name")})
        strategies = sorted({row.get("strategy") for row in fund_rows if row.get("strategy")})
        paragraphs.append(
            f"The memo scope covers {len(fund_rows)} fund(s) under the active diligence context. "
            f"Managers referenced in the current scope include {', '.join(managers) if managers else 'the manager data currently loaded in the app'}."
        )
        if strategies:
            paragraphs.append(f"Primary strategy descriptors in the current data include {', '.join(strategies)}.")
        if snippets:
            paragraphs.append(f"Source-document context: {snippets[0]}")
    elif section_key == "performance_and_benchmarking":
        benchmark_complete = facts.get("benchmark_complete_funds") or 0
        pme_complete = facts.get("pme_complete_funds") or 0
        paragraphs.append(
            f"Benchmark coverage is complete for {benchmark_complete} fund(s), while PME coverage is complete for {pme_complete} fund(s). "
            "Interpret relative-performance conclusions within those coverage constraints."
        )
        risk_flags = (lp_payload.get("benchmark_confidence") or {}).get("risk_flags") or []
        if risk_flags:
            paragraphs.append("Benchmarking caveats: " + "; ".join(risk_flags[:3]))
        if snippets:
            paragraphs.append(f"Supporting materials also state: {snippets[0]}")
    elif section_key == "liquidity_and_realization":
        paragraphs.append(
            f"Current liquidity signals show a representative DPI of {facts.get('liquidity_current_dpi') if facts.get('liquidity_current_dpi') is not None else 'n/a'} "
            f"and TVPI of {facts.get('liquidity_current_tvpi') if facts.get('liquidity_current_tvpi') is not None else 'n/a'} in the currently loaded fund data."
        )
        liquidity_flags = (lp_payload.get("liquidity_quality") or {}).get("risk_flags") or []
        if liquidity_flags:
            paragraphs.append("Liquidity risk flags: " + "; ".join(liquidity_flags[:3]))
    elif section_key == "risks_and_open_questions":
        if evidence_bundle.missing_data:
            paragraphs.append("Missing or incomplete data items: " + "; ".join(item.get("message", "") for item in evidence_bundle.missing_data[:5]))
        if evidence_bundle.conflicts:
            paragraphs.append("Conflicting or low-confidence items: " + "; ".join(item.get("message", "") for item in evidence_bundle.conflicts[:5]))
        if evidence_bundle.open_questions:
            paragraphs.append("Open diligence questions: " + "; ".join(item.get("question", "") for item in evidence_bundle.open_questions[:5]))
    elif section_key == "recommendation":
        issue_count = len(evidence_bundle.missing_data) + len(evidence_bundle.conflicts)
        if issue_count == 0:
            paragraphs.append(f"{voice['recommend_prefix']} continue toward approval, subject to routine confirmatory diligence.")
        elif issue_count <= 3:
            paragraphs.append(f"{voice['recommend_prefix']} continue diligence with conditions tied to the unresolved items listed above.")
        else:
            paragraphs.append(f"{voice['recommend_prefix']} do not finalize an investment memo until the unresolved conflicts and missing data are addressed.")
    else:
        paragraphs.append(
            f"This section synthesizes existing app outputs and source materials for {section_spec['title'].lower()} without extending beyond the available evidence."
        )
        if snippets:
            paragraphs.append(f"Retrieved support: {snippets[0]}")

    if not paragraphs:
        paragraphs.append("No grounded evidence was available to draft this section.")

    text = "\n\n".join(paragraphs).strip()
    paragraph_map = [{"text": paragraph, "citation_ids": [citation["id"] for citation in citations[:2]]} for paragraph in paragraphs]
    claims = extract_claims(section_key, text, citations)
    open_questions = [
        question for question in evidence_bundle.open_questions if question.get("question")
    ][:5]
    return DraftSection(
        key=section_key,
        title=section_spec["title"],
        text=text,
        citations=citations,
        claims=claims,
        open_questions=open_questions if section_key in {"executive_summary", "risks_and_open_questions", "recommendation"} else [],
        paragraph_map=paragraph_map,
        metadata={
            "required_evidence": section_spec.get("required_evidence") or [],
            "style_profile_id": style_profile.get("id"),
            "exemplar_count": len(retrieval_pack.exemplars),
        },
    )


def _draft_section(section_spec: dict, style_profile: dict, evidence_bundle, retrieval_pack) -> DraftSection:
    prompt = build_section_drafting_prompt(
        section_spec=section_spec,
        style_profile=style_profile,
        retrieval_pack=dataclass_to_dict(retrieval_pack),
        evidence_bundle=dataclass_to_dict(evidence_bundle),
    )
    ai_result = _call_openai_json(current_app.config["MEMO_LLM_MODEL_DRAFT"], prompt)
    if isinstance(ai_result, dict) and ai_result.get("text"):
        citations = ai_result.get("citations") or []
        claims = ai_result.get("claims") or extract_claims(section_spec["key"], ai_result["text"], citations)
        paragraph_map = ai_result.get("paragraph_map") or [
            {"text": paragraph.strip(), "citation_ids": [citation.get("id") for citation in citations if citation.get("id")]}
            for paragraph in ai_result["text"].split("\n\n")
            if paragraph.strip()
        ]
        return DraftSection(
            key=section_spec["key"],
            title=section_spec["title"],
            text=ai_result["text"].strip(),
            citations=citations,
            claims=claims,
            open_questions=ai_result.get("open_questions") or [],
            paragraph_map=paragraph_map,
            metadata=ai_result.get("metadata") or {},
        )
    return _draft_text_for_section(section_spec, style_profile, evidence_bundle, retrieval_pack)


def _persist_section(run: MemoGenerationRun, section_order: int, section_spec: dict, draft: DraftSection, validation_result) -> MemoGenerationSection:
    section_row = MemoGenerationSection.query.filter_by(run_id=run.id, section_key=section_spec["key"]).first()
    if section_row is None:
        section_row = MemoGenerationSection(run_id=run.id, section_key=section_spec["key"])
    section_row.section_order = section_order
    section_row.title = section_spec["title"]
    section_row.objective = section_spec.get("objective")
    section_row.required_evidence_json = _json_dumps(section_spec.get("required_evidence") or [])
    section_row.draft_json = _json_dumps(dataclass_to_dict(draft))
    section_row.draft_text = draft.text
    section_row.validation_json = _json_dumps(dataclass_to_dict(validation_result))
    section_row.review_status = "needs_review" if validation_result.status != "ready" else "ready"
    section_row.status = validation_result.status
    db.session.add(section_row)
    db.session.flush()

    MemoGenerationClaim.query.filter_by(run_id=run.id, section_id=section_row.id).delete(synchronize_session=False)
    for claim in draft.claims:
        citation_ids = claim.get("citation_ids") or []
        mismatch_reason = None
        if validation_result.numeric_mismatches:
            for mismatch in validation_result.numeric_mismatches:
                if mismatch.get("claim_text") == claim.get("claim_text"):
                    mismatch_reason = mismatch.get("reason")
                    break
        db.session.add(
            MemoGenerationClaim(
                run_id=run.id,
                section_id=section_row.id,
                claim_type=claim.get("claim_type") or "synthesis",
                claim_text=claim.get("claim_text") or "",
                provenance_type=claim.get("provenance_type"),
                provenance_id=claim.get("provenance_id"),
                citation_json=_json_dumps({"citation_ids": citation_ids}),
                validation_status="ready" if mismatch_reason is None else "mismatch",
                mismatch_reason=mismatch_reason,
                status="ready" if mismatch_reason is None else "blocked",
            )
        )
    db.session.commit()
    return section_row


def _assemble_and_save(run: MemoGenerationRun, style_profile: dict) -> MemoGenerationRun:
    section_rows = (
        MemoGenerationSection.query.filter_by(run_id=run.id)
        .order_by(MemoGenerationSection.section_order.asc(), MemoGenerationSection.id.asc())
        .all()
    )
    assembled = assemble_memo(
        style_profile,
        [
            {
                "section_key": row.section_key,
                "title": row.title,
                "draft_text": row.draft_text,
                "validation": _json_loads(row.validation_json, {}),
                "review_status": row.review_status,
            }
            for row in section_rows
        ],
    )
    run.final_markdown = assembled.markdown
    run.final_html = assembled.html
    run.status = "review_required"
    run.progress_stage = "review"
    db.session.add(run)
    db.session.commit()
    return run


def generate_memo_run(run_id: int) -> MemoGenerationRun:
    run = db.session.get(MemoGenerationRun, run_id)
    if run is None:
        raise ValueError(f"Memo run {run_id} not found")

    run.status = "running"
    run.progress_stage = "building_evidence"
    db.session.add(run)
    db.session.commit()

    style_profile = load_style_profile(run.style_profile_id)
    evidence_bundle = build_memo_evidence_bundle(run.id)
    outline = _build_outline(style_profile, evidence_bundle)
    run.outline_json = _json_dumps({"sections": outline})
    run.progress_stage = "drafting"
    db.session.add(run)
    db.session.commit()

    MemoGenerationClaim.query.filter_by(run_id=run.id).delete(synchronize_session=False)
    MemoGenerationSection.query.filter_by(run_id=run.id).delete(synchronize_session=False)
    db.session.commit()

    for index, section_spec in enumerate(outline, start=1):
        run.progress_stage = f"drafting:{section_spec['key']}"
        db.session.add(run)
        db.session.commit()
        retrieval_pack = retrieve_section_evidence(section_spec, evidence_bundle, style_profile)
        draft = _draft_section(section_spec, style_profile, evidence_bundle, retrieval_pack)
        validation_result = validate_section(draft, evidence_bundle)
        _persist_section(run, index, section_spec, draft, validation_result)

    return _assemble_and_save(run, style_profile)


def rerun_memo_section(run_id: int, section_key: str) -> MemoGenerationRun:
    run = db.session.get(MemoGenerationRun, run_id)
    if run is None:
        raise ValueError(f"Memo run {run_id} not found")

    style_profile = load_style_profile(run.style_profile_id)
    evidence_bundle = build_memo_evidence_bundle(run.id)
    outline_sections = (_json_loads(run.outline_json, {}) or {}).get("sections") or _outline_from_style(style_profile)
    section_spec = next((section for section in outline_sections if section.get("key") == section_key), None)
    if section_spec is None:
        raise ValueError(f"Section {section_key} not found in run outline")

    run.status = "running"
    run.progress_stage = f"rerunning:{section_key}"
    db.session.add(run)
    db.session.commit()

    retrieval_pack = retrieve_section_evidence(section_spec, evidence_bundle, style_profile)
    draft = _draft_section(section_spec, style_profile, evidence_bundle, retrieval_pack)
    validation_result = validate_section(draft, evidence_bundle)

    existing = MemoGenerationSection.query.filter_by(run_id=run.id, section_key=section_key).first()
    section_order = existing.section_order if existing is not None else 999
    _persist_section(run, section_order, section_spec, draft, validation_result)
    return _assemble_and_save(run, style_profile)

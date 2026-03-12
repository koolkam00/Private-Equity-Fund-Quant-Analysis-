from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from statistics import median

from flask import current_app

from models import MemoDocument, MemoDocumentChunk, MemoStyleExemplar, MemoStyleProfile, db
from peqa.services.memos.chunking import canonical_section_key
from peqa.services.memos.llm import call_openai_json
from peqa.services.memos.prompts import build_style_extraction_prompt


STYLE_SOURCE_ROLES = {"prior_memo", "approved_generated_memo"}
TOKEN_RE = re.compile(r"[a-z0-9\[\]_']+")
SENTENCE_RE = re.compile(r"(?<=[\.\?\!])\s+")
HEDGE_PHRASES = (
    "subject to",
    "requires further diligence",
    "requires follow up",
    "remains outstanding",
    "pending",
    "unclear",
    "we need",
    "open question",
    "requires confirmation",
)
RECOMMENDATION_PHRASES = (
    "we recommend",
    "recommend approval",
    "recommend proceeding",
    "do not recommend",
    "we would proceed",
    "we would not underwrite",
    "we would decline",
)
TRANSITION_PHRASES = (
    "that said",
    "however",
    "against that backdrop",
    "on balance",
    "more importantly",
    "in addition",
    "as such",
    "accordingly",
    "by contrast",
)


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


def _profile_writing_stats(chunks: list[MemoDocumentChunk]) -> dict:
    paragraph_lengths = []
    first_person_hits = 0
    uncertainty_hits = 0
    recommendation_hits = 0

    for chunk in chunks:
        text = chunk.text or ""
        paragraphs = [paragraph.strip() for paragraph in text.split("\n\n") if paragraph.strip()]
        paragraph_lengths.extend(len(paragraph.split()) for paragraph in paragraphs)
        lowered = text.lower()
        if " we " in f" {lowered} " or lowered.startswith("we "):
            first_person_hits += 1
        if any(token in lowered for token in ("unclear", "requires", "pending", "subject to", "we need", "open question")):
            uncertainty_hits += 1
        if any(token in lowered for token in ("recommend", "underwrite", "proceed", "decline", "invest")):
            recommendation_hits += 1

    avg_paragraph_words = round(sum(paragraph_lengths) / len(paragraph_lengths), 1) if paragraph_lengths else 0.0
    return {
        "avg_paragraph_words": avg_paragraph_words,
        "first_person_chunk_ratio": round(first_person_hits / len(chunks), 3) if chunks else 0.0,
        "uncertainty_chunk_ratio": round(uncertainty_hits / len(chunks), 3) if chunks else 0.0,
        "recommendation_chunk_ratio": round(recommendation_hits / len(chunks), 3) if chunks else 0.0,
    }


def _paragraphs(text: str) -> list[str]:
    return [paragraph.strip() for paragraph in (text or "").split("\n\n") if paragraph.strip()]


def _sentences(text: str) -> list[str]:
    return [sentence.strip() for sentence in SENTENCE_RE.split((text or "").strip()) if sentence.strip()]


def _tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall((text or "").lower())


def _top_phrases(chunks: list[MemoDocumentChunk], candidates: tuple[str, ...], limit: int = 4) -> list[str]:
    counter = Counter()
    for chunk in chunks:
        lowered = (chunk.text or "").lower()
        for phrase in candidates:
            if phrase in lowered:
                counter[phrase] += 1
    return [phrase for phrase, _count in counter.most_common(limit)]


def _top_sentence_starts(texts: list[str], limit: int = 3) -> list[str]:
    counter = Counter()
    for text in texts:
        sentences = _sentences(text)
        if not sentences:
            continue
        prefix = " ".join(_tokenize(sentences[0])[:4]).strip()
        if prefix:
            counter[prefix] += 1
    return [phrase for phrase, _count in counter.most_common(limit)]


def _top_sentence_endings(texts: list[str], limit: int = 3) -> list[str]:
    counter = Counter()
    for text in texts:
        sentences = _sentences(text)
        if not sentences:
            continue
        suffix_tokens = _tokenize(sentences[-1])[-4:]
        suffix = " ".join(suffix_tokens).strip()
        if suffix:
            counter[suffix] += 1
    return [phrase for phrase, _count in counter.most_common(limit)]


def _sentence_role(sentence: str, is_closing: bool = False) -> str:
    lowered = (sentence or "").lower()
    if any(phrase in lowered for phrase in RECOMMENDATION_PHRASES):
        return "recommendation_first" if not is_closing else "recommendation_close"
    if "?" in lowered or "open question" in lowered or "requires" in lowered or "pending" in lowered:
        return "question_first" if not is_closing else "open_issue_close"
    if any(token in lowered for token in ("however", "that said", "on balance", "therefore", "accordingly")):
        return "transition_first" if not is_closing else "qualified_close"
    return "evidence_first" if not is_closing else "evidence_close"


def _section_style_profile(section_key: str, chunks: list[MemoDocumentChunk]) -> dict:
    texts = [chunk.text or "" for chunk in chunks if (chunk.text or "").strip()]
    paragraph_counts = [len(_paragraphs(text)) for text in texts if _paragraphs(text)]
    paragraph_lengths = []
    first_person_hits = 0
    opening_roles = Counter()
    closing_roles = Counter()

    for text in texts:
        paragraphs = _paragraphs(text)
        paragraph_lengths.extend(len(paragraph.split()) for paragraph in paragraphs)
        lowered = text.lower()
        if " we " in f" {lowered} " or lowered.startswith("we "):
            first_person_hits += 1
        sentences = _sentences(text)
        if sentences:
            opening_roles[_sentence_role(sentences[0])] += 1
            closing_roles[_sentence_role(sentences[-1], is_closing=True)] += 1

    paragraph_count_target = int(round(median(paragraph_counts))) if paragraph_counts else 1
    avg_paragraph_words = round(sum(paragraph_lengths) / len(paragraph_lengths), 1) if paragraph_lengths else 0.0
    return {
        "section_key": section_key,
        "paragraph_count_target": paragraph_count_target,
        "avg_paragraph_words": avg_paragraph_words,
        "first_person_ratio": round(first_person_hits / len(texts), 3) if texts else 0.0,
        "opening_moves": [role for role, _count in opening_roles.most_common(2)],
        "closing_moves": [role for role, _count in closing_roles.most_common(2)],
        "opening_phrases": _top_sentence_starts(texts),
        "closing_phrases": _top_sentence_endings(texts),
        "hedge_phrases": _top_phrases(chunks, HEDGE_PHRASES),
        "recommendation_phrases": _top_phrases(chunks, RECOMMENDATION_PHRASES),
        "transition_phrases": _top_phrases(chunks, TRANSITION_PHRASES),
    }


def _deterministic_profile_payload(
    documents: list[MemoDocument],
    chunk_rows: list[MemoDocumentChunk],
    heading_variants: dict[str, Counter[str]],
    chunks_by_section: dict[str, list[MemoDocumentChunk]],
    section_order: list[str],
) -> dict:
    tone = _profile_writing_stats(chunk_rows)
    section_profiles = {
        section_key: _section_style_profile(section_key, rows)
        for section_key, rows in chunks_by_section.items()
    }
    return {
        "style_version": 2,
        "section_order": section_order,
        "heading_variants": {
            key: [heading for heading, _count in counter.most_common(5)]
            for key, counter in heading_variants.items()
        },
        "primary_headings": {
            key: (counter.most_common(1)[0][0] if counter else key.replace("_", " ").title())
            for key, counter in heading_variants.items()
        },
        "global_voice": {
            **tone,
            "preferred_person": "first_person_plural" if (tone.get("first_person_chunk_ratio") or 0) >= 0.2 else "neutral",
            "hedge_phrases": _top_phrases(chunk_rows, HEDGE_PHRASES),
            "recommendation_phrases": _top_phrases(chunk_rows, RECOMMENDATION_PHRASES),
            "transition_phrases": _top_phrases(chunk_rows, TRANSITION_PHRASES),
        },
        "tone": tone,
        "reasoning_patterns": {
            "uses_open_questions": any("open" in section_key or "question" in section_key for section_key in section_order),
            "uses_explicit_recommendation_section": "recommendation" in section_order,
            "section_count": len(section_order),
        },
        "section_profiles": section_profiles,
        "style_learning": {
            "source_document_count": len(documents),
            "deterministic_chunk_count": len(chunk_rows),
            "llm_enriched": False,
        },
    }


def _style_sections_for_prompt(chunks_by_section: dict[str, list[MemoDocumentChunk]]) -> list[dict]:
    sections = []
    for section_key, rows in sorted(chunks_by_section.items()):
        top_rows = sorted(rows, key=lambda row: len(row.text_delexicalized or row.text or ""), reverse=True)[:3]
        sections.append(
            {
                "section_key": section_key,
                "samples": [row.text_delexicalized or row.text or "" for row in top_rows if (row.text_delexicalized or row.text or "").strip()],
                "sample_count": len(rows),
                "paragraph_count_target": _section_style_profile(section_key, rows).get("paragraph_count_target"),
            }
        )
    return sections


def _clean_string_list(value, limit: int = 5) -> list[str]:
    if not isinstance(value, list):
        return []
    items = []
    seen = set()
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if not cleaned:
            continue
        normalized = cleaned.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        items.append(cleaned[:120])
        if len(items) >= limit:
            break
    return items


def _merge_style_profile_payload(profile_payload: dict, llm_payload: dict | None) -> dict:
    if not isinstance(llm_payload, dict):
        return profile_payload

    merged = dict(profile_payload)
    global_voice = dict(profile_payload.get("global_voice") or {})
    ai_global_voice = llm_payload.get("global_voice") if isinstance(llm_payload.get("global_voice"), dict) else {}
    for key in ("style_summary", "preferred_person"):
        value = ai_global_voice.get(key)
        if isinstance(value, str) and value.strip():
            global_voice[key] = value.strip()
    for key in ("opening_phrases", "closing_phrases", "hedge_phrases", "recommendation_phrases", "transition_phrases"):
        cleaned = _clean_string_list(ai_global_voice.get(key), limit=6)
        if cleaned:
            global_voice[key] = cleaned
    merged["global_voice"] = global_voice

    section_profiles = {
        key: dict(value)
        for key, value in (profile_payload.get("section_profiles") or {}).items()
        if isinstance(value, dict)
    }
    ai_section_profiles = llm_payload.get("section_profiles") if isinstance(llm_payload.get("section_profiles"), dict) else {}
    for section_key, ai_profile in ai_section_profiles.items():
        if not isinstance(ai_profile, dict):
            continue
        section_profile = dict(section_profiles.get(section_key) or {})
        for key in ("paragraph_count_target", "avg_paragraph_words", "first_person_ratio"):
            value = ai_profile.get(key)
            if isinstance(value, (int, float)):
                section_profile[key] = round(float(value), 3) if key == "first_person_ratio" else value
        for key in ("opening_moves", "closing_moves", "opening_phrases", "closing_phrases", "hedge_phrases", "recommendation_phrases", "transition_phrases"):
            cleaned = _clean_string_list(ai_profile.get(key), limit=6)
            if cleaned:
                section_profile[key] = cleaned
        if section_profile:
            section_profiles[section_key] = section_profile
    merged["section_profiles"] = section_profiles

    style_learning = dict(profile_payload.get("style_learning") or {})
    style_learning["llm_enriched"] = bool(ai_section_profiles or ai_global_voice)
    merged["style_learning"] = style_learning
    return merged


def _call_openai_json(model: str, prompt: dict) -> dict | None:
    return call_openai_json(model, prompt, logger)


def _select_exemplars(chunks_by_section: dict[str, list[MemoDocumentChunk]], top_n: int = 3) -> list[tuple[str, MemoDocumentChunk, int]]:
    selected = []
    for section_key, rows in chunks_by_section.items():
        ranked = sorted(rows, key=lambda row: len(row.text or ""), reverse=True)[:top_n]
        for rank, row in enumerate(ranked, start=1):
            selected.append((section_key, row, rank))
    return selected


def rebuild_style_profile(style_profile_id: int, document_ids: list[int] | None = None) -> MemoStyleProfile:
    profile = db.session.get(MemoStyleProfile, style_profile_id)
    if profile is None:
        raise ValueError(f"Style profile {style_profile_id} not found")

    query = MemoDocument.query.filter(
        MemoDocument.team_id == profile.team_id,
        MemoDocument.created_by_user_id == profile.created_by_user_id,
        MemoDocument.document_role.in_(sorted(STYLE_SOURCE_ROLES)),
        MemoDocument.status == "ready",
        MemoDocument.extraction_status == "ready",
    )
    if document_ids:
        query = query.filter(MemoDocument.id.in_(document_ids))
    documents = query.order_by(MemoDocument.created_at.asc(), MemoDocument.id.asc()).all()
    doc_ids = [document.id for document in documents]

    chunk_rows = []
    if doc_ids:
        chunk_rows = (
            MemoDocumentChunk.query.filter(
                MemoDocumentChunk.document_id.in_(doc_ids),
                MemoDocumentChunk.status == "ready",
            )
            .order_by(MemoDocumentChunk.document_id.asc(), MemoDocumentChunk.chunk_index.asc())
            .all()
        )

    section_counter: Counter[str] = Counter()
    heading_variants: dict[str, Counter[str]] = defaultdict(Counter)
    chunks_by_section: dict[str, list[MemoDocumentChunk]] = defaultdict(list)

    for chunk in chunk_rows:
        section_key = canonical_section_key(chunk.section_key) or "general"
        metadata = _json_loads(chunk.metadata_json, {})
        heading = (metadata.get("heading") or chunk.section_key or section_key).strip()
        section_counter[section_key] += 1
        if heading:
            heading_variants[section_key][heading] += 1
        chunks_by_section[section_key].append(chunk)

    section_order = [section_key for section_key, _count in section_counter.most_common()]
    if not section_order:
        section_order = ["executive_summary", "fund_overview", "performance_and_benchmarking", "risks_and_open_questions", "recommendation"]

    profile_payload = _deterministic_profile_payload(documents, chunk_rows, heading_variants, chunks_by_section, section_order)
    llm_prompt_sections = _style_sections_for_prompt(chunks_by_section)
    llm_profile_payload = None
    if llm_prompt_sections:
        llm_profile_payload = _call_openai_json(
            current_app.config["MEMO_LLM_MODEL_STYLE"],
            build_style_extraction_prompt(llm_prompt_sections, prior_profile=profile_payload),
        )
    profile_payload = _merge_style_profile_payload(profile_payload, llm_profile_payload)

    MemoStyleExemplar.query.filter_by(style_profile_id=profile.id).delete(synchronize_session=False)
    exemplar_rows = _select_exemplars(chunks_by_section)
    for section_key, chunk, rank in exemplar_rows:
        metadata = _json_loads(chunk.metadata_json, {})
        db.session.add(
            MemoStyleExemplar(
                style_profile_id=profile.id,
                document_id=chunk.document_id,
                section_key=section_key,
                heading_text=metadata.get("heading") or chunk.section_key,
                text_raw=chunk.text,
                text_delexicalized=chunk.text_delexicalized,
                embedding_json=chunk.embedding_json,
                rank=rank,
                status="ready",
            )
        )

    profile.profile_json = _json_dumps(profile_payload)
    profile.source_document_count = len(documents)
    profile.approved_exemplar_count = len(exemplar_rows)
    profile.status = "ready" if documents else "empty"
    db.session.add(profile)
    db.session.commit()
    return profile


def load_style_profile(profile_id: int) -> dict:
    profile = db.session.get(MemoStyleProfile, profile_id)
    if profile is None:
        raise ValueError(f"Style profile {profile_id} not found")
    payload = _json_loads(profile.profile_json, {})
    payload["id"] = profile.id
    payload["name"] = profile.name
    payload["status"] = profile.status
    return payload


def list_style_exemplars(profile_id: int, section_key: str | None = None) -> list[dict]:
    query = MemoStyleExemplar.query.filter_by(style_profile_id=profile_id, status="ready")
    if section_key:
        query = query.filter(MemoStyleExemplar.section_key == section_key)
    rows = query.order_by(MemoStyleExemplar.section_key.asc(), MemoStyleExemplar.rank.asc()).all()
    return [
        {
            "id": row.id,
            "document_id": row.document_id,
            "section_key": row.section_key,
            "heading_text": row.heading_text,
            "text_raw": row.text_raw,
            "text_delexicalized": row.text_delexicalized,
            "rank": row.rank,
        }
        for row in rows
    ]

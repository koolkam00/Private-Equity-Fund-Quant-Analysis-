from __future__ import annotations

import json
from collections import Counter, defaultdict

from models import MemoDocument, MemoDocumentChunk, MemoStyleExemplar, MemoStyleProfile, db
from peqa.services.memos.chunking import canonical_section_key


STYLE_SOURCE_ROLES = {"prior_memo", "approved_generated_memo"}


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

    profile_payload = {
        "section_order": section_order,
        "heading_variants": {
            key: [heading for heading, _count in counter.most_common(5)]
            for key, counter in heading_variants.items()
        },
        "primary_headings": {
            key: (counter.most_common(1)[0][0] if counter else key.replace("_", " ").title())
            for key, counter in heading_variants.items()
        },
        "tone": _profile_writing_stats(chunk_rows),
        "reasoning_patterns": {
            "uses_open_questions": any("open" in section_key or "question" in section_key for section_key in section_order),
            "uses_explicit_recommendation_section": "recommendation" in section_order,
            "section_count": len(section_order),
        },
    }

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

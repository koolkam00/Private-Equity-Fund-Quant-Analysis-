from __future__ import annotations

import math
import re

from models import MemoStyleExemplar
from peqa.services.memos.types import RetrievalItem, RetrievalPack


TOKEN_RE = re.compile(r"[a-z0-9]+")


def _tokenize(text: str) -> set[str]:
    return set(TOKEN_RE.findall((text or "").lower()))


def _score_text(query_tokens: set[str], text: str, section_key: str | None, target_section_key: str) -> float:
    tokens = _tokenize(text)
    if not tokens:
        return 0.0
    overlap = len(query_tokens & tokens)
    if overlap == 0:
        return 0.0
    score = overlap / math.sqrt(len(tokens))
    if section_key and section_key == target_section_key:
        score += 2.0
    return score


def retrieve_section_evidence(section_spec: dict, evidence_bundle, style_profile: dict, top_k: int = 6) -> RetrievalPack:
    section_key = section_spec["key"]
    query_tokens = _tokenize(f"{section_spec['title']} {section_spec.get('objective', '')}")
    items = []
    for snippet in evidence_bundle.document_snippets:
        score = _score_text(query_tokens, snippet.get("text", ""), snippet.get("section_key"), section_key)
        if score <= 0:
            continue
        items.append(
            RetrievalItem(
                source_type="document_chunk",
                source_id=str(snippet["id"]),
                label=f"Document {snippet.get('document_id')}",
                text=snippet.get("text", ""),
                page_start=snippet.get("page_start"),
                page_end=snippet.get("page_end"),
                score=score,
                metadata=snippet.get("metadata") or {},
            )
        )
    items.sort(key=lambda item: item.score, reverse=True)

    exemplar_rows = (
        MemoStyleExemplar.query.filter_by(style_profile_id=style_profile["id"], section_key=section_key, status="ready")
        .order_by(MemoStyleExemplar.rank.asc(), MemoStyleExemplar.id.asc())
        .limit(3)
        .all()
    )
    exemplars = [
        {
            "id": row.id,
            "section_key": row.section_key,
            "heading_text": row.heading_text,
            "text_raw": row.text_raw,
            "text_delexicalized": row.text_delexicalized,
            "rank": row.rank,
        }
        for row in exemplar_rows
    ]

    return RetrievalPack(
        section_key=section_key,
        items=items[:top_k],
        exemplars=exemplars,
    )

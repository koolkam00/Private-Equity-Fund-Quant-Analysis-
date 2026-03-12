from __future__ import annotations

import re

from peqa.services.memos.types import DraftSection, ValidationResult


NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?\b")


def _flatten_evidence_numbers(evidence_bundle) -> set[str]:
    values = set()
    structured = evidence_bundle.structured_facts or {}
    for value in structured.values():
        if isinstance(value, (int, float)):
            values.add(str(int(value)) if float(value).is_integer() else str(value))
    for snippet in evidence_bundle.document_snippets or []:
        for number in NUMBER_RE.findall(snippet.get("text") or ""):
            values.add(number)
    for item in (evidence_bundle.missing_data or []) + (evidence_bundle.conflicts or []) + (evidence_bundle.open_questions or []):
        text = item.get("message") or item.get("question") or ""
        for number in NUMBER_RE.findall(text):
            values.add(number)
    return values


def _paragraphs(text: str) -> list[str]:
    return [paragraph.strip() for paragraph in (text or "").split("\n\n") if paragraph.strip()]


def extract_claims(section_key: str, text: str, citations: list[dict]) -> list[dict]:
    citation_ids = [citation.get("id") for citation in citations if citation.get("id")]
    out = []
    for sentence in re.split(r"(?<=[\.\?\!])\s+", text.strip()):
        sentence = sentence.strip()
        if not sentence:
            continue
        claim_type = "numeric" if NUMBER_RE.search(sentence) else "synthesis"
        out.append(
            {
                "claim_type": claim_type,
                "claim_text": sentence,
                "citation_ids": citation_ids[:3],
                "provenance_type": "memo_section",
                "provenance_id": section_key,
            }
        )
    return out


def validate_section(section_draft: DraftSection, evidence_bundle) -> ValidationResult:
    citations = section_draft.citations or []
    claims = section_draft.claims or []
    paragraph_map = section_draft.paragraph_map or []
    known_citation_ids = {citation.get("id") for citation in citations if citation.get("id")}
    evidence_numbers = _flatten_evidence_numbers(evidence_bundle)

    unsupported_claims = []
    numeric_mismatches = []
    citation_gaps = []

    for claim in claims:
        claim_citation_ids = [citation_id for citation_id in claim.get("citation_ids") or [] if citation_id]
        if not claim_citation_ids:
            unsupported_claims.append(
                {
                    "claim_text": claim.get("claim_text"),
                    "reason": "No citations attached to claim.",
                }
            )
        elif not any(citation_id in known_citation_ids for citation_id in claim_citation_ids):
            unsupported_claims.append(
                {
                    "claim_text": claim.get("claim_text"),
                    "reason": "Claim cites unknown evidence ids.",
                }
            )
        if claim.get("claim_type") == "numeric":
            for number in NUMBER_RE.findall(claim.get("claim_text") or ""):
                if number not in evidence_numbers:
                    numeric_mismatches.append(
                        {
                            "claim_text": claim.get("claim_text"),
                            "number": number,
                            "reason": "Numeric value not found in structured facts or retrieved source material.",
                        }
                    )

    if paragraph_map:
        for paragraph in paragraph_map:
            if not (paragraph.get("citation_ids") or []):
                citation_gaps.append(
                    {
                        "paragraph_text": paragraph.get("text"),
                        "reason": "Paragraph has no supporting citations.",
                    }
                )
    else:
        for paragraph in _paragraphs(section_draft.text):
            citation_gaps.append(
                {
                    "paragraph_text": paragraph,
                    "reason": "Paragraph-level citation map is missing.",
                }
            )

    status = "blocked" if unsupported_claims or numeric_mismatches or citation_gaps else "ready"
    summary = "Section validation passed." if status == "ready" else "Section validation found unsupported content that requires review."
    return ValidationResult(
        status=status,
        unsupported_claims=unsupported_claims,
        numeric_mismatches=numeric_mismatches,
        citation_gaps=citation_gaps,
        open_questions=section_draft.open_questions or [],
        summary=summary,
    )

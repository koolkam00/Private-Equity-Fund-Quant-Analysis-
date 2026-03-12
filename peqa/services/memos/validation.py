from __future__ import annotations

import re
from difflib import SequenceMatcher

from peqa.services.memos.types import DraftSection, ValidationResult


NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?\b")
TOKEN_RE = re.compile(r"[a-z0-9']+")
RECOMMENDATION_TERMS = ("recommend", "approval", "proceed", "decline", "underwrite")
QUESTION_TERMS = ("open question", "pending", "requires", "outstanding", "unclear")


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


def _sentences(text: str) -> list[str]:
    return [sentence.strip() for sentence in re.split(r"(?<=[\.\?\!])\s+", (text or "").strip()) if sentence.strip()]


def _tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall((text or "").lower())


def _normalized_text(text: str) -> str:
    return " ".join(_tokenize(text))


def _contains_first_person(text: str) -> bool:
    lowered = (text or "").lower()
    return " we " in f" {lowered} " or lowered.startswith("we ") or " our " in f" {lowered} "


def _style_move(sentence: str, is_closing: bool = False) -> str:
    lowered = (sentence or "").lower()
    if any(term in lowered for term in RECOMMENDATION_TERMS):
        return "recommendation_close" if is_closing else "recommendation_first"
    if any(term in lowered for term in QUESTION_TERMS) or "?" in lowered:
        return "open_issue_close" if is_closing else "question_first"
    if any(term in lowered for term in ("however", "that said", "on balance", "accordingly", "therefore")):
        return "qualified_close" if is_closing else "transition_first"
    return "evidence_close" if is_closing else "evidence_first"


def _style_field_list(profile: dict, key: str) -> list[str]:
    values = profile.get(key)
    if not isinstance(values, list):
        return []
    return [item.strip().lower() for item in values if isinstance(item, str) and item.strip()]


def _section_style_profile(style_profile: dict | None, section_key: str) -> dict:
    if not isinstance(style_profile, dict):
        return {}
    section_profiles = style_profile.get("section_profiles")
    if not isinstance(section_profiles, dict):
        return {}
    value = section_profiles.get(section_key)
    return value if isinstance(value, dict) else {}


def validate_style_match(section_draft: DraftSection, style_profile: dict | None, retrieval_pack=None) -> dict:
    if not isinstance(style_profile, dict):
        return {"style_score": 1.0, "style_findings": [], "copy_risk_flags": []}

    text = section_draft.text or ""
    paragraphs = _paragraphs(text)
    paragraph_count = len(paragraphs)
    paragraph_lengths = [len(paragraph.split()) for paragraph in paragraphs]
    avg_paragraph_words = sum(paragraph_lengths) / len(paragraph_lengths) if paragraph_lengths else 0.0
    first_sentence = _sentences(text)[0] if _sentences(text) else ""
    last_sentence = _sentences(text)[-1] if _sentences(text) else ""

    global_voice = style_profile.get("global_voice") if isinstance(style_profile.get("global_voice"), dict) else {}
    section_profile = _section_style_profile(style_profile, section_draft.key)
    findings = []
    copy_risk_flags = []
    score = 1.0

    paragraph_target = section_profile.get("paragraph_count_target")
    if isinstance(paragraph_target, (int, float)) and paragraph_target > 0:
        paragraph_gap = abs(paragraph_count - int(round(paragraph_target)))
        if paragraph_gap >= 2:
            score -= min(0.3, 0.12 * paragraph_gap)
            findings.append(
                {
                    "type": "paragraph_count",
                    "expected": int(round(paragraph_target)),
                    "actual": paragraph_count,
                    "reason": "Paragraph count materially differs from the learned section rhythm.",
                }
            )

    target_avg_words = section_profile.get("avg_paragraph_words") or global_voice.get("avg_paragraph_words")
    if isinstance(target_avg_words, (int, float)) and target_avg_words:
        deviation_ratio = abs(avg_paragraph_words - float(target_avg_words)) / max(float(target_avg_words), 1.0)
        if deviation_ratio >= 0.4:
            score -= 0.1
            findings.append(
                {
                    "type": "paragraph_length",
                    "expected": round(float(target_avg_words), 1),
                    "actual": round(avg_paragraph_words, 1),
                    "reason": "Average paragraph length is outside the learned section range.",
                }
            )

    preferred_person = (global_voice.get("preferred_person") or "").strip().lower()
    if preferred_person == "first_person_plural" and not _contains_first_person(text):
        score -= 0.08
        findings.append(
            {
                "type": "voice",
                "reason": "The draft does not use the first-person plural voice seen in prior memos.",
            }
        )

    opening_moves = _style_field_list(section_profile, "opening_moves")
    if opening_moves:
        actual_opening_move = _style_move(first_sentence)
        if actual_opening_move not in opening_moves:
            score -= 0.08
            findings.append(
                {
                    "type": "opening_move",
                    "expected": opening_moves[0],
                    "actual": actual_opening_move,
                    "reason": "The section opens differently from the learned pattern.",
                }
            )

    closing_moves = _style_field_list(section_profile, "closing_moves")
    if closing_moves:
        actual_closing_move = _style_move(last_sentence, is_closing=True)
        if actual_closing_move not in closing_moves:
            score -= 0.08
            findings.append(
                {
                    "type": "closing_move",
                    "expected": closing_moves[0],
                    "actual": actual_closing_move,
                    "reason": "The section closes differently from the learned pattern.",
                }
            )

    recommendation_phrases = _style_field_list(section_profile, "recommendation_phrases") or _style_field_list(global_voice, "recommendation_phrases")
    if section_draft.key in {"executive_summary", "recommendation"} and recommendation_phrases:
        lowered = text.lower()
        if not any(phrase in lowered for phrase in recommendation_phrases):
            score -= 0.08
            findings.append(
                {
                    "type": "recommendation_phrase",
                    "reason": "The draft does not use the recommendation language seen in prior memos.",
                }
            )

    hedge_phrases = _style_field_list(section_profile, "hedge_phrases") or _style_field_list(global_voice, "hedge_phrases")
    if section_draft.key in {"risks_and_open_questions", "executive_summary"} and hedge_phrases:
        lowered = text.lower()
        if not any(phrase in lowered for phrase in hedge_phrases):
            score -= 0.08
            findings.append(
                {
                    "type": "hedge_phrase",
                    "reason": "The draft is missing the uncertainty framing usually used in this section.",
                }
            )

    exemplar_texts = []
    if retrieval_pack is not None:
        for exemplar in getattr(retrieval_pack, "exemplars", []) or []:
            exemplar_text = exemplar.get("text_raw") if isinstance(exemplar, dict) else None
            if exemplar_text:
                exemplar_texts.append(exemplar_text)
    normalized_draft = _normalized_text(text)
    for exemplar_text in exemplar_texts:
        normalized_exemplar = _normalized_text(exemplar_text)
        if len(normalized_draft) < 80 or len(normalized_exemplar) < 80:
            continue
        similarity = SequenceMatcher(None, normalized_draft, normalized_exemplar).ratio()
        if similarity >= 0.92:
            copy_risk_flags.append(
                {
                    "reason": "Draft is too close to a prior memo exemplar.",
                    "similarity": round(similarity, 3),
                }
            )
            score -= 0.25
            break

    score = max(0.0, round(score, 3))
    return {
        "style_score": score,
        "style_findings": findings,
        "copy_risk_flags": copy_risk_flags,
    }


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


def validate_section(section_draft: DraftSection, evidence_bundle, style_profile: dict | None = None, retrieval_pack=None) -> ValidationResult:
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

    style_result = validate_style_match(section_draft, style_profile, retrieval_pack)
    copy_risk_flags = style_result["copy_risk_flags"]

    status = "blocked" if unsupported_claims or numeric_mismatches or citation_gaps or copy_risk_flags else "ready"
    if status == "ready":
        summary = "Section validation passed."
    else:
        summary = "Section validation found unsupported content that requires review."
    if style_result["style_findings"] and status == "ready":
        summary = "Section validation passed, but style drift was detected against prior memos."
    return ValidationResult(
        status=status,
        unsupported_claims=unsupported_claims,
        numeric_mismatches=numeric_mismatches,
        citation_gaps=citation_gaps,
        open_questions=section_draft.open_questions or [],
        style_score=style_result["style_score"],
        style_findings=style_result["style_findings"],
        copy_risk_flags=copy_risk_flags,
        summary=summary,
    )

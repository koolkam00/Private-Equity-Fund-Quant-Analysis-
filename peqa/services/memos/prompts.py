from __future__ import annotations

from peqa.services.memos.types import SectionSpec


DEFAULT_MEMO_SECTIONS = [
    SectionSpec(
        key="executive_summary",
        title="Executive Summary",
        objective="State the bottom-line recommendation, strongest supporting evidence, and key reservations.",
        required_evidence=["structured_facts", "analysis_summaries", "open_questions"],
    ),
    SectionSpec(
        key="fund_overview",
        title="Fund Overview",
        objective="Summarize the manager, fund mandate, strategy, scope, and fit within the current diligence context.",
        required_evidence=["structured_facts", "document_snippets"],
    ),
    SectionSpec(
        key="performance_and_benchmarking",
        title="Performance and Benchmarking",
        objective="Explain the fund's performance, benchmark position, and the reliability of the benchmark comparison.",
        required_evidence=["structured_facts", "analysis_summaries", "benchmark_context"],
    ),
    SectionSpec(
        key="liquidity_and_realization",
        title="Liquidity and Realization",
        objective="Assess DPI, unrealized dependence, liquidity timing, and realization risk.",
        required_evidence=["analysis_summaries", "structured_facts", "document_snippets"],
    ),
    SectionSpec(
        key="risks_and_open_questions",
        title="Risks and Open Questions",
        objective="List missing data, conflicting figures, reporting concerns, and unresolved diligence questions without guessing.",
        required_evidence=["missing_data", "conflicts", "open_questions"],
    ),
    SectionSpec(
        key="recommendation",
        title="Recommendation",
        objective="Provide a concise recommendation with explicit rationale, conditions, and next diligence steps.",
        required_evidence=["structured_facts", "analysis_summaries", "open_questions"],
    ),
]


def build_style_extraction_prompt(delexicalized_sections: list[dict], prior_profile: dict | None = None) -> dict:
    return {
        "system": (
            "Extract memo-writing style as strict JSON. Focus on structure, heading patterns, paragraph rhythm, "
            "section-level opening and closing moves, transition language, recommendation phrasing, how uncertainty is "
            "expressed, and how risks are framed. Return only style instructions and never infer fund facts."
        ),
        "input": {
            "prior_profile": prior_profile or {},
            "sections": delexicalized_sections,
        },
    }


def build_outline_prompt(style_profile: dict, evidence_summary: dict) -> dict:
    return {
        "system": (
            "Produce a strict JSON memo outline that follows the learned section order while surfacing missing data, "
            "conflicts, and open diligence questions. Do not draft prose."
        ),
        "input": {
            "style_profile": style_profile,
            "evidence_summary": evidence_summary,
        },
    }


def build_section_drafting_prompt(section_spec: dict, style_profile: dict, retrieval_pack: dict, evidence_bundle: dict) -> dict:
    return {
        "system": (
            "Draft one memo section as a grounded base draft in strict JSON with text, citations, explicit open questions, "
            "and claim objects. Use only supplied evidence. Distinguish facts, calculations, and synthesis. Keep the prose "
            "clean and direct; do not optimize for stylistic mimicry yet."
        ),
        "input": {
            "section_spec": section_spec,
            "style_profile": style_profile,
            "retrieval_pack": retrieval_pack,
            "evidence_bundle": evidence_bundle,
        },
    }


def build_style_rewrite_prompt(
    section_spec: dict,
    style_profile: dict,
    section_profile: dict,
    exemplars: list[dict],
    grounded_draft: dict,
) -> dict:
    return {
        "system": (
            "Rewrite the supplied grounded memo section into the user's memo style as strict JSON. Preserve every fact, "
            "number, recommendation stance, open question, and citation id. Improve only wording, transitions, emphasis, "
            "and paragraph structure. Do not copy exemplar language verbatim."
        ),
        "input": {
            "section_spec": section_spec,
            "style_profile": style_profile,
            "section_profile": section_profile,
            "exemplars": exemplars,
            "grounded_draft": grounded_draft,
        },
    }


def build_validation_prompt(section_draft: dict, evidence_bundle: dict) -> dict:
    return {
        "system": (
            "Validate the memo section as strict JSON. Flag unsupported claims, numeric mismatches, citation gaps, "
            "and unresolved conflicts. Never repair the draft silently."
        ),
        "input": {
            "section_draft": section_draft,
            "evidence_bundle": evidence_bundle,
        },
    }


def build_final_assembly_prompt(style_profile: dict, sections: list[dict]) -> dict:
    return {
        "system": (
            "Assemble final markdown from validated sections only. Preserve section order and style. Do not invent new claims."
        ),
        "input": {
            "style_profile": style_profile,
            "sections": sections,
        },
    }

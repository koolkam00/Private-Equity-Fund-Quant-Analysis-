from peqa.services.memos.types import DraftSection, MemoEvidenceBundle
from peqa.services.memos.validation import validate_section


def test_validate_section_flags_missing_citations():
    draft = DraftSection(
        key="executive_summary",
        title="Executive Summary",
        text="The fund count is 2.",
        citations=[],
        claims=[{"claim_type": "numeric", "claim_text": "The fund count is 2.", "citation_ids": []}],
        paragraph_map=[{"text": "The fund count is 2.", "citation_ids": []}],
    )
    evidence = MemoEvidenceBundle(
        run_id=1,
        firm_id=1,
        team_id=1,
        filters={},
        benchmark_asset_class="Buyout",
        reporting_currency_context={},
        structured_facts={"fund_count": 2},
    )

    result = validate_section(draft, evidence)
    assert result.status == "blocked"
    assert result.unsupported_claims
    assert result.citation_gaps
    assert result.style_score == 1.0

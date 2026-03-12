from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class ExtractedPage:
    page_number: int
    text: str


@dataclass
class ExtractedDocument:
    document_id: int
    file_name: str
    mime_type: str
    text: str
    pages: list[ExtractedPage] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def page_count(self) -> int:
        return len(self.pages) if self.pages else (1 if self.text.strip() else 0)


@dataclass
class Chunk:
    chunk_index: int
    text: str
    text_delexicalized: str
    section_key: str | None
    page_start: int | None
    page_end: int | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SectionSpec:
    key: str
    title: str
    objective: str
    required_evidence: list[str] = field(default_factory=list)


@dataclass
class RetrievalItem:
    source_type: str
    source_id: str
    label: str
    text: str
    page_start: int | None = None
    page_end: int | None = None
    score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RetrievalPack:
    section_key: str
    items: list[RetrievalItem] = field(default_factory=list)
    exemplars: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class MemoEvidenceBundle:
    run_id: int
    firm_id: int
    team_id: int
    filters: dict[str, Any]
    benchmark_asset_class: str
    reporting_currency_context: dict[str, Any]
    structured_facts: dict[str, Any] = field(default_factory=dict)
    analysis_summaries: dict[str, Any] = field(default_factory=dict)
    document_snippets: list[dict[str, Any]] = field(default_factory=list)
    missing_data: list[dict[str, Any]] = field(default_factory=list)
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    open_questions: list[dict[str, Any]] = field(default_factory=list)
    benchmark_context: dict[str, Any] = field(default_factory=dict)


@dataclass
class DraftSection:
    key: str
    title: str
    text: str
    citations: list[dict[str, Any]] = field(default_factory=list)
    claims: list[dict[str, Any]] = field(default_factory=list)
    open_questions: list[dict[str, Any]] = field(default_factory=list)
    paragraph_map: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    status: str
    unsupported_claims: list[dict[str, Any]] = field(default_factory=list)
    numeric_mismatches: list[dict[str, Any]] = field(default_factory=list)
    citation_gaps: list[dict[str, Any]] = field(default_factory=list)
    open_questions: list[dict[str, Any]] = field(default_factory=list)
    summary: str = ""


@dataclass
class FinalMemoArtifact:
    markdown: str
    html: str
    sections: list[dict[str, Any]] = field(default_factory=list)


def dataclass_to_dict(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    return value

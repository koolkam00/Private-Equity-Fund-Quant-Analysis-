from __future__ import annotations

from html import escape

from peqa.services.memos.types import FinalMemoArtifact


def _paragraphs(text: str) -> list[str]:
    return [paragraph.strip() for paragraph in (text or "").split("\n\n") if paragraph.strip()]


def assemble_memo(style_profile: dict, sections: list[dict]) -> FinalMemoArtifact:
    markdown_parts = []
    html_parts = ['<article class="memo-article">']
    normalized_sections = []

    for section in sections:
        title = section.get("title") or section.get("section_key", "").replace("_", " ").title()
        draft_text = section.get("draft_text") or ""
        markdown_parts.append(f"## {title}\n\n{draft_text}".strip())

        html_parts.append(f"<section class=\"memo-section\"><h2>{escape(title)}</h2>")
        for paragraph in _paragraphs(draft_text):
            html_parts.append(f"<p>{escape(paragraph)}</p>")
        html_parts.append("</section>")

        normalized_sections.append(
            {
                "section_key": section.get("section_key"),
                "title": title,
                "draft_text": draft_text,
                "validation": section.get("validation"),
                "review_status": section.get("review_status"),
            }
        )

    html_parts.append("</article>")
    return FinalMemoArtifact(
        markdown="\n\n".join(part for part in markdown_parts if part).strip(),
        html="".join(html_parts),
        sections=normalized_sections,
    )

from __future__ import annotations

import re

from peqa.services.memos.delexicalize import delexicalize_text
from peqa.services.memos.types import Chunk, ExtractedDocument


HEADING_RE = re.compile(r"^\s*(?:\d+[\.\)]\s+)?([A-Z][A-Za-z/&,\- ]{2,80})\s*$")


def canonical_section_key(heading: str | None) -> str | None:
    value = (heading or "").strip().lower()
    if not value:
        return None
    value = re.sub(r"[^a-z0-9]+", "_", value).strip("_")
    return value or None


def detect_sections(text: str) -> list[tuple[str | None, str]]:
    lines = (text or "").splitlines()
    sections: list[tuple[str | None, str]] = []
    current_heading: str | None = None
    current_lines: list[str] = []

    def flush():
        if current_lines:
            sections.append((current_heading, "\n".join(current_lines).strip()))

    for line in lines:
        match = HEADING_RE.match(line.strip())
        if match and len(line.strip().split()) <= 8:
            flush()
            current_heading = match.group(1).strip()
            current_lines = []
            continue
        current_lines.append(line)

    flush()
    return [(heading, body) for heading, body in sections if body]


def chunk_document(extracted: ExtractedDocument, max_chars: int = 1800) -> list[Chunk]:
    sections = detect_sections(extracted.text)
    if not sections:
        sections = [(None, extracted.text or "")]

    chunks: list[Chunk] = []
    chunk_index = 0
    page_count = extracted.page_count or None

    for heading, body in sections:
        section_key = canonical_section_key(heading)
        paragraphs = [paragraph.strip() for paragraph in re.split(r"\n\s*\n", body) if paragraph.strip()]
        current: list[str] = []
        current_len = 0
        for paragraph in paragraphs:
            if current and current_len + len(paragraph) > max_chars:
                text = "\n\n".join(current).strip()
                chunks.append(
                    Chunk(
                        chunk_index=chunk_index,
                        text=text,
                        text_delexicalized=delexicalize_text(text),
                        section_key=section_key,
                        page_start=1 if page_count else None,
                        page_end=page_count,
                        metadata={"heading": heading},
                    )
                )
                chunk_index += 1
                current = [paragraph]
                current_len = len(paragraph)
            else:
                current.append(paragraph)
                current_len += len(paragraph)
        if current:
            text = "\n\n".join(current).strip()
            chunks.append(
                Chunk(
                    chunk_index=chunk_index,
                    text=text,
                    text_delexicalized=delexicalize_text(text),
                    section_key=section_key,
                    page_start=1 if page_count else None,
                    page_end=page_count,
                    metadata={"heading": heading},
                )
            )
            chunk_index += 1
    return chunks

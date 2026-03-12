from __future__ import annotations

from io import BytesIO
from pathlib import Path

from peqa.services.memos.types import ExtractedDocument, ExtractedPage


def _extract_text_pdf(file_bytes: bytes) -> list[ExtractedPage]:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("pypdf is required to extract PDF memo documents") from exc

    try:
        reader = PdfReader(BytesIO(file_bytes))
    except Exception as exc:
        raise RuntimeError(
            "Unable to read this PDF. If it is encrypted, password-protected, or uses unsupported security settings, "
            "export an unlocked PDF and upload it again."
        ) from exc

    try:
        if getattr(reader, "is_encrypted", False):
            try:
                decrypt_result = reader.decrypt("")
            except Exception as exc:
                raise RuntimeError(
                    "This PDF appears to be encrypted. Upload an unlocked PDF or a version that can be opened without a password."
                ) from exc
            if decrypt_result == 0:
                raise RuntimeError(
                    "This PDF is password-protected. Upload an unlocked PDF or a version that can be opened without a password."
                )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            "Unable to read this PDF. If it is encrypted, password-protected, or uses unsupported security settings, "
            "export an unlocked PDF and upload it again."
        ) from exc

    pages = []
    for index, page in enumerate(reader.pages, start=1):
        pages.append(ExtractedPage(page_number=index, text=(page.extract_text() or "").strip()))
    return pages


def _extract_text_docx(file_bytes: bytes) -> list[ExtractedPage]:
    try:
        from docx import Document
    except ImportError as exc:
        raise RuntimeError("python-docx is required to extract DOCX memo documents") from exc

    document = Document(BytesIO(file_bytes))
    text = "\n".join(paragraph.text for paragraph in document.paragraphs if paragraph.text)
    return [ExtractedPage(page_number=1, text=text)]


def _extract_text_pptx(file_bytes: bytes) -> list[ExtractedPage]:
    try:
        from pptx import Presentation
    except ImportError as exc:
        raise RuntimeError("python-pptx is required to extract PPTX memo documents") from exc

    deck = Presentation(BytesIO(file_bytes))
    pages = []
    for index, slide in enumerate(deck.slides, start=1):
        texts = []
        for shape in slide.shapes:
            if hasattr(shape, "text") and shape.text:
                texts.append(shape.text)
        pages.append(ExtractedPage(page_number=index, text="\n".join(texts).strip()))
    return pages


def extract_document(document, storage) -> ExtractedDocument:
    file_bytes = storage.get(document.storage_key)
    suffix = Path(document.file_name).suffix.lower()
    if suffix in {".txt", ".md"}:
        text = file_bytes.decode("utf-8", errors="ignore")
        pages = [ExtractedPage(page_number=1, text=text)]
    elif suffix == ".pdf":
        pages = _extract_text_pdf(file_bytes)
        text = "\n\n".join(page.text for page in pages if page.text)
    elif suffix == ".docx":
        pages = _extract_text_docx(file_bytes)
        text = "\n\n".join(page.text for page in pages if page.text)
    elif suffix == ".pptx":
        pages = _extract_text_pptx(file_bytes)
        text = "\n\n".join(page.text for page in pages if page.text)
    else:
        text = file_bytes.decode("utf-8", errors="ignore")
        pages = [ExtractedPage(page_number=1, text=text)]

    return ExtractedDocument(
        document_id=document.id,
        file_name=document.file_name,
        mime_type=document.mime_type,
        text=text.strip(),
        pages=pages,
        metadata={"suffix": suffix},
    )

from io import BytesIO

from models import MemoDocument, MemoDocumentChunk
from peqa.services.memos.types import ExtractedPage


def test_memo_upload_limit_does_not_exceed_global_request_limit(app_context):
    assert app_context.config["MAX_CONTENT_LENGTH"] >= app_context.config["MEMO_MAX_DOCUMENT_MB"] * 1024 * 1024


def test_memo_document_upload_creates_chunks(client):
    response = client.post(
        "/api/memos/documents",
        data={
            "document_role": "prior_memo",
            "file": (BytesIO(b"Executive Summary\n\nWe recommend proceeding.\n\nRisks\n\nOpen item remains."), "memo.txt"),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 201
    payload = response.get_json()
    assert payload["status"] == "ready"
    assert payload["extraction_status"] == "ready"

    document = MemoDocument.query.get(payload["id"])
    assert document is not None
    assert MemoDocumentChunk.query.filter_by(document_id=document.id).count() >= 1


def test_memo_document_upload_returns_failed_document_instead_of_500(client, monkeypatch):
    def fail_extract(*_args, **_kwargs):
        raise RuntimeError("simulated pdf extraction failure")

    monkeypatch.setattr("peqa.services.memos.jobs.extract_document", fail_extract)

    response = client.post(
        "/api/memos/documents",
        data={
            "document_role": "prior_memo",
            "file": (BytesIO(b"%PDF-1.7\nbroken"), "memo.pdf"),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 201
    payload = response.get_json()
    assert payload["status"] == "failed"
    assert payload["extraction_status"] == "failed"
    assert "simulated pdf extraction failure" in payload["error_text"]


def test_scanned_pdf_uses_ocr_when_enabled(app_context, monkeypatch):
    from peqa.services.memos import extractors

    app_context.config["MEMO_ENABLE_OCR"] = True
    app_context.config["MEMO_OCR_MAX_PAGES"] = 5
    app_context.config["MEMO_OCR_MIN_PAGE_TEXT_CHARS"] = 10
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    monkeypatch.setattr(
        extractors,
        "_extract_pdf_pages_native",
        lambda _file_bytes: [
            ExtractedPage(page_number=1, text=""),
            ExtractedPage(page_number=2, text="Already extracted from PDF text layer."),
        ],
    )
    monkeypatch.setattr(
        extractors,
        "_ocr_pdf_pages",
        lambda _file_bytes, page_numbers: {page_number: f"OCR text for page {page_number}" for page_number in page_numbers},
    )

    pages, metadata = extractors._extract_text_pdf(b"%PDF-1.7\nscanned")

    assert [page.text for page in pages] == [
        "OCR text for page 1",
        "Already extracted from PDF text layer.",
    ]
    assert metadata["ocr_attempted"] is True
    assert metadata["ocr_page_numbers"] == [1]
    assert metadata["ocr_completed_pages"] == 1


def test_scanned_pdf_without_ocr_raises_clear_error(app_context, monkeypatch):
    from peqa.services.memos import extractors

    app_context.config["MEMO_ENABLE_OCR"] = False
    monkeypatch.setattr(
        extractors,
        "_extract_pdf_pages_native",
        lambda _file_bytes: [ExtractedPage(page_number=1, text=""), ExtractedPage(page_number=2, text="")],
    )

    try:
        extractors._extract_text_pdf(b"%PDF-1.7\nscanned")
        assert False, "Expected OCR-disabled scanned PDF extraction to fail"
    except RuntimeError as exc:
        assert "enable memo OCR" in str(exc)


def test_scanned_pdf_respects_ocr_page_cap(app_context, monkeypatch):
    from peqa.services.memos import extractors

    app_context.config["MEMO_ENABLE_OCR"] = True
    app_context.config["MEMO_OCR_MAX_PAGES"] = 1
    app_context.config["MEMO_OCR_MIN_PAGE_TEXT_CHARS"] = 10
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    monkeypatch.setattr(
        extractors,
        "_extract_pdf_pages_native",
        lambda _file_bytes: [ExtractedPage(page_number=1, text=""), ExtractedPage(page_number=2, text="")],
    )

    try:
        extractors._extract_text_pdf(b"%PDF-1.7\nscanned")
        assert False, "Expected OCR page cap to fail"
    except RuntimeError as exc:
        assert "MEMO_OCR_MAX_PAGES=1" in str(exc)

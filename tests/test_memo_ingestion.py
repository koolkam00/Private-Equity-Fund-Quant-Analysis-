from io import BytesIO

from models import MemoDocument, MemoDocumentChunk


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

from io import BytesIO

from models import MemoDocument, MemoDocumentChunk


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

from models import MemoStoredBlob
from peqa.services.memos.storage import get_document_storage


def test_database_document_storage_round_trip(app_context):
    app_context.config["MEMO_STORAGE_BACKEND"] = "db"
    storage = get_document_storage(app_context.config)

    key = "memo-documents/test/blob.txt"
    payload = b"hello memo storage"

    storage.put(payload, key)
    assert storage.get(key) == payload

    row = MemoStoredBlob.query.filter_by(storage_key=key).first()
    assert row is not None
    assert row.size_bytes == len(payload)

    storage.delete(key)
    assert MemoStoredBlob.query.filter_by(storage_key=key).first() is None

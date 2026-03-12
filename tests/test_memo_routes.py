from io import BytesIO

from models import MemoDocument, MemoStyleProfile, db


def test_memo_pages_render(client):
    response = client.get("/memos")
    assert response.status_code == 200
    assert b"AI Memo Studio" in response.data
    assert b"Learn Your Style" in response.data
    assert b"Generate and Review" in response.data
    assert b"Dashboard benchmark" in response.data

    response = client.get("/memos/style-library")
    assert response.status_code == 200
    assert b"Style Library" in response.data
    assert b"Build a stronger style corpus for the memo engine" in response.data

    response = client.get("/memos/source-library")
    assert response.status_code == 200
    assert b"Source Library" in response.data
    assert b"Curate the diligence pack that grounds each memo run" in response.data


def test_memo_document_delete_archives_document(client):
    upload_response = client.post(
        "/api/memos/documents",
        data={
            "document_role": "ddq",
            "file": (BytesIO(b"Fund Overview\n\nGrounding text"), "ddq.txt"),
        },
        content_type="multipart/form-data",
    )
    assert upload_response.status_code == 201
    document_id = upload_response.get_json()["id"]

    delete_response = client.delete(f"/api/memos/documents/{document_id}")
    assert delete_response.status_code == 200
    assert delete_response.get_json()["status"] == "deleted"

    document = db.session.get(MemoDocument, document_id)
    assert document is not None
    assert document.status == "deleted"
    assert document.extraction_status == "deleted"

    list_response = client.get("/api/memos/documents")
    assert list_response.status_code == 200
    assert all(item["id"] != document_id for item in list_response.get_json()["items"])


def test_memo_style_profile_delete_archives_profile(client):
    upload_response = client.post(
        "/api/memos/documents",
        data={
            "document_role": "prior_memo",
            "file": (BytesIO(b"Executive Summary\n\nProceed."), "memo.txt"),
        },
        content_type="multipart/form-data",
    )
    assert upload_response.status_code == 201

    profile_response = client.post("/api/memos/style-profiles/rebuild", json={"name": "Delete Me"})
    assert profile_response.status_code == 201
    profile_id = profile_response.get_json()["id"]

    delete_response = client.delete(f"/api/memos/style-profiles/{profile_id}")
    assert delete_response.status_code == 200
    assert delete_response.get_json()["status"] == "deleted"

    profile = db.session.get(MemoStyleProfile, profile_id)
    assert profile is not None
    assert profile.status == "deleted"

    list_response = client.get("/api/memos/style-profiles")
    assert list_response.status_code == 200
    assert all(item["id"] != profile_id for item in list_response.get_json()["items"])

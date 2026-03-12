from io import BytesIO

from models import MemoStyleExemplar, MemoStyleProfile


def test_rebuild_style_profile_from_prior_memo(client):
    upload_response = client.post(
        "/api/memos/documents",
        data={
            "document_role": "prior_memo",
            "file": (
                BytesIO(
                    b"Executive Summary\n\nWe recommend proceeding with diligence.\n\n"
                    b"Risks and Open Questions\n\nKey DDQ response remains outstanding."
                ),
                "prior_memo.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert upload_response.status_code == 201

    rebuild_response = client.post(
        "/api/memos/style-profiles/rebuild",
        json={"name": "My Memo Style"},
    )
    assert rebuild_response.status_code == 201
    payload = rebuild_response.get_json()
    assert payload["status"] == "ready"
    assert payload["source_document_count"] == 1

    profile = MemoStyleProfile.query.get(payload["id"])
    assert profile is not None
    assert MemoStyleExemplar.query.filter_by(style_profile_id=profile.id).count() >= 1

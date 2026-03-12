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


def test_rebuild_style_profile_captures_section_profiles(client, monkeypatch):
    monkeypatch.setattr(
        "peqa.services.memos.style_profiles._call_openai_json",
        lambda model, prompt: {
            "global_voice": {
                "style_summary": "Leads with a recommendation, then frames the reservation explicitly.",
            },
            "section_profiles": {
                "executive_summary": {
                    "opening_moves": ["recommendation_first"],
                    "closing_moves": ["open_issue_close"],
                    "transition_phrases": ["that said"],
                }
            },
        },
    )

    upload_response = client.post(
        "/api/memos/documents",
        data={
            "document_role": "prior_memo",
            "file": (
                BytesIO(
                    b"Executive Summary\n\nWe recommend proceeding, subject to final legal diligence.\n\n"
                    b"That said, one DDQ item remains outstanding.\n\n"
                    b"Recommendation\n\nWe recommend approval after the reserve policy is confirmed."
                ),
                "prior_memo.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert upload_response.status_code == 201

    rebuild_response = client.post(
        "/api/memos/style-profiles/rebuild",
        json={"name": "My Rich Memo Style"},
    )
    assert rebuild_response.status_code == 201
    payload = rebuild_response.get_json()
    profile = payload["profile"]

    assert profile["style_version"] == 2
    assert profile["global_voice"]["style_summary"] == "Leads with a recommendation, then frames the reservation explicitly."
    assert profile["section_profiles"]["executive_summary"]["opening_moves"][0] == "recommendation_first"
    assert "subject to" in profile["global_voice"]["hedge_phrases"]
    assert profile["style_learning"]["llm_enriched"] is True

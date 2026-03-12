from datetime import date
from io import BytesIO

from models import (
    BenchmarkPoint,
    Deal,
    Firm,
    FundCashflow,
    FundMetadata,
    FundQuarterSnapshot,
    MemoDocument,
    MemoJob,
    MemoGenerationRun,
    MemoStyleProfile,
    PublicMarketIndexLevel,
    Team,
    TeamFirmAccess,
    TeamMembership,
    User,
    db,
)
from peqa.services.memos.jobs import _fail_job


def _seed_generation_data():
    membership = TeamMembership.query.first()
    access = TeamFirmAccess.query.filter_by(team_id=membership.team_id).first()
    deal = Deal(
        company_name="MemoCo",
        fund_number="Fund Memo",
        team_id=membership.team_id,
        firm_id=access.firm_id,
        status="Unrealized",
        investment_date=date(2021, 1, 1),
        equity_invested=100,
        realized_value=20,
        unrealized_value=120,
        net_irr=0.14,
        net_moic=1.5,
        net_dpi=0.4,
    )
    db.session.add(deal)
    db.session.flush()
    db.session.add_all(
        [
            FundMetadata(
                team_id=membership.team_id,
                firm_id=access.firm_id,
                fund_number="Fund Memo",
                vintage_year=2021,
                strategy="Buyout",
                manager_name="Memo Manager",
                benchmark_peer_group="BUYOUT_IDX",
            ),
            FundQuarterSnapshot(
                fund_number="Fund Memo",
                team_id=membership.team_id,
                firm_id=access.firm_id,
                quarter_end=date(2025, 12, 31),
                paid_in_capital=100,
                distributed_capital=40,
                nav=120,
            ),
            FundCashflow(
                team_id=membership.team_id,
                firm_id=access.firm_id,
                fund_number="Fund Memo",
                event_date=date(2021, 1, 15),
                event_type="Capital Call",
                amount=-100,
                nav_after_event=100,
                currency_code="USD",
            ),
            PublicMarketIndexLevel(
                team_id=membership.team_id,
                benchmark_code="BUYOUT_IDX",
                level_date=date(2021, 1, 15),
                level=1000,
                currency_code="USD",
                source="Fixture",
            ),
            PublicMarketIndexLevel(
                team_id=membership.team_id,
                benchmark_code="BUYOUT_IDX",
                level_date=date(2025, 12, 31),
                level=1450,
                currency_code="USD",
                source="Fixture",
            ),
            BenchmarkPoint(team_id=membership.team_id, asset_class="Buyout", vintage_year=2021, metric="net_irr", quartile="lower_quartile", value=0.08),
            BenchmarkPoint(team_id=membership.team_id, asset_class="Buyout", vintage_year=2021, metric="net_irr", quartile="median", value=0.12),
            BenchmarkPoint(team_id=membership.team_id, asset_class="Buyout", vintage_year=2021, metric="net_irr", quartile="upper_quartile", value=0.16),
            BenchmarkPoint(team_id=membership.team_id, asset_class="Buyout", vintage_year=2021, metric="net_moic", quartile="lower_quartile", value=1.1),
            BenchmarkPoint(team_id=membership.team_id, asset_class="Buyout", vintage_year=2021, metric="net_moic", quartile="median", value=1.4),
            BenchmarkPoint(team_id=membership.team_id, asset_class="Buyout", vintage_year=2021, metric="net_moic", quartile="upper_quartile", value=1.7),
            BenchmarkPoint(team_id=membership.team_id, asset_class="Buyout", vintage_year=2021, metric="net_dpi", quartile="lower_quartile", value=0.5),
            BenchmarkPoint(team_id=membership.team_id, asset_class="Buyout", vintage_year=2021, metric="net_dpi", quartile="median", value=0.7),
            BenchmarkPoint(team_id=membership.team_id, asset_class="Buyout", vintage_year=2021, metric="net_dpi", quartile="upper_quartile", value=0.9),
        ]
    )
    db.session.commit()


def test_memo_generation_run_end_to_end(client):
    _seed_generation_data()

    prior_memo = client.post(
        "/api/memos/documents",
        data={
            "document_role": "prior_memo",
            "file": (
                BytesIO(
                    b"Executive Summary\n\nWe recommend proceeding with diligence.\n\n"
                    b"Recommendation\n\nWe recommend approval after closing the remaining diligence points."
                ),
                "prior_memo.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert prior_memo.status_code == 201

    style_profile = client.post("/api/memos/style-profiles/rebuild", json={"name": "Memo Style"})
    assert style_profile.status_code == 201
    style_profile_id = style_profile.get_json()["id"]

    source_doc = client.post(
        "/api/memos/documents",
        data={
            "document_role": "ddq",
            "file": (
                BytesIO(
                    b"Fund Overview\n\nFund Memo targets North America buyout opportunities.\n\n"
                    b"Risks\n\nOperational diligence is still underway."
                ),
                "ddq.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert source_doc.status_code == 201
    source_doc_id = source_doc.get_json()["id"]

    run_response = client.post(
        "/api/memos/runs",
        json={
            "style_profile_id": style_profile_id,
            "benchmark_asset_class": "Buyout",
            "document_ids": [source_doc_id],
            "memo_type": "fund_investment",
            "filters": {},
        },
    )
    assert run_response.status_code == 201
    run_payload = run_response.get_json()
    assert run_payload["status"] == "review_required"
    assert run_payload["style_profile_name"] == "Memo Style"
    assert run_payload["source_document_count"] == 1
    assert run_payload["latest_job_label"]

    run = MemoGenerationRun.query.get(run_payload["id"])
    assert run is not None
    assert run.final_markdown

    sections_response = client.get(f"/api/memos/runs/{run.id}/sections")
    assert sections_response.status_code == 200
    sections_payload = sections_response.get_json()
    assert len(sections_payload["items"]) >= 1
    for section in sections_payload["items"]:
        assert "citation_count" in section
        assert "open_question_count" in section
        assert "review_status_tone" in section

    for section in sections_payload["items"]:
        review_response = client.post(
            f"/api/memos/runs/{run.id}/sections/{section['section_key']}/review",
            json={"review_status": "reviewed"},
        )
        assert review_response.status_code == 200

    approve_response = client.post(f"/api/memos/runs/{run.id}/approve")
    assert approve_response.status_code == 200
    assert approve_response.get_json()["status"] == "approved"

    export_response = client.post(f"/api/memos/runs/{run.id}/export?format=markdown")
    assert export_response.status_code == 200
    assert export_response.mimetype == "text/markdown"


def test_generate_memo_requires_ready_style_profile(client):
    _seed_generation_data()
    membership = TeamMembership.query.first()
    profile = MemoStyleProfile(
        team_id=membership.team_id,
        created_by_user_id=User.query.filter_by(email="tester@example.com").first().id,
        name="Pending Style",
        status="queued",
    )
    db.session.add(profile)
    db.session.commit()

    response = client.post(
        "/api/memos/runs",
        json={
            "style_profile_id": profile.id,
            "benchmark_asset_class": "Buyout",
            "document_ids": [],
            "memo_type": "fund_investment",
            "filters": {},
        },
    )

    assert response.status_code == 409
    assert response.get_json()["error"] == "style_profile_not_ready"


def test_generate_memo_requires_ready_documents(client):
    _seed_generation_data()
    membership = TeamMembership.query.first()
    user = User.query.filter_by(email="tester@example.com").first()
    profile = MemoStyleProfile(
        team_id=membership.team_id,
        created_by_user_id=user.id,
        name="Ready Style",
        status="ready",
        profile_json="{}",
    )
    document = MemoDocument(
        team_id=membership.team_id,
        firm_id=TeamFirmAccess.query.filter_by(team_id=membership.team_id).first().firm_id,
        created_by_user_id=user.id,
        document_role="ddq",
        file_name="pending_ddq.txt",
        mime_type="text/plain",
        storage_key="memo-documents/test/pending_ddq.txt",
        sha256="abc123",
        status="uploaded",
        extraction_status="pending",
    )
    db.session.add_all([profile, document])
    db.session.commit()

    response = client.post(
        "/api/memos/runs",
        json={
            "style_profile_id": profile.id,
            "benchmark_asset_class": "Buyout",
            "document_ids": [document.id],
            "memo_type": "fund_investment",
            "filters": {},
        },
    )

    assert response.status_code == 409
    payload = response.get_json()
    assert payload["error"] == "documents_not_ready"
    assert "pending_ddq.txt" in payload["documents"]


def test_generate_memo_prefers_dashboard_benchmark_selection(client):
    _seed_generation_data()
    membership = TeamMembership.query.first()
    db.session.add(
        BenchmarkPoint(
            team_id=membership.team_id,
            asset_class="Growth",
            vintage_year=2021,
            metric="net_irr",
            quartile="median",
            value=0.13,
        )
    )
    db.session.commit()

    prior_memo = client.post(
        "/api/memos/documents",
        data={
            "document_role": "prior_memo",
            "file": (
                BytesIO(b"Executive Summary\n\nProceed."),
                "prior_memo.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert prior_memo.status_code == 201

    style_profile = client.post("/api/memos/style-profiles/rebuild", json={"name": "Memo Style"})
    assert style_profile.status_code == 201
    style_profile_id = style_profile.get_json()["id"]

    source_doc = client.post(
        "/api/memos/documents",
        data={
            "document_role": "ddq",
            "file": (
                BytesIO(b"Fund Overview\n\nGrounding text."),
                "ddq.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert source_doc.status_code == 201
    source_doc_id = source_doc.get_json()["id"]

    with client.session_transaction() as session_state:
        session_state["selected_benchmark_asset_class"] = "Growth"

    response = client.post(
        "/api/memos/runs",
        json={
            "style_profile_id": style_profile_id,
            "benchmark_asset_class": "Buyout",
            "document_ids": [source_doc_id],
            "memo_type": "fund_investment",
            "filters": {},
        },
    )

    assert response.status_code == 201
    payload = response.get_json()
    assert payload["benchmark_asset_class"] == "Growth"


def test_manual_section_edit_requires_re_review_before_approval(client):
    _seed_generation_data()

    prior_memo = client.post(
        "/api/memos/documents",
        data={
            "document_role": "prior_memo",
            "file": (
                BytesIO(
                    b"Executive Summary\n\nWe recommend proceeding with diligence.\n\n"
                    b"Recommendation\n\nWe recommend approval after closing the remaining diligence points."
                ),
                "prior_memo.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert prior_memo.status_code == 201

    style_profile = client.post("/api/memos/style-profiles/rebuild", json={"name": "Memo Style"})
    assert style_profile.status_code == 201
    style_profile_id = style_profile.get_json()["id"]

    source_doc = client.post(
        "/api/memos/documents",
        data={
            "document_role": "ddq",
            "file": (
                BytesIO(
                    b"Fund Overview\n\nFund Memo targets North America buyout opportunities.\n\n"
                    b"Risks\n\nOperational diligence is still underway."
                ),
                "ddq.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert source_doc.status_code == 201
    source_doc_id = source_doc.get_json()["id"]

    run_response = client.post(
        "/api/memos/runs",
        json={
            "style_profile_id": style_profile_id,
            "benchmark_asset_class": "Buyout",
            "document_ids": [source_doc_id],
            "memo_type": "fund_investment",
            "filters": {},
        },
    )
    assert run_response.status_code == 201
    run_id = run_response.get_json()["id"]

    sections_response = client.get(f"/api/memos/runs/{run_id}/sections")
    assert sections_response.status_code == 200
    sections = sections_response.get_json()["items"]
    assert sections
    section_key = sections[0]["section_key"]

    update_response = client.patch(
        f"/api/memos/runs/{run_id}/sections/{section_key}",
        json={
            "draft_text": "Executive Summary\n\nUpdated memo language for the investment committee.",
            "editor_notes": "Tighten the risk framing and keep the tone restrained.",
        },
    )
    assert update_response.status_code == 200
    update_payload = update_response.get_json()
    assert update_payload["section"]["draft_text"].startswith("Executive Summary")
    assert update_payload["section"]["editor_notes"] == "Tighten the risk framing and keep the tone restrained."
    assert update_payload["section"]["review_status"] == "needs_review"
    assert "Updated memo language" in (update_payload["run"]["final_markdown"] or "")

    blocked_approval = client.post(f"/api/memos/runs/{run_id}/approve")
    assert blocked_approval.status_code == 409
    assert blocked_approval.get_json()["error"] == "approval_blocked"
    assert section_key in blocked_approval.get_json()["blocked_sections"]

    review_response = client.post(
        f"/api/memos/runs/{run_id}/sections/{section_key}/review",
        json={"review_status": "reviewed"},
    )
    assert review_response.status_code == 200
    review_payload = review_response.get_json()
    assert review_payload["section"]["review_status"] == "reviewed"
    assert review_payload["run"]["reviewed_section_count"] >= 1


def test_failed_generation_job_marks_run_failed(app_context):
    membership = TeamMembership(team_id=1, user_id=1, role="owner")
    profile = MemoStyleProfile(
        id=1,
        team_id=1,
        created_by_user_id=1,
        name="Ready Style",
        status="ready",
        profile_json="{}",
    )
    db.session.add_all(
        [
            Team(id=1, name="Memo Team", slug="memo-team"),
            User(id=1, email="owner@example.com", password_hash="x", is_active=True),
            Firm(id=1, name="Memo Firm", slug="memo-firm"),
            membership,
            TeamFirmAccess(team_id=1, firm_id=1, created_by_user_id=1),
            profile,
        ]
    )
    db.session.flush()
    run = MemoGenerationRun(
        team_id=1,
        firm_id=1,
        created_by_user_id=1,
        style_profile_id=1,
        memo_type="fund_investment",
        status="running",
        progress_stage="drafting",
    )
    db.session.add(run)
    db.session.flush()
    job = MemoJob(
        team_id=1,
        run_id=run.id,
        job_type="generate_memo_run",
        status="running",
        attempt_count=3,
        payload_json="{}",
    )
    db.session.add(job)
    db.session.commit()

    _fail_job(job, RuntimeError("generation exploded"))

    db.session.refresh(run)
    db.session.refresh(job)
    assert job.status == "failed"
    assert run.status == "failed"
    assert run.progress_stage == "failed"


def test_generate_memo_run_returns_failed_run_instead_of_500(client, monkeypatch):
    _seed_generation_data()

    prior_memo = client.post(
        "/api/memos/documents",
        data={
            "document_role": "prior_memo",
            "file": (
                BytesIO(b"Executive Summary\n\nProceed."),
                "prior_memo.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert prior_memo.status_code == 201

    style_profile = client.post("/api/memos/style-profiles/rebuild", json={"name": "Memo Style"})
    assert style_profile.status_code == 201
    style_profile_id = style_profile.get_json()["id"]

    source_doc = client.post(
        "/api/memos/documents",
        data={
            "document_role": "ddq",
            "file": (
                BytesIO(b"Fund Overview\n\nGrounding text."),
                "ddq.txt",
            ),
        },
        content_type="multipart/form-data",
    )
    assert source_doc.status_code == 201
    source_doc_id = source_doc.get_json()["id"]

    def fail_generate(_run_id):
        raise RuntimeError("simulated generation failure")

    monkeypatch.setattr("peqa.services.memos.jobs.generate_memo_run", fail_generate)

    response = client.post(
        "/api/memos/runs",
        json={
            "style_profile_id": style_profile_id,
            "benchmark_asset_class": "Buyout",
            "document_ids": [source_doc_id],
            "memo_type": "fund_investment",
            "filters": {},
        },
    )

    assert response.status_code == 201
    payload = response.get_json()
    assert payload["status"] in {"queued", "failed"}
    assert payload["latest_job"] is not None
    assert "simulated generation failure" in (payload["latest_job"]["error_text"] or "")

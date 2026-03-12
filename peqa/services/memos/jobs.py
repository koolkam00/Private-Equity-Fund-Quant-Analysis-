from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from flask import current_app

from models import MemoDocument, MemoDocumentChunk, MemoGenerationRun, MemoJob, MemoStyleProfile, db
from peqa.services.memos.chunking import chunk_document
from peqa.services.memos.extractors import extract_document
from peqa.services.memos.orchestrator import generate_memo_run, rerun_memo_section
from peqa.services.memos.storage import get_document_storage
from peqa.services.memos.style_profiles import rebuild_style_profile


logger = logging.getLogger(__name__)
_async_job_ids: set[int] = set()
_async_job_ids_lock = threading.Lock()


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json_dumps(value):
    return json.dumps(value, sort_keys=True, default=str)


def _json_loads(value, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def enqueue_job(team_id: int, job_type: str, payload: dict, run_id: int | None = None) -> MemoJob:
    job = MemoJob(
        team_id=team_id,
        run_id=run_id,
        job_type=job_type,
        status="queued",
        payload_json=_json_dumps(payload),
    )
    db.session.add(job)
    db.session.commit()

    use_web_async = bool(current_app.config.get("MEMO_WEB_ASYNC_JOBS"))
    use_inline = bool(current_app.config.get("MEMO_INLINE_JOBS"))
    is_production = bool(current_app.config.get("IS_PRODUCTION"))

    if use_web_async or (is_production and use_inline):
        launch_async_job(job.id)
    elif use_inline:
        try:
            run_inline_job(job.id)
        except Exception:
            logger.exception("Inline memo job %s failed during enqueue", job.id)
        job = db.session.get(MemoJob, job.id)
    return job


def _mark_job_running(job: MemoJob) -> MemoJob:
    job.status = "running"
    job.attempt_count = (job.attempt_count or 0) + 1
    job.lease_expires_at = _utc_now_naive() + timedelta(seconds=90)
    db.session.add(job)
    db.session.commit()
    return job


def _complete_job(job: MemoJob):
    job.status = "completed"
    job.lease_expires_at = None
    job.error_text = None
    db.session.add(job)
    db.session.commit()


def _fail_job(job: MemoJob, exc: Exception):
    job.error_text = str(exc)
    max_attempts = 2 if job.job_type == "extract_document" else 3
    if (job.attempt_count or 0) >= max_attempts:
        job.status = "failed"
    else:
        job.status = "queued"
        job.lease_expires_at = _utc_now_naive() + timedelta(seconds=2 ** min(job.attempt_count, 5))
    if job.status == "failed":
        if job.job_type == "rebuild_style_profile":
            payload = _json_loads(job.payload_json, {})
            profile = db.session.get(MemoStyleProfile, payload.get("style_profile_id"))
            if profile is not None:
                profile.status = "failed"
                db.session.add(profile)
        elif job.run_id:
            run = db.session.get(MemoGenerationRun, job.run_id)
            if run is not None:
                if job.job_type == "export_memo":
                    run.export_status = "failed"
                else:
                    run.status = "failed"
                    run.progress_stage = "failed"
                db.session.add(run)
    db.session.add(job)
    db.session.commit()


def _process_extract_document(payload: dict):
    document = db.session.get(MemoDocument, payload["document_id"])
    if document is None:
        raise ValueError(f"Memo document {payload['document_id']} not found")

    storage = get_document_storage()
    document.status = "processing"
    document.extraction_status = "running"
    db.session.add(document)
    db.session.commit()

    try:
        extracted = extract_document(document, storage)
        if not extracted.text.strip():
            raise ValueError("No extractable text was found in the uploaded document.")
        MemoDocumentChunk.query.filter_by(document_id=document.id).delete(synchronize_session=False)
        chunks = chunk_document(extracted)
        for chunk in chunks:
            db.session.add(
                MemoDocumentChunk(
                    document_id=document.id,
                    team_id=document.team_id,
                    firm_id=document.firm_id,
                    chunk_index=chunk.chunk_index,
                    section_key=chunk.section_key,
                    page_start=chunk.page_start,
                    page_end=chunk.page_end,
                    text=chunk.text,
                    text_delexicalized=chunk.text_delexicalized,
                    metadata_json=_json_dumps(chunk.metadata),
                    status="ready",
                )
            )
        document.page_count = extracted.page_count
        document.status = "ready"
        document.extraction_status = "ready"
        document.error_text = None
        document.metadata_json = _json_dumps(extracted.metadata)
        db.session.add(document)
        db.session.commit()
    except Exception as exc:
        document.status = "failed"
        document.extraction_status = "failed"
        document.error_text = str(exc)
        db.session.add(document)
        db.session.commit()
        raise


def _process_rebuild_style_profile(payload: dict):
    rebuild_style_profile(payload["style_profile_id"], document_ids=payload.get("document_ids"))


def _process_generate_memo(payload: dict):
    run = generate_memo_run(payload["run_id"])
    return run.id


def _process_rerun_section(payload: dict):
    run = rerun_memo_section(payload["run_id"], payload["section_key"])
    return run.id


def _process_export_memo(payload: dict):
    run = db.session.get(MemoGenerationRun, payload["run_id"])
    if run is None:
        raise ValueError(f"Memo run {payload['run_id']} not found")
    if not run.final_markdown:
        generate_memo_run(run.id)
        run = db.session.get(MemoGenerationRun, run.id)
    run.export_status = "ready"
    db.session.add(run)
    db.session.commit()


def process_job(job: MemoJob):
    payload = _json_loads(job.payload_json, {})
    if job.job_type == "extract_document":
        return _process_extract_document(payload)
    if job.job_type == "rebuild_style_profile":
        return _process_rebuild_style_profile(payload)
    if job.job_type == "generate_memo_run":
        return _process_generate_memo(payload)
    if job.job_type == "rerun_section":
        return _process_rerun_section(payload)
    if job.job_type == "export_memo":
        return _process_export_memo(payload)
    raise ValueError(f"Unsupported memo job type: {job.job_type}")


def run_inline_job(job_id: int):
    job = claim_job_by_id(job_id)
    if job is None:
        raise ValueError(f"Memo job {job_id} was not available to claim")
    try:
        process_job(job)
    except Exception as exc:
        logger.exception("Memo job %s failed", job.id)
        _fail_job(job, exc)
        raise
    else:
        _complete_job(job)
        return job


def claim_job_by_id(job_id: int) -> MemoJob | None:
    now = _utc_now_naive()
    query = MemoJob.query.filter(
        MemoJob.id == job_id,
        MemoJob.status == "queued",
        (MemoJob.lease_expires_at.is_(None)) | (MemoJob.lease_expires_at <= now),
    )
    try:
        job = query.with_for_update(skip_locked=True).first()
    except TypeError:
        job = query.with_for_update().first()
    if job is None:
        return None
    return _mark_job_running(job)


def claim_next_job() -> MemoJob | None:
    now = _utc_now_naive()
    query = (
        MemoJob.query.filter(
            MemoJob.status == "queued",
            (MemoJob.lease_expires_at.is_(None)) | (MemoJob.lease_expires_at <= now),
        )
        .order_by(MemoJob.created_at.asc(), MemoJob.id.asc())
    )
    try:
        job = query.with_for_update(skip_locked=True).first()
    except TypeError:
        job = query.with_for_update().first()
    if job is None:
        return None
    return _mark_job_running(job)


def _run_async_job(app, job_id: int):
    try:
        with app.app_context():
            job = claim_job_by_id(job_id)
            if job is None:
                return
            try:
                process_job(job)
            except Exception as exc:
                logger.exception("Async memo job %s failed", job.id)
                _fail_job(job, exc)
            else:
                _complete_job(job)
            finally:
                db.session.remove()
    finally:
        with _async_job_ids_lock:
            _async_job_ids.discard(job_id)


def launch_async_job(job_id: int):
    app = current_app._get_current_object()
    with _async_job_ids_lock:
        if job_id in _async_job_ids:
            return
        _async_job_ids.add(job_id)
    thread = threading.Thread(target=_run_async_job, args=(app, job_id), daemon=True, name=f"memo-job-{job_id}")
    thread.start()


def run_worker_loop(poll_interval: float = 2.0):
    while True:
        job = claim_next_job()
        if job is None:
            time.sleep(poll_interval)
            continue
        try:
            process_job(job)
        except Exception as exc:
            logger.exception("Memo worker failed job %s", job.id)
            _fail_job(job, exc)
        else:
            _complete_job(job)

from __future__ import annotations

import json

from models import Firm, MemoDocumentChunk, MemoGenerationRun, UploadIssue, db
from peqa.services.filtering import apply_deal_filters, build_deal_scope_query
from peqa.services.memos.types import MemoEvidenceBundle
from services.metrics import compute_fund_liquidity_analysis, compute_ic_memo_payload, compute_lp_due_diligence_memo
from services.utils import DEFAULT_CURRENCY_CODE, currency_symbol, currency_unit_label, normalize_currency_code


def _json_loads(value, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _json_dumps(value):
    return json.dumps(value, sort_keys=True, default=str)


def _reporting_currency_context(firm) -> dict:
    code = normalize_currency_code(getattr(firm, "base_currency", None), default=DEFAULT_CURRENCY_CODE) or DEFAULT_CURRENCY_CODE
    return {
        "reporting_currency_code": code,
        "currency_symbol": currency_symbol(code) or "",
        "currency_unit_label": currency_unit_label(code),
        "firm_name": getattr(firm, "name", None),
    }


def _build_missing_data(lp_payload: dict, upload_issues: list[UploadIssue]) -> list[dict]:
    missing = []
    for issue in upload_issues:
        missing.append(
            {
                "source": "upload_issue",
                "severity": issue.severity,
                "message": issue.message,
                "row_number": issue.row_number,
                "file_type": issue.file_type,
            }
        )
    if not (lp_payload.get("fund_liquidity") or {}).get("has_data"):
        missing.append(
            {
                "source": "fund_liquidity",
                "severity": "warning",
                "message": "No fund quarter snapshots are available for current liquidity metrics.",
            }
        )
    for key in ("public_market_comparison", "nav_at_risk"):
        payload = lp_payload.get(key) or {}
        for flag in payload.get("risk_flags") or []:
            missing.append({"source": key, "severity": "warning", "message": flag})
    return missing


def _build_conflicts(lp_payload: dict) -> list[dict]:
    conflicts = []
    public_market = lp_payload.get("public_market_comparison") or {}
    for row in public_market.get("fund_rows") or []:
        if row.get("coverage") == "partial":
            conflicts.append(
                {
                    "source": "public_market_comparison",
                    "fund_number": row.get("fund_number"),
                    "message": "Public market comparison coverage is partial for this fund.",
                }
            )
    return conflicts


def build_memo_evidence_bundle(run_id: int) -> MemoEvidenceBundle:
    run = db.session.get(MemoGenerationRun, run_id)
    if run is None:
        raise ValueError(f"Memo run {run_id} not found")

    filters = _json_loads(run.filters_json, {})
    document_ids = _json_loads(run.document_ids_json, [])
    firm = db_get_firm(run.firm_id)
    deals = apply_deal_filters(
        build_deal_scope_query(team_id=run.team_id, firm_id=run.firm_id),
        filters,
    ).all()
    lp_payload = compute_lp_due_diligence_memo(
        deals,
        team_id=run.team_id,
        firm_id=run.firm_id,
        benchmark_asset_class=run.benchmark_asset_class or "",
    )
    ic_payload = compute_ic_memo_payload(deals)
    fund_liquidity = lp_payload.get("fund_liquidity") or compute_fund_liquidity_analysis(deals, firm_id=run.firm_id)

    chunk_query = MemoDocumentChunk.query.filter(MemoDocumentChunk.team_id == run.team_id)
    if document_ids:
        chunk_query = chunk_query.filter(MemoDocumentChunk.document_id.in_(document_ids))
    document_chunks = chunk_query.order_by(MemoDocumentChunk.document_id.asc(), MemoDocumentChunk.chunk_index.asc()).all()
    document_snippets = [
        {
            "id": f"chunk:{row.id}",
            "document_id": row.document_id,
            "section_key": row.section_key,
            "page_start": row.page_start,
            "page_end": row.page_end,
            "text": row.text,
            "metadata": _json_loads(row.metadata_json, {}),
        }
        for row in document_chunks
    ]

    upload_issues = (
        UploadIssue.query.filter(
            UploadIssue.team_id == run.team_id,
            UploadIssue.firm_id == run.firm_id,
        )
        .order_by(UploadIssue.created_at.desc(), UploadIssue.id.desc())
        .limit(50)
        .all()
    )
    missing_data = _build_missing_data(lp_payload, upload_issues)
    conflicts = _build_conflicts(lp_payload)
    open_questions = [
        {"source": item.get("source"), "question": item.get("message")}
        for item in missing_data + conflicts
    ]

    structured_facts = {
        "as_of_date": (lp_payload.get("meta") or {}).get("as_of_date"),
        "benchmark_asset_class": run.benchmark_asset_class or "",
        "fund_count": len(lp_payload.get("fund_metadata") or []),
        "fund_metadata": lp_payload.get("fund_metadata") or [],
        "pme_complete_funds": (lp_payload.get("public_market_comparison") or {}).get("coverage", {}).get("funds_with_complete_coverage"),
        "liquidity_current_dpi": (fund_liquidity.get("latest") or {}).get("dpi"),
        "liquidity_current_tvpi": (fund_liquidity.get("latest") or {}).get("tvpi"),
        "liquidity_current_rvpi": (fund_liquidity.get("latest") or {}).get("rvpi"),
        "top_10_nav_pct": (lp_payload.get("nav_at_risk") or {}).get("summary", {}).get("top_10_nav_pct"),
        "stale_nav_pct": (lp_payload.get("nav_at_risk") or {}).get("summary", {}).get("stale_nav_pct"),
        "ic_portfolio_summary": ic_payload.get("portfolio_summary") or {},
        "top_value_creation_deals": ic_payload.get("top_5_deals_by_value_created") or [],
    }

    analysis_summaries = {
        "lp_due_diligence_memo": lp_payload,
        "ic_memo": ic_payload,
    }

    bundle = MemoEvidenceBundle(
        run_id=run.id,
        firm_id=run.firm_id,
        team_id=run.team_id,
        filters=filters,
        benchmark_asset_class=run.benchmark_asset_class or "",
        reporting_currency_context=_reporting_currency_context(firm),
        structured_facts=structured_facts,
        analysis_summaries=analysis_summaries,
        document_snippets=document_snippets,
        missing_data=missing_data,
        conflicts=conflicts,
        open_questions=open_questions,
        benchmark_context={
            "asset_class": run.benchmark_asset_class or "",
            "summary": lp_payload.get("benchmarking_summary") or {},
        },
    )

    run.evidence_json = _json_dumps({
        "structured_facts": bundle.structured_facts,
        "missing_data": bundle.missing_data,
        "conflicts": bundle.conflicts,
        "open_questions": bundle.open_questions,
    })
    run.missing_data_json = _json_dumps(bundle.missing_data)
    run.conflicts_json = _json_dumps(bundle.conflicts)
    run.open_questions_json = _json_dumps(bundle.open_questions)
    db.session.add(run)
    db.session.commit()
    return bundle


def db_get_firm(firm_id: int | None):
    if firm_id is None:
        return None
    return db.session.get(Firm, firm_id)

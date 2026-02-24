import logging
import os
from io import BytesIO
from pathlib import Path
from datetime import datetime

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_file, url_for
from sqlalchemy.exc import OperationalError
from werkzeug.utils import secure_filename

from config import Config
from models import Deal, db, ensure_schema_updates
from services.deal_parser import parse_deals
from services.metrics import (
    build_methodology_payload,
    compute_bridge_aggregate,
    compute_bridge_view,
    compute_data_quality,
    compute_deals_rollup_details,
    compute_deal_trajectory_analysis,
    compute_deal_metrics,
    compute_deal_track_record,
    compute_exit_readiness_analysis,
    compute_exit_type_performance,
    compute_fund_liquidity_analysis,
    compute_ic_memo_payload,
    compute_lead_partner_scorecard,
    compute_loss_and_distribution,
    compute_loss_concentration_heatmap,
    compute_moic_hold_scatter,
    compute_portfolio_analytics,
    compute_realized_unrealized_exposure,
    compute_stress_lab_analysis,
    compute_underwrite_outcome_analysis,
    compute_value_creation_mix,
    compute_valuation_quality_analysis,
    compute_vintage_series,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_object(Config)
db.init_app(app)

os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
os.makedirs(os.path.join(os.path.dirname(__file__), "instance"), exist_ok=True)

DEAL_TEMPLATE_FILENAME = "PE_Fund_Data_Template.xlsx"

ANALYSIS_PAGES = {
    "fund-liquidity": {
        "title": "Fund Liquidity & Performance Curve",
        "description": "Quarterly paid-in, distributed, NAV, unfunded, and TVPI/DPI/RVPI/PIC trends.",
    },
    "underwrite-outcome": {
        "title": "Underwrite vs Outcome",
        "description": "Planned vs realized returns, hold periods, and driver deltas by strategy segment.",
    },
    "valuation-quality": {
        "title": "Unrealized Valuation Quality",
        "description": "Mark staleness, volatility, markdown concentration, and pre-exit mark backtests.",
    },
    "exit-readiness": {
        "title": "Exit Readiness & Aging",
        "description": "Hold-age diagnostics, thesis completion, and time-above-target signals for unrealized deals.",
    },
    "stress-lab": {
        "title": "Concentration Stress Lab",
        "description": "Scenario stress on multiples, EBITDA, and timing to identify downside concentration.",
    },
    "deal-trajectory": {
        "title": "Deal Trajectory",
        "description": "Quarterly EV/net debt/equity and cash-flow path for an individual deal.",
    },
}


def _bootstrap_schema():
    db.create_all()
    ensure_schema_updates()


def _is_missing_table_error(exc):
    message = str(exc).lower()
    return "no such table:" in message and any(marker in message for marker in ("deals", "upload_issues"))


def _recover_missing_tables(exc):
    if not _is_missing_table_error(exc):
        return False
    logger.warning("Detected missing table at runtime. Repairing schema. Error: %s", exc)
    db.session.rollback()
    _bootstrap_schema()
    return True


with app.app_context():
    _bootstrap_schema()


def _allowed_file(filename):
    return os.path.splitext(filename)[1].lower() in app.config["ALLOWED_EXTENSIONS"]


def _deal_vintage_year(deal):
    if deal.year_invested is not None:
        return int(deal.year_invested)
    if deal.investment_date is not None:
        return int(deal.investment_date.year)
    return None


def _fmt_track_date(value):
    if value is None:
        return "—"
    return value.strftime("%b-%y")


def _fmt_track_years(value):
    if value is None:
        return "—"
    return f"{value:.1f}"


def _fmt_track_pct(value):
    if value is None:
        return "—"
    return f"{value * 100:.1f}%"


def _fmt_track_multiple(value):
    if value is None:
        return "—"
    return f"{value:.2f}x"


def _fmt_track_currency(value):
    if value is None:
        return "—"
    return f"${value:.1f}M"


def _track_totals_to_pdf_row(label, totals, include_fund_size=True):
    return [
        "",
        label,
        "",
        "—",
        "—",
        _fmt_track_years(totals.get("hold_period")),
        _fmt_track_pct(totals.get("ownership_pct")),
        _fmt_track_pct(totals.get("pct_total_invested")),
        _fmt_track_pct(totals.get("pct_fund_size")) if include_fund_size else "—",
        _fmt_track_currency(totals.get("invested_equity")),
        _fmt_track_currency(totals.get("realized_value")),
        _fmt_track_currency(totals.get("unrealized_value")),
        _fmt_track_currency(totals.get("total_value")),
        _fmt_track_pct(totals.get("gross_irr")),
        _fmt_track_multiple(totals.get("gross_moic")),
        _fmt_track_multiple(totals.get("realized_gross_moic")),
        _fmt_track_multiple(totals.get("unrealized_gross_moic")),
    ]


def _build_track_record_pdf(track_record):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A3, landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    headers = [
        "#",
        "Company",
        "Status",
        "Investment Date",
        "Exit Date",
        "Hold Period",
        "Ownership",
        "% of Total Invested",
        "% of Fund Size",
        "Total Invested",
        "Realized Value",
        "Unrealized Value",
        "Total Value",
        "Gross IRR",
        "Gross MOIC",
        "Realized Gross MOIC",
        "Unrealized Gross MOIC",
    ]
    rows = [headers]
    row_tags = ["header"]

    for fund in track_record.get("funds", []):
        fund_title = fund.get("fund_name") or "Unknown Fund"
        if fund.get("fund_size") is not None:
            fund_title = f"{fund_title} (${fund['fund_size']:.1f}MM)"
        if fund.get("fund_size_conflict"):
            fund_title = f"{fund_title} [fund size conflict]"
        rows.append([fund_title] + [""] * 16)
        row_tags.append("fund_header")

        for row in fund.get("rows", []):
            rows.append(
                [
                    str(row.get("row_num") or ""),
                    row.get("company_name") or "Unknown Company",
                    row.get("status") or "Other",
                    _fmt_track_date(row.get("investment_date")),
                    _fmt_track_date(row.get("exit_date")),
                    _fmt_track_years(row.get("hold_period")),
                    _fmt_track_pct(row.get("ownership_pct")),
                    _fmt_track_pct(row.get("pct_total_invested")),
                    _fmt_track_pct(row.get("pct_fund_size")),
                    _fmt_track_currency(row.get("invested_equity")),
                    _fmt_track_currency(row.get("realized_value")),
                    _fmt_track_currency(row.get("unrealized_value")),
                    _fmt_track_currency(row.get("total_value")),
                    _fmt_track_pct(row.get("gross_irr")),
                    _fmt_track_multiple(row.get("gross_moic")),
                    _fmt_track_multiple(row.get("realized_gross_moic")),
                    _fmt_track_multiple(row.get("unrealized_gross_moic")),
                ]
            )
            row_tags.append("detail")

        for rollup in fund.get("status_rollups", []):
            rows.append(_track_totals_to_pdf_row(rollup.get("label", "Status Rollup"), rollup.get("totals", {}), include_fund_size=True))
            row_tags.append("rollup_status")

        for rollup in fund.get("summary_rollups", []):
            rows.append(_track_totals_to_pdf_row(rollup.get("label", "Fund Rollup"), rollup.get("totals", {}), include_fund_size=True))
            row_tags.append("rollup_summary")

        net = fund.get("net_performance", {})
        irr_conflict = (net.get("conflicts") or {}).get("net_irr")
        moic_conflict = (net.get("conflicts") or {}).get("net_moic")
        dpi_conflict = (net.get("conflicts") or {}).get("net_dpi")
        net_irr_val = "N/A" if irr_conflict or net.get("net_irr") is None else _fmt_track_pct(net.get("net_irr"))
        net_moic_val = "N/A" if moic_conflict or net.get("net_moic") is None else _fmt_track_multiple(net.get("net_moic"))
        net_dpi_val = "N/A" if dpi_conflict or net.get("net_dpi") is None else _fmt_track_multiple(net.get("net_dpi"))

        rows.append(
            [
                "",
                f"{fund.get('fund_name', 'Fund')} Net Performance",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "IRR:",
                net_irr_val,
                "Net MOIC:",
                net_moic_val,
            ]
        )
        row_tags.append("net")
        rows.append([""] * 13 + ["DPI:", net_dpi_val, "", ""])
        row_tags.append("net")
        rows.append([""] * 17)
        row_tags.append("gap")

    rows.append(["All Funds Summary"] + [""] * 16)
    row_tags.append("overall_header")
    for rollup in track_record.get("overall", {}).get("status_rollups", []):
        rows.append(_track_totals_to_pdf_row(rollup.get("label", "Status Rollup"), rollup.get("totals", {}), include_fund_size=False))
        row_tags.append("rollup_status")
    for rollup in track_record.get("overall", {}).get("summary_rollups", []):
        rows.append(_track_totals_to_pdf_row(rollup.get("label", "Overall Rollup"), rollup.get("totals", {}), include_fund_size=False))
        row_tags.append("rollup_overall")

    page_width, _ = landscape(A3)
    left_margin = right_margin = 18
    available_width = page_width - left_margin - right_margin
    base_widths = [18, 110, 72, 52, 52, 42, 52, 64, 60, 65, 65, 65, 65, 52, 52, 60, 64]
    scale = available_width / sum(base_widths)
    col_widths = [w * scale for w in base_widths]

    table = Table(rows, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ("FONT", (0, 0), (-1, -1), "Helvetica", 6.4),
        ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", 6.6),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4d78")),
        ("ALIGN", (0, 0), (0, -1), "RIGHT"),
        ("ALIGN", (5, 0), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#9eb3c5")),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]

    detail_alt = False
    for idx, tag in enumerate(row_tags):
        if idx == 0:
            continue
        if tag in {"fund_header", "overall_header"}:
            style_cmds.extend(
                [
                    ("SPAN", (0, idx), (-1, idx)),
                    ("BACKGROUND", (0, idx), (-1, idx), colors.HexColor("#8eb0cf") if tag == "fund_header" else colors.HexColor("#6f97bd")),
                    ("TEXTCOLOR", (0, idx), (-1, idx), colors.HexColor("#0d2740")),
                    ("FONT", (0, idx), (-1, idx), "Helvetica-Bold", 7.0),
                    ("LINEABOVE", (0, idx), (-1, idx), 0.6, colors.HexColor("#4f6e89")),
                    ("LINEBELOW", (0, idx), (-1, idx), 0.6, colors.HexColor("#4f6e89")),
                ]
            )
            detail_alt = False
        elif tag == "detail":
            style_cmds.append(
                ("BACKGROUND", (0, idx), (-1, idx), colors.HexColor("#e5e5e5") if detail_alt else colors.HexColor("#f1f1f1"))
            )
            detail_alt = not detail_alt
        elif tag == "rollup_status":
            style_cmds.extend(
                [
                    ("BACKGROUND", (0, idx), (-1, idx), colors.HexColor("#9dbbda")),
                    ("FONT", (0, idx), (-1, idx), "Helvetica-Bold", 6.4),
                    ("LINEABOVE", (0, idx), (-1, idx), 0.5, colors.HexColor("#557894")),
                ]
            )
        elif tag == "rollup_summary":
            style_cmds.extend(
                [
                    ("BACKGROUND", (0, idx), (-1, idx), colors.HexColor("#8db0d0")),
                    ("FONT", (0, idx), (-1, idx), "Helvetica-Bold", 6.4),
                    ("LINEABOVE", (0, idx), (-1, idx), 0.5, colors.HexColor("#557894")),
                ]
            )
        elif tag == "rollup_overall":
            style_cmds.extend(
                [
                    ("BACKGROUND", (0, idx), (-1, idx), colors.HexColor("#7ca4cb")),
                    ("FONT", (0, idx), (-1, idx), "Helvetica-Bold", 6.4),
                    ("LINEABOVE", (0, idx), (-1, idx), 0.5, colors.HexColor("#557894")),
                ]
            )
        elif tag == "net":
            style_cmds.extend(
                [
                    ("BACKGROUND", (0, idx), (-1, idx), colors.HexColor("#84a9cc")),
                    ("FONT", (0, idx), (-1, idx), "Helvetica-Bold", 6.4),
                ]
            )
        elif tag == "gap":
            style_cmds.extend(
                [
                    ("BACKGROUND", (0, idx), (-1, idx), colors.HexColor("#c8d9e8")),
                    ("LINEABOVE", (0, idx), (-1, idx), 0, colors.white),
                    ("LINEBELOW", (0, idx), (-1, idx), 0, colors.white),
                ]
            )

    table.setStyle(TableStyle(style_cmds))

    styles = getSampleStyleSheet()
    title = Paragraph("Deal Level Track Record (Print-Ready PDF)", styles["Heading4"])
    generated = Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", styles["Normal"])

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A3),
        leftMargin=left_margin,
        rightMargin=right_margin,
        topMargin=16,
        bottomMargin=16,
        title="Track Record Print PDF",
    )
    doc.build([title, generated, Spacer(1, 8), table])
    return buffer.getvalue()


def _handle_upload(parse_func, redirect_route):
    if "file" not in request.files:
        flash("No file selected.", "danger")
        return redirect(url_for("upload"))

    file = request.files["file"]
    if file.filename == "":
        flash("No file selected.", "danger")
        return redirect(url_for("upload"))

    if not _allowed_file(file.filename):
        flash("Invalid file type. Please upload an .xlsx or .xls file.", "danger")
        return redirect(url_for("upload"))

    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(file_path)

    try:
        try:
            result = parse_func(file_path)
        except OperationalError as exc:
            if not _recover_missing_tables(exc):
                raise
            result = parse_func(file_path)
        if result["success"] > 0:
            flash(f"Successfully imported {result['success']} deal records (batch {result['batch_id']}).", "success")
        if result.get("duplicates_skipped", 0) > 0:
            flash(f"Skipped {result['duplicates_skipped']} duplicate deal records.", "warning")
        if result.get("quarantined_count", 0) > 0:
            flash(
                f"Quarantined {result['quarantined_count']} invalid row(s). "
                f"Issue Report ID: {result.get('issue_report_id')}",
                "warning",
            )
        if result.get("bridge_complete") is not None and result["success"] > 0:
            pct = result["bridge_complete"] / result["success"] * 100
            flash(f"Bridge-ready deals: {result['bridge_complete']}/{result['success']} ({pct:.0f}%).", "info")
        supplemental = result.get("supplemental_counts") or {}
        if supplemental:
            extras = []
            if supplemental.get("cashflows", 0) > 0:
                extras.append(f"{supplemental['cashflows']} cashflow events")
            if supplemental.get("deal_quarterly", 0) > 0:
                extras.append(f"{supplemental['deal_quarterly']} deal quarterly marks")
            if supplemental.get("fund_quarterly", 0) > 0:
                extras.append(f"{supplemental['fund_quarterly']} fund quarterly snapshots")
            if supplemental.get("underwrite", 0) > 0:
                extras.append(f"{supplemental['underwrite']} underwrite baseline rows")
            if extras:
                flash("Imported supplemental analysis rows: " + ", ".join(extras) + ".", "info")
        for err in result.get("errors", [])[:8]:
            flash(err, "warning")
        if len(result.get("errors", [])) > 8:
            flash(f"...and {len(result['errors']) - 8} additional warnings.", "warning")
        if result["success"] == 0 and not result.get("errors"):
            flash("No records found in the file.", "warning")
    except Exception as exc:
        logger.exception("Error processing upload")
        flash(f"Error processing file: {str(exc)}", "danger")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    return redirect(url_for(redirect_route))


def _empty_dashboard_context():
    metric_pair = {"avg": None, "wavg": None}
    empty_entry_exit = {
        "entry": {
            "tev_ebitda": metric_pair,
            "tev_revenue": metric_pair,
            "net_debt_ebitda": metric_pair,
            "net_debt_tev": metric_pair,
            "ebitda_margin": metric_pair,
        },
        "exit": {
            "tev_ebitda": metric_pair,
            "tev_revenue": metric_pair,
            "net_debt_ebitda": metric_pair,
            "net_debt_tev": metric_pair,
            "ebitda_margin": metric_pair,
        },
        "growth": {
            "revenue_growth": metric_pair,
            "ebitda_growth": metric_pair,
            "revenue_cagr": metric_pair,
            "ebitda_cagr": metric_pair,
        },
        "returns": {
            "gross_moic": metric_pair,
            "implied_irr": metric_pair,
            "hold_period": metric_pair,
        },
    }

    return {
        "kpis": {
            "total_deals": 0,
            "total_equity": 0,
            "total_value": 0,
            "total_value_created": 0,
            "gross_moic": None,
            "implied_irr": None,
        },
        "loss": {"count_pct": None, "capital_pct": None, "loss_count": 0, "total_count": 0},
        "moic_distribution": [],
        "entry_exit_summary": empty_entry_exit,
        "bridge_aggregate": {
            "model": "additive",
            "basis": "fund",
            "ready_count": 0,
            "drivers": {"dollar": {}, "moic": {}, "pct": {}},
            "total_value_created": 0,
            "total_equity": 0,
            "start_end": {
                "dollar": {"start": 0, "end": 0},
                "moic": {"start": None, "end": None},
                "pct": {"start": 0, "end": 1},
            },
        },
        "vintage_series": [],
        "moic_hold_scatter": [],
        "value_creation_mix": {
            "current": "fund",
            "series": {
                "fund": {"labels": [], "drivers": {}, "totals_dollar": []},
                "sector": {"labels": [], "drivers": {}, "totals_dollar": []},
                "exit_type": {"labels": [], "drivers": {}, "totals_dollar": []},
            },
        },
        "realized_unrealized_exposure": {"labels": [], "realized": [], "unrealized": []},
        "loss_concentration_heatmap": {"sectors": [], "geographies": [], "values": [], "max_value": 0},
        "exit_type_performance": {
            "labels": [],
            "calculated_moic": [],
            "deal_count": [],
            "realized_value": [],
        },
        "lead_partner_scorecard": [],
        "deal_metrics": {},
        "deals": [],
        "data_quality": {"total_deals": 0, "complete_deals": 0, "bridge_ready": 0, "warnings": []},
        "funds": [],
        "statuses": [],
        "sectors": [],
        "geographies": [],
        "vintages": [],
        "exit_types": [],
        "lead_partners": [],
        "security_types": [],
        "deal_types": [],
        "entry_channels": [],
        "current_fund": "",
        "current_status": "",
        "current_sector": "",
        "current_geography": "",
        "current_vintage": "",
        "current_exit_type": "",
        "current_lead_partner": "",
        "current_security_type": "",
        "current_deal_type": "",
        "current_entry_channel": "",
    }


def _build_filtered_deals_context(fund_override=None):
    try:
        all_deals = Deal.query.all()
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            raise
        all_deals = Deal.query.all()

    funds = sorted({d.fund_number for d in all_deals if d.fund_number})
    statuses = sorted({d.status for d in all_deals if d.status})
    sectors = sorted({d.sector for d in all_deals if d.sector})
    geographies = sorted({d.geography for d in all_deals if d.geography})
    vintages = sorted({_deal_vintage_year(d) for d in all_deals if _deal_vintage_year(d) is not None})
    exit_types = sorted({d.exit_type or "Not Specified" for d in all_deals})
    lead_partners = sorted({d.lead_partner or "Unassigned" for d in all_deals})
    security_types = sorted({d.security_type or "Common Equity" for d in all_deals})
    deal_types = sorted({d.deal_type or "Platform" for d in all_deals})
    entry_channels = sorted({d.entry_channel or "Unknown" for d in all_deals})

    current_fund = fund_override if fund_override is not None else request.args.get("fund", "")
    current_status = request.args.get("status", "")
    current_sector = request.args.get("sector", "")
    current_geography = request.args.get("geography", "")
    current_vintage = request.args.get("vintage", "")
    current_exit_type = request.args.get("exit_type", "")
    current_lead_partner = request.args.get("lead_partner", "")
    current_security_type = request.args.get("security_type", "")
    current_deal_type = request.args.get("deal_type", "")
    current_entry_channel = request.args.get("entry_channel", "")

    filtered = all_deals
    if current_fund:
        filtered = [d for d in filtered if d.fund_number == current_fund]
    if current_status:
        filtered = [d for d in filtered if d.status == current_status]
    if current_sector:
        filtered = [d for d in filtered if d.sector == current_sector]
    if current_geography:
        filtered = [d for d in filtered if (d.geography or "Unknown") == current_geography]
    if current_vintage:
        try:
            vintage_int = int(current_vintage)
            filtered = [d for d in filtered if _deal_vintage_year(d) == vintage_int]
        except ValueError:
            filtered = []
    if current_exit_type:
        filtered = [d for d in filtered if (d.exit_type or "Not Specified") == current_exit_type]
    if current_lead_partner:
        filtered = [d for d in filtered if (d.lead_partner or "Unassigned") == current_lead_partner]
    if current_security_type:
        filtered = [d for d in filtered if (d.security_type or "Common Equity") == current_security_type]
    if current_deal_type:
        filtered = [d for d in filtered if (d.deal_type or "Platform") == current_deal_type]
    if current_entry_channel:
        filtered = [d for d in filtered if (d.entry_channel or "Unknown") == current_entry_channel]

    return {
        "deals": filtered,
        "funds": funds,
        "statuses": statuses,
        "sectors": sectors,
        "geographies": geographies,
        "vintages": vintages,
        "exit_types": exit_types,
        "lead_partners": lead_partners,
        "security_types": security_types,
        "deal_types": deal_types,
        "entry_channels": entry_channels,
        "current_fund": current_fund,
        "current_status": current_status,
        "current_sector": current_sector,
        "current_geography": current_geography,
        "current_vintage": current_vintage,
        "current_exit_type": current_exit_type,
        "current_lead_partner": current_lead_partner,
        "current_security_type": current_security_type,
        "current_deal_type": current_deal_type,
        "current_entry_channel": current_entry_channel,
    }


def _build_dashboard_payload(filtered_deals):
    metrics_by_id = {d.id: compute_deal_metrics(d) for d in filtered_deals}

    portfolio = compute_portfolio_analytics(filtered_deals, metrics_by_id=metrics_by_id)
    risk = compute_loss_and_distribution(filtered_deals, metrics_by_id=metrics_by_id)
    additive = compute_bridge_aggregate(filtered_deals, basis="fund")
    vintage = compute_vintage_series(filtered_deals, metrics_by_id=metrics_by_id)
    moic_hold_scatter = compute_moic_hold_scatter(filtered_deals, metrics_by_id=metrics_by_id)
    value_creation_mix = {
        "current": "fund",
        "series": {
            "fund": compute_value_creation_mix(filtered_deals, metrics_by_id=metrics_by_id, group_by="fund"),
            "sector": compute_value_creation_mix(filtered_deals, metrics_by_id=metrics_by_id, group_by="sector"),
            "exit_type": compute_value_creation_mix(filtered_deals, metrics_by_id=metrics_by_id, group_by="exit_type"),
        },
    }
    realized_unrealized_exposure = compute_realized_unrealized_exposure(filtered_deals)
    loss_concentration_heatmap = compute_loss_concentration_heatmap(filtered_deals, metrics_by_id=metrics_by_id)
    exit_type_performance = compute_exit_type_performance(filtered_deals, metrics_by_id=metrics_by_id)
    lead_partner_scorecard = compute_lead_partner_scorecard(filtered_deals, metrics_by_id=metrics_by_id)
    quality = compute_data_quality(filtered_deals, metrics_by_id)

    kpis = {
        "total_deals": len(filtered_deals),
        "total_equity": portfolio["total_equity"],
        "total_value": portfolio["total_value"],
        "total_value_created": portfolio["total_value_created"],
        "gross_moic": portfolio["returns"]["gross_moic"]["avg"],
        "implied_irr": portfolio["returns"]["implied_irr"]["wavg"],
    }

    return {
        "kpis": kpis,
        "loss": risk["loss_ratios"],
        "moic_distribution": risk["moic_distribution"],
        "entry_exit_summary": {
            "entry": portfolio["entry"],
            "exit": portfolio["exit"],
            "growth": portfolio["growth"],
            "returns": portfolio["returns"],
        },
        "bridge_aggregate": additive,
        "vintage_series": vintage,
        "moic_hold_scatter": moic_hold_scatter,
        "value_creation_mix": value_creation_mix,
        "realized_unrealized_exposure": realized_unrealized_exposure,
        "loss_concentration_heatmap": loss_concentration_heatmap,
        "exit_type_performance": exit_type_performance,
        "lead_partner_scorecard": lead_partner_scorecard,
        "deal_metrics": metrics_by_id,
        "data_quality": quality,
    }


def _analysis_route_payload(page, filtered_deals):
    metrics_by_id = {d.id: compute_deal_metrics(d) for d in filtered_deals}

    if page == "fund-liquidity":
        return compute_fund_liquidity_analysis(filtered_deals)
    if page == "underwrite-outcome":
        return compute_underwrite_outcome_analysis(filtered_deals, metrics_by_id=metrics_by_id)
    if page == "valuation-quality":
        return compute_valuation_quality_analysis(filtered_deals)
    if page == "exit-readiness":
        return compute_exit_readiness_analysis(filtered_deals, metrics_by_id=metrics_by_id)
    if page == "stress-lab":
        deal_overrides = {}
        for deal in filtered_deals:
            multiple_raw = request.args.get(f"ms_{deal.id}")
            ebitda_raw = request.args.get(f"es_{deal.id}")
            hold_raw = request.args.get(f"hp_{deal.id}")
            if multiple_raw in (None, "") and ebitda_raw in (None, "") and hold_raw in (None, ""):
                continue

            override = {}
            if multiple_raw not in (None, ""):
                try:
                    override["multiple_shock"] = float(multiple_raw)
                except ValueError:
                    pass
            if ebitda_raw not in (None, ""):
                try:
                    override["ebitda_shock"] = float(ebitda_raw) / 100.0
                except ValueError:
                    pass
            if hold_raw not in (None, ""):
                try:
                    override["expected_hold_years"] = float(hold_raw)
                except ValueError:
                    pass
            if override:
                deal_overrides[deal.id] = override

        scenario = {
            "default_multiple_shock": 0.0,
            "default_ebitda_shock": 0.0,
        }
        return compute_stress_lab_analysis(
            filtered_deals,
            scenario=scenario,
            deal_overrides=deal_overrides,
            metrics_by_id=metrics_by_id,
        )
    if page == "deal-trajectory":
        return compute_deal_trajectory_analysis(
            filtered_deals,
            deal_id=request.args.get("deal_id"),
            metrics_by_id=metrics_by_id,
        )
    abort(404)


@app.route("/")
def index():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    try:
        filter_ctx = _build_filtered_deals_context()
        payload = _build_dashboard_payload(filter_ctx["deals"])
        return render_template(
            "dashboard.html",
            deals=filter_ctx["deals"],
            kpis=payload["kpis"],
            loss=payload["loss"],
            moic_distribution=payload["moic_distribution"],
            entry_exit_summary=payload["entry_exit_summary"],
            bridge_aggregate=payload["bridge_aggregate"],
            vintage_series=payload["vintage_series"],
            deal_metrics=payload["deal_metrics"],
            data_quality=payload["data_quality"],
            funds=filter_ctx["funds"],
            statuses=filter_ctx["statuses"],
            sectors=filter_ctx["sectors"],
            geographies=filter_ctx["geographies"],
            vintages=filter_ctx["vintages"],
            exit_types=filter_ctx["exit_types"],
            lead_partners=filter_ctx["lead_partners"],
            security_types=filter_ctx["security_types"],
            deal_types=filter_ctx["deal_types"],
            entry_channels=filter_ctx["entry_channels"],
            current_fund=filter_ctx["current_fund"],
            current_status=filter_ctx["current_status"],
            current_sector=filter_ctx["current_sector"],
            current_geography=filter_ctx["current_geography"],
            current_vintage=filter_ctx["current_vintage"],
            current_exit_type=filter_ctx["current_exit_type"],
            current_lead_partner=filter_ctx["current_lead_partner"],
            current_security_type=filter_ctx["current_security_type"],
            current_deal_type=filter_ctx["current_deal_type"],
            current_entry_channel=filter_ctx["current_entry_channel"],
        )
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            logger.exception("Dashboard computation failed")
            flash(f"Error computing dashboard metrics: {str(exc)}", "danger")
            return render_template("dashboard.html", **_empty_dashboard_context())

        filter_ctx = _build_filtered_deals_context()
        payload = _build_dashboard_payload(filter_ctx["deals"])
        return render_template(
            "dashboard.html",
            deals=filter_ctx["deals"],
            kpis=payload["kpis"],
            loss=payload["loss"],
            moic_distribution=payload["moic_distribution"],
            entry_exit_summary=payload["entry_exit_summary"],
            bridge_aggregate=payload["bridge_aggregate"],
            vintage_series=payload["vintage_series"],
            deal_metrics=payload["deal_metrics"],
            data_quality=payload["data_quality"],
            funds=filter_ctx["funds"],
            statuses=filter_ctx["statuses"],
            sectors=filter_ctx["sectors"],
            geographies=filter_ctx["geographies"],
            vintages=filter_ctx["vintages"],
            exit_types=filter_ctx["exit_types"],
            lead_partners=filter_ctx["lead_partners"],
            security_types=filter_ctx["security_types"],
            deal_types=filter_ctx["deal_types"],
            entry_channels=filter_ctx["entry_channels"],
            current_fund=filter_ctx["current_fund"],
            current_status=filter_ctx["current_status"],
            current_sector=filter_ctx["current_sector"],
            current_geography=filter_ctx["current_geography"],
            current_vintage=filter_ctx["current_vintage"],
            current_exit_type=filter_ctx["current_exit_type"],
            current_lead_partner=filter_ctx["current_lead_partner"],
            current_security_type=filter_ctx["current_security_type"],
            current_deal_type=filter_ctx["current_deal_type"],
            current_entry_channel=filter_ctx["current_entry_channel"],
        )
    except Exception as exc:
        logger.exception("Dashboard computation failed")
        flash(f"Error computing dashboard metrics: {str(exc)}", "danger")
        return render_template("dashboard.html", **_empty_dashboard_context())


@app.route("/api/dashboard/series")
def dashboard_series_api():
    try:
        filter_ctx = _build_filtered_deals_context()
        payload = _build_dashboard_payload(filter_ctx["deals"])
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            raise
        filter_ctx = _build_filtered_deals_context()
        payload = _build_dashboard_payload(filter_ctx["deals"])
    return jsonify(
        {
            "kpis": payload["kpis"],
            "loss_ratios": payload["loss"],
            "moic_distribution": payload["moic_distribution"],
            "entry_exit_summary": payload["entry_exit_summary"],
            "bridge_aggregate": payload["bridge_aggregate"],
            "vintage_series": payload["vintage_series"],
            "moic_hold_scatter": payload["moic_hold_scatter"],
            "value_creation_mix": payload["value_creation_mix"],
            "realized_unrealized_exposure": payload["realized_unrealized_exposure"],
            "loss_concentration_heatmap": payload["loss_concentration_heatmap"],
            "exit_type_performance": payload["exit_type_performance"],
            "lead_partner_scorecard": payload["lead_partner_scorecard"],
        }
    )


@app.route("/analysis/<page>")
def analysis_page(page):
    if page not in ANALYSIS_PAGES:
        abort(404)

    try:
        filter_ctx = _build_filtered_deals_context()
        payload = _analysis_route_payload(page, filter_ctx["deals"])
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            raise
        filter_ctx = _build_filtered_deals_context()
        payload = _analysis_route_payload(page, filter_ctx["deals"])

    return render_template(
        "analysis_page.html",
        page_key=page,
        page_meta=ANALYSIS_PAGES[page],
        analysis=payload,
        **filter_ctx,
    )


@app.route("/api/analysis/<page>/series")
def analysis_series_api(page):
    if page not in ANALYSIS_PAGES:
        abort(404)

    try:
        filter_ctx = _build_filtered_deals_context()
        payload = _analysis_route_payload(page, filter_ctx["deals"])
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            raise
        filter_ctx = _build_filtered_deals_context()
        payload = _analysis_route_payload(page, filter_ctx["deals"])

    return jsonify(
        {
            "page": page,
            "title": ANALYSIS_PAGES[page]["title"],
            "payload": payload,
        }
    )


@app.route("/ic-memo")
@app.route("/ic-memo/<fund_name>")
def ic_memo(fund_name=None):
    try:
        filter_ctx = _build_filtered_deals_context(fund_override=fund_name)
        metrics_by_id = {d.id: compute_deal_metrics(d) for d in filter_ctx["deals"]}
        payload = compute_ic_memo_payload(
            filter_ctx["deals"],
            metrics_by_id=metrics_by_id,
            ranking_basis="weighted_moic",
            decile_pct=0.10,
            decile_min=1,
        )
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            raise
        filter_ctx = _build_filtered_deals_context(fund_override=fund_name)
        metrics_by_id = {d.id: compute_deal_metrics(d) for d in filter_ctx["deals"]}
        payload = compute_ic_memo_payload(
            filter_ctx["deals"],
            metrics_by_id=metrics_by_id,
            ranking_basis="weighted_moic",
            decile_pct=0.10,
            decile_min=1,
        )

    active_fund_scope = filter_ctx["current_fund"] or "All Funds"
    payload["meta"]["fund_scope"] = active_fund_scope
    payload["meta"]["filters_applied"] = {
        "fund": filter_ctx["current_fund"],
        "status": filter_ctx["current_status"],
        "sector": filter_ctx["current_sector"],
        "geography": filter_ctx["current_geography"],
        "vintage": filter_ctx["current_vintage"],
        "exit_type": filter_ctx["current_exit_type"],
        "lead_partner": filter_ctx["current_lead_partner"],
        "deal_type": filter_ctx["current_deal_type"],
        "entry_channel": filter_ctx["current_entry_channel"],
    }

    return render_template(
        "ic_memo.html",
        memo=payload,
        fund_scope_path=fund_name,
        **filter_ctx,
    )


@app.route("/methodology")
def methodology():
    payload = build_methodology_payload()
    return render_template("methodology.html", methodology=payload)


@app.route("/audit")
def methodology_alias():
    return redirect(url_for("methodology"))


@app.route("/api/deals/<int:deal_id>/bridge")
def deal_bridge_api(deal_id):
    try:
        deal = db.session.get(Deal, deal_id)
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            raise
        deal = db.session.get(Deal, deal_id)
    if deal is None:
        abort(404)

    model = request.args.get("model", "additive").lower()
    basis = "fund"
    unit = request.args.get("unit", "moic").lower()

    if model != "additive":
        return jsonify({"error": "Only additive model is supported"}), 400
    if request.args.get("basis") and request.args.get("basis", "fund").lower() != "fund":
        return jsonify({"error": "Only fund pro-rata basis is supported"}), 400
    if unit not in {"dollar", "moic", "pct"}:
        return jsonify({"error": "unit must be dollar, moic, or pct"}), 400

    warnings = []
    bridge = compute_bridge_view(deal, model="additive", basis=basis, unit=unit, warnings=warnings)

    equity = deal.equity_invested
    value_created = bridge.get("value_created")
    ownership = bridge.get("ownership_pct")

    if basis == "fund":
        start_dollar = equity if equity is not None else None
    else:
        start_dollar = (equity / ownership) if (equity is not None and ownership is not None and ownership > 0) else None

    end_dollar = (
        start_dollar + value_created
        if start_dollar is not None and value_created is not None
        else None
    )

    if unit == "dollar":
        start_value, end_value = start_dollar, end_dollar
    elif unit == "moic":
        start_value = 1.0
        driver_vals = bridge.get("drivers", {})
        if all(driver_vals.get(k) is not None for k in ("revenue", "margin", "multiple", "leverage", "other")):
            end_value = start_value + sum(driver_vals[k] for k in ("revenue", "margin", "multiple", "leverage", "other"))
        else:
            end_value = None
    else:  # pct
        start_value = 0.0
        end_value = 1.0 if bridge.get("ready") else None

    return jsonify(
        {
            "deal_id": deal.id,
            "company": deal.company_name,
            "model": "additive",
            "basis": basis,
            "unit": unit,
            "ready": bridge.get("ready"),
            "ownership_pct": bridge.get("ownership_pct"),
            "equity_invested": equity,
            "start_value": start_value,
            "end_value": end_value,
            "start_dollar": start_dollar,
            "end_dollar": end_dollar,
            "drivers": bridge.get("drivers"),
            "drivers_dollar": bridge.get("drivers_dollar"),
            "value_created": bridge.get("value_created"),
            "fund_value_created": bridge.get("fund_value_created"),
            "company_value_created": bridge.get("company_value_created"),
            "diagnostics": bridge.get("diagnostics", {}),
            "warnings": warnings,
        }
    )


@app.route("/upload")
def upload():
    return render_template("upload.html")


@app.route("/upload/deals/template")
def download_deal_template():
    template_path = Path(app.root_path) / DEAL_TEMPLATE_FILENAME
    if not template_path.exists():
        logger.error("Deal template file not found at %s", template_path)
        flash("Deal template file is unavailable right now.", "danger")
        return redirect(url_for("upload"))

    return send_file(
        template_path,
        as_attachment=True,
        download_name=DEAL_TEMPLATE_FILENAME,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/upload/deals", methods=["POST"])
def upload_deals():
    return _handle_upload(parse_deals, "deals")


@app.route("/deals")
def deals():
    try:
        all_deals = Deal.query.order_by(Deal.created_at.desc()).all()
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            raise
        all_deals = Deal.query.order_by(Deal.created_at.desc()).all()
    deal_metrics = {d.id: compute_deal_metrics(d) for d in all_deals}
    track_record = compute_deal_track_record(all_deals, metrics_by_id=deal_metrics)
    rollup_details = compute_deals_rollup_details(all_deals, track_record, metrics_by_id=deal_metrics)
    deals_by_id = {d.id: d for d in all_deals}
    return render_template(
        "deals.html",
        deals=all_deals,
        deal_metrics=deal_metrics,
        track_record=track_record,
        rollup_details=rollup_details,
        deals_by_id=deals_by_id,
    )


@app.route("/track-record")
def track_record():
    try:
        all_deals = Deal.query.order_by(Deal.fund_number.asc(), Deal.company_name.asc()).all()
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            raise
        all_deals = Deal.query.order_by(Deal.fund_number.asc(), Deal.company_name.asc()).all()

    metrics_by_id = {d.id: compute_deal_metrics(d) for d in all_deals}
    record = compute_deal_track_record(all_deals, metrics_by_id=metrics_by_id)
    return render_template("track_record.html", track_record=record, deals=all_deals)


@app.route("/track-record/pdf")
def download_track_record_pdf():
    try:
        all_deals = Deal.query.order_by(Deal.fund_number.asc(), Deal.company_name.asc()).all()
    except OperationalError as exc:
        if not _recover_missing_tables(exc):
            raise
        all_deals = Deal.query.order_by(Deal.fund_number.asc(), Deal.company_name.asc()).all()

    metrics_by_id = {d.id: compute_deal_metrics(d) for d in all_deals}
    record = compute_deal_track_record(all_deals, metrics_by_id=metrics_by_id)

    try:
        pdf_bytes = _build_track_record_pdf(record)
    except ImportError:
        flash("PDF export dependency missing. Install requirements and retry.", "danger")
        return redirect(url_for("track_record"))

    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name="track_record_print_ready.pdf",
        mimetype="application/pdf",
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5001"))
    app.run(debug=True, port=port)

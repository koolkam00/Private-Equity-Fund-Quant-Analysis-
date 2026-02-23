import logging
import os

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from config import Config
from models import Deal, db, ensure_schema_updates
from services.deal_parser import parse_deals
from services.metrics import (
    compute_bridge_aggregate,
    compute_bridge_view,
    compute_data_quality,
    compute_deal_metrics,
    compute_loss_and_distribution,
    compute_portfolio_analytics,
    compute_vintage_series,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_object(Config)
db.init_app(app)

os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
os.makedirs(os.path.join(os.path.dirname(__file__), "instance"), exist_ok=True)

with app.app_context():
    db.create_all()
    ensure_schema_updates()


def _allowed_file(filename):
    return os.path.splitext(filename)[1].lower() in app.config["ALLOWED_EXTENSIONS"]


def _deal_vintage_year(deal):
    if deal.year_invested is not None:
        return int(deal.year_invested)
    if deal.investment_date is not None:
        return int(deal.investment_date.year)
    return None


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
            "additive": {},
            "multiplicative": {},
            "diagnostics": {"driver_delta_dollar": {}, "low_confidence_count": 0, "ready_count": 0},
        },
        "vintage_series": [],
        "deal_metrics": {},
        "deals": [],
        "data_quality": {"total_deals": 0, "complete_deals": 0, "bridge_ready": 0, "warnings": []},
        "funds": [],
        "statuses": [],
        "sectors": [],
        "geographies": [],
        "vintages": [],
        "current_fund": "",
        "current_status": "",
        "current_sector": "",
        "current_geography": "",
        "current_vintage": "",
    }


def _build_filtered_deals_context():
    all_deals = Deal.query.all()

    funds = sorted({d.fund_number for d in all_deals if d.fund_number})
    statuses = sorted({d.status for d in all_deals if d.status})
    sectors = sorted({d.sector for d in all_deals if d.sector})
    geographies = sorted({d.geography for d in all_deals if d.geography})
    vintages = sorted({_deal_vintage_year(d) for d in all_deals if _deal_vintage_year(d) is not None})

    current_fund = request.args.get("fund", "")
    current_status = request.args.get("status", "")
    current_sector = request.args.get("sector", "")
    current_geography = request.args.get("geography", "")
    current_vintage = request.args.get("vintage", "")

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

    return {
        "deals": filtered,
        "funds": funds,
        "statuses": statuses,
        "sectors": sectors,
        "geographies": geographies,
        "vintages": vintages,
        "current_fund": current_fund,
        "current_status": current_status,
        "current_sector": current_sector,
        "current_geography": current_geography,
        "current_vintage": current_vintage,
    }


def _aggregate_bridge_diagnostics(metrics_by_id):
    deltas = {k: [] for k in ("revenue", "margin", "multiple", "leverage", "other")}
    low_confidence = 0
    ready = 0

    for m in metrics_by_id.values():
        diag = m.get("bridge_diagnostics", {})
        if diag.get("low_confidence_bridge"):
            low_confidence += 1

        bridge_add = m.get("bridge_additive_fund", {})
        if bridge_add.get("ready"):
            ready += 1

        for k, v in (diag.get("driver_delta_dollar") or {}).items():
            if v is not None:
                deltas[k].append(v)

    delta_avg = {k: (sum(vs) / len(vs) if vs else None) for k, vs in deltas.items()}
    return {
        "driver_delta_dollar": delta_avg,
        "low_confidence_count": low_confidence,
        "ready_count": ready,
    }


def _build_dashboard_payload(filtered_deals):
    metrics_by_id = {d.id: compute_deal_metrics(d) for d in filtered_deals}

    portfolio = compute_portfolio_analytics(filtered_deals, metrics_by_id=metrics_by_id)
    risk = compute_loss_and_distribution(filtered_deals, metrics_by_id=metrics_by_id)
    additive = compute_bridge_aggregate(filtered_deals, model="additive", basis="fund")
    multiplicative = compute_bridge_aggregate(filtered_deals, model="multiplicative", basis="fund")
    vintage = compute_vintage_series(filtered_deals, metrics_by_id=metrics_by_id)
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
        "bridge_aggregate": {
            "additive": additive,
            "multiplicative": multiplicative,
            "diagnostics": _aggregate_bridge_diagnostics(metrics_by_id),
        },
        "vintage_series": vintage,
        "deal_metrics": metrics_by_id,
        "data_quality": quality,
    }


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
            current_fund=filter_ctx["current_fund"],
            current_status=filter_ctx["current_status"],
            current_sector=filter_ctx["current_sector"],
            current_geography=filter_ctx["current_geography"],
            current_vintage=filter_ctx["current_vintage"],
        )
    except Exception as exc:
        logger.exception("Dashboard computation failed")
        flash(f"Error computing dashboard metrics: {str(exc)}", "danger")
        return render_template("dashboard.html", **_empty_dashboard_context())


@app.route("/api/dashboard/series")
def dashboard_series_api():
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
        }
    )


@app.route("/api/deals/<int:deal_id>/bridge")
def deal_bridge_api(deal_id):
    deal = db.session.get(Deal, deal_id)
    if deal is None:
        abort(404)

    model = request.args.get("model", "additive").lower()
    basis = request.args.get("basis", "fund").lower()
    unit = request.args.get("unit", "moic").lower()

    if model not in {"additive", "multiplicative"}:
        return jsonify({"error": "model must be additive or multiplicative"}), 400
    if basis not in {"fund", "company"}:
        return jsonify({"error": "basis must be fund or company"}), 400
    if unit not in {"dollar", "moic", "pct"}:
        return jsonify({"error": "unit must be dollar, moic, or pct"}), 400

    warnings = []
    bridge = compute_bridge_view(deal, model=model, basis=basis, unit=unit, warnings=warnings)

    add_fund = compute_bridge_view(deal, model="additive", basis="fund", unit="dollar", warnings=[])
    mul_fund = compute_bridge_view(deal, model="multiplicative", basis="fund", unit="dollar", warnings=[])

    diagnostics = {
        "low_confidence_bridge": bool(mul_fund.get("low_confidence_bridge")),
        "driver_delta_dollar": {
            k: (
                (mul_fund.get("fund_drivers_dollar", {}).get(k) - add_fund.get("fund_drivers_dollar", {}).get(k))
                if add_fund.get("fund_drivers_dollar", {}).get(k) is not None and mul_fund.get("fund_drivers_dollar", {}).get(k) is not None
                else None
            )
            for k in ("revenue", "margin", "multiple", "leverage", "other")
        },
    }

    return jsonify(
        {
            "deal_id": deal.id,
            "company": deal.company_name,
            "model": model,
            "basis": basis,
            "unit": unit,
            "ready": bridge.get("ready"),
            "low_confidence_bridge": bridge.get("low_confidence_bridge"),
            "ownership_pct": bridge.get("ownership_pct"),
            "drivers": bridge.get("drivers"),
            "drivers_dollar": bridge.get("drivers_dollar"),
            "value_created": bridge.get("value_created"),
            "fund_value_created": bridge.get("fund_value_created"),
            "company_value_created": bridge.get("company_value_created"),
            "diagnostics": diagnostics,
            "warnings": warnings,
        }
    )


@app.route("/upload")
def upload():
    return render_template("upload.html")


@app.route("/upload/deals", methods=["POST"])
def upload_deals():
    return _handle_upload(parse_deals, "deals")


@app.route("/deals")
def deals():
    all_deals = Deal.query.order_by(Deal.created_at.desc()).all()
    deal_metrics = {d.id: compute_deal_metrics(d) for d in all_deals}
    return render_template("deals.html", deals=all_deals, deal_metrics=deal_metrics)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5001"))
    app.run(debug=True, port=port)

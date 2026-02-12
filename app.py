import os

from flask import Flask, render_template, request, redirect, url_for, flash
from sqlalchemy import func
from werkzeug.utils import secure_filename

from config import Config
from models import db, Deal, Cashflow
from services.deal_parser import parse_deals
from services.cashflow_parser import parse_cashflows

app = Flask(__name__)
app.config.from_object(Config)

db.init_app(app)

# Ensure directories exist
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
os.makedirs(os.path.join(os.path.dirname(__file__), "instance"), exist_ok=True)

with app.app_context():
    db.create_all()


def _compute_deal_metrics(deal):
    """Compute derived performance metrics for a single deal."""
    m = {}
    # Hold Period (years)
    if deal.investment_date and deal.exit_date:
        delta = deal.exit_date - deal.investment_date
        m["hold_period"] = delta.days / 365.25
    else:
        m["hold_period"] = None

    # Revenue Growth %
    if deal.entry_revenue and deal.exit_revenue and deal.entry_revenue != 0:
        m["revenue_growth"] = (deal.exit_revenue - deal.entry_revenue) / deal.entry_revenue * 100
    else:
        m["revenue_growth"] = None

    # Revenue Growth CAGR %
    if m["revenue_growth"] is not None and m["hold_period"] and m["hold_period"] > 0:
        m["revenue_cagr"] = ((deal.exit_revenue / deal.entry_revenue) ** (1 / m["hold_period"]) - 1) * 100
    else:
        m["revenue_cagr"] = None

    # EBITDA Growth %
    if deal.entry_ebitda and deal.exit_ebitda and deal.entry_ebitda != 0:
        m["ebitda_growth"] = (deal.exit_ebitda - deal.entry_ebitda) / deal.entry_ebitda * 100
    else:
        m["ebitda_growth"] = None

    # EBITDA Growth CAGR %
    if m["ebitda_growth"] is not None and m["hold_period"] and m["hold_period"] > 0:
        m["ebitda_cagr"] = ((deal.exit_ebitda / deal.entry_ebitda) ** (1 / m["hold_period"]) - 1) * 100
    else:
        m["ebitda_cagr"] = None

    return m


def _compute_portfolio_analytics(deals):
    """Compute simple averages and equity-weighted averages across deals."""
    metrics = []
    for d in deals:
        m = _compute_deal_metrics(d)
        m["equity"] = d.equity_invested or 0
        m["moic"] = d.moic
        m["irr"] = d.irr if d.irr is not None else None
        metrics.append(m)

    fields = [
        "revenue_growth", "revenue_cagr", "ebitda_growth", "ebitda_cagr",
        "hold_period", "moic", "irr",
    ]
    result = {}
    for f in fields:
        vals = [(m[f], m["equity"]) for m in metrics if m[f] is not None]
        if vals:
            result[f"avg_{f}"] = sum(v for v, _ in vals) / len(vals)
            total_w = sum(w for _, w in vals)
            if total_w > 0:
                result[f"wavg_{f}"] = sum(v * w for v, w in vals) / total_w
            else:
                result[f"wavg_{f}"] = result[f"avg_{f}"]
        else:
            result[f"avg_{f}"] = None
            result[f"wavg_{f}"] = None
    return result


def _allowed_file(filename):
    ext = os.path.splitext(filename)[1].lower()
    return ext in app.config["ALLOWED_EXTENSIONS"]


def _handle_upload(parse_func, redirect_route):
    """Shared logic for deal and cashflow uploads."""
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
            flash(
                f"Successfully imported {result['success']} records (batch {result['batch_id']}).",
                "success",
            )
        if result["errors"]:
            for err in result["errors"][:10]:
                flash(err, "warning")
            if len(result["errors"]) > 10:
                flash(f"...and {len(result['errors']) - 10} more warnings.", "warning")
        if result["success"] == 0 and not result["errors"]:
            flash("No records found in the file.", "warning")
    except Exception as e:
        flash(f"Error processing file: {str(e)}", "danger")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    return redirect(url_for(redirect_route))


@app.route("/")
def index():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    # --- Filter dropdown options (always unfiltered) ---
    funds = [
        r[0] for r in db.session.query(Deal.fund_number).distinct()
        if r[0]
    ]
    statuses = [
        r[0] for r in db.session.query(Deal.status).distinct()
        if r[0]
    ]
    sectors = [
        r[0] for r in db.session.query(Deal.sector).distinct()
        if r[0]
    ]
    funds.sort()
    statuses.sort()
    sectors.sort()

    # --- Read current filter values ---
    current_fund = request.args.get("fund", "")
    current_status = request.args.get("status", "")
    current_sector = request.args.get("sector", "")

    # --- Build filtered deal query ---
    deal_query = Deal.query
    if current_fund:
        deal_query = deal_query.filter(Deal.fund_number == current_fund)
    if current_status:
        deal_query = deal_query.filter(Deal.status == current_status)
    if current_sector:
        deal_query = deal_query.filter(Deal.sector == current_sector)

    filtered_deals = deal_query.all()

    # --- KPIs from filtered deals ---
    total_deals = len(filtered_deals)
    fully_realized = sum(1 for d in filtered_deals if d.status == "Fully Realized")
    partially_realized = sum(1 for d in filtered_deals if d.status == "Partially Realized")
    unrealized = sum(1 for d in filtered_deals if d.status == "Unrealized")

    total_equity = sum(d.equity_invested or 0 for d in filtered_deals)
    moic_vals = [d.moic for d in filtered_deals if d.moic is not None]
    avg_moic = sum(moic_vals) / len(moic_vals) if moic_vals else 0
    irr_vals = [d.irr for d in filtered_deals if d.irr is not None]
    avg_irr = sum(irr_vals) / len(irr_vals) if irr_vals else 0

    # --- Cashflow sums (filtered by matching company names / funds) ---
    cf_query = db.session.query(
        func.sum(Cashflow.capital_called),
        func.sum(Cashflow.distributions),
    )
    if current_fund:
        cf_query = cf_query.filter(Cashflow.fund_number == current_fund)
    if current_fund or current_status or current_sector:
        # Further restrict to companies in the filtered deal set
        company_names = [d.company_name for d in filtered_deals]
        if company_names:
            cf_query = cf_query.filter(Cashflow.company_name.in_(company_names))
        else:
            cf_query = cf_query.filter(False)
    cf_agg = cf_query.first()
    total_capital_called = cf_agg[0] or 0
    total_distributions = cf_agg[1] or 0

    # --- Portfolio analytics (simple avg + equity-weighted avg) ---
    analytics = _compute_portfolio_analytics(filtered_deals)

    # --- Recent deals (filtered) ---
    recent_deals = deal_query.order_by(Deal.created_at.desc()).limit(5).all()

    return render_template(
        "dashboard.html",
        total_deals=total_deals,
        fully_realized=fully_realized,
        partially_realized=partially_realized,
        unrealized=unrealized,
        total_equity=total_equity,
        avg_moic=avg_moic,
        avg_irr=avg_irr,
        total_capital_called=total_capital_called,
        total_distributions=total_distributions,
        recent_deals=recent_deals,
        analytics=analytics,
        funds=funds,
        statuses=statuses,
        sectors=sectors,
        current_fund=current_fund,
        current_status=current_status,
        current_sector=current_sector,
    )


@app.route("/upload")
def upload():
    return render_template("upload.html")


@app.route("/upload/deals", methods=["POST"])
def upload_deals():
    return _handle_upload(parse_deals, "deals")


@app.route("/upload/cashflows", methods=["POST"])
def upload_cashflows():
    return _handle_upload(parse_cashflows, "cashflows")


@app.route("/deals")
def deals():
    all_deals = Deal.query.order_by(Deal.created_at.desc()).all()
    deal_metrics = {d.id: _compute_deal_metrics(d) for d in all_deals}
    return render_template("deals.html", deals=all_deals, deal_metrics=deal_metrics)


@app.route("/cashflows")
def cashflows():
    all_cashflows = Cashflow.query.order_by(
        Cashflow.company_name, Cashflow.date
    ).all()
    return render_template("cashflows.html", cashflows=all_cashflows)


if __name__ == "__main__":
    app.run(debug=True, port=5000)

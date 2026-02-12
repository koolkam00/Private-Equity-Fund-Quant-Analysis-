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
    if (m["revenue_growth"] is not None
            and m["hold_period"] and m["hold_period"] > 0
            and deal.entry_revenue > 0 and deal.exit_revenue > 0):
        try:
            m["revenue_cagr"] = ((deal.exit_revenue / deal.entry_revenue) ** (1 / m["hold_period"]) - 1) * 100
        except (ValueError, ZeroDivisionError, OverflowError):
            m["revenue_cagr"] = None
    else:
        m["revenue_cagr"] = None

    # EBITDA Growth %
    if deal.entry_ebitda and deal.exit_ebitda and deal.entry_ebitda != 0:
        m["ebitda_growth"] = (deal.exit_ebitda - deal.entry_ebitda) / deal.entry_ebitda * 100
    else:
        m["ebitda_growth"] = None

    # EBITDA Growth CAGR %
    if (m["ebitda_growth"] is not None
            and m["hold_period"] and m["hold_period"] > 0
            and deal.entry_ebitda > 0 and deal.exit_ebitda > 0):
        try:
            m["ebitda_cagr"] = ((deal.exit_ebitda / deal.entry_ebitda) ** (1 / m["hold_period"]) - 1) * 100
        except (ValueError, ZeroDivisionError, OverflowError):
            m["ebitda_cagr"] = None
    else:
        m["ebitda_cagr"] = None

    # --- Financial Ratios (entry) ---

    # Entry EV/EBITDA
    if deal.entry_enterprise_value is not None and deal.entry_ebitda and deal.entry_ebitda != 0:
        m["entry_ev_ebitda"] = deal.entry_enterprise_value / deal.entry_ebitda
    else:
        m["entry_ev_ebitda"] = None

    # Entry EV/Revenue
    if deal.entry_enterprise_value is not None and deal.entry_revenue and deal.entry_revenue != 0:
        m["entry_ev_revenue"] = deal.entry_enterprise_value / deal.entry_revenue
    else:
        m["entry_ev_revenue"] = None

    # Entry Net Debt/EBITDA
    if deal.entry_net_debt is not None and deal.entry_ebitda and deal.entry_ebitda != 0:
        m["entry_net_debt_ebitda"] = deal.entry_net_debt / deal.entry_ebitda
    else:
        m["entry_net_debt_ebitda"] = None

    # Entry EBITDA Margin (%)
    if deal.entry_ebitda is not None and deal.entry_revenue and deal.entry_revenue != 0:
        m["entry_ebitda_margin"] = deal.entry_ebitda / deal.entry_revenue * 100
    else:
        m["entry_ebitda_margin"] = None

    # Entry Net Debt/EV (%)
    if deal.entry_net_debt is not None and deal.entry_enterprise_value and deal.entry_enterprise_value != 0:
        m["entry_net_debt_ev"] = deal.entry_net_debt / deal.entry_enterprise_value * 100
    else:
        m["entry_net_debt_ev"] = None

    # --- Financial Ratios (exit) ---

    # Exit EV/EBITDA
    if deal.exit_enterprise_value is not None and deal.exit_ebitda and deal.exit_ebitda != 0:
        m["exit_ev_ebitda"] = deal.exit_enterprise_value / deal.exit_ebitda
    else:
        m["exit_ev_ebitda"] = None

    # Exit EV/Revenue
    if deal.exit_enterprise_value is not None and deal.exit_revenue and deal.exit_revenue != 0:
        m["exit_ev_revenue"] = deal.exit_enterprise_value / deal.exit_revenue
    else:
        m["exit_ev_revenue"] = None

    # Exit Net Debt/EBITDA
    if deal.exit_net_debt is not None and deal.exit_ebitda and deal.exit_ebitda != 0:
        m["exit_net_debt_ebitda"] = deal.exit_net_debt / deal.exit_ebitda
    else:
        m["exit_net_debt_ebitda"] = None

    # Exit EBITDA Margin (%)
    if deal.exit_ebitda is not None and deal.exit_revenue and deal.exit_revenue != 0:
        m["exit_ebitda_margin"] = deal.exit_ebitda / deal.exit_revenue * 100
    else:
        m["exit_ebitda_margin"] = None

    # Exit Net Debt/EV (%)
    if deal.exit_net_debt is not None and deal.exit_enterprise_value and deal.exit_enterprise_value != 0:
        m["exit_net_debt_ev"] = deal.exit_net_debt / deal.exit_enterprise_value * 100
    else:
        m["exit_net_debt_ev"] = None

    # --- Value Creation Bridge (Additive Decomposition) ---
    # Requires all 8 entry/exit operating fields, non-zero denominators, and equity_invested
    _vb_fields = [
        deal.entry_revenue, deal.exit_revenue,
        deal.entry_ebitda, deal.exit_ebitda,
        deal.entry_enterprise_value, deal.exit_enterprise_value,
        deal.entry_net_debt, deal.exit_net_debt,
    ]
    _can_bridge = (
        all(v is not None for v in _vb_fields)
        and deal.entry_ebitda != 0
        and deal.entry_revenue != 0
        and deal.equity_invested and deal.equity_invested > 0
    )
    if _can_bridge:
        entry_margin = deal.entry_ebitda / deal.entry_revenue  # decimal
        exit_margin = deal.exit_ebitda / deal.exit_revenue if deal.exit_revenue != 0 else 0
        entry_multiple = deal.entry_enterprise_value / deal.entry_ebitda
        exit_multiple = deal.exit_enterprise_value / deal.exit_ebitda if deal.exit_ebitda != 0 else 0

        entry_equity = deal.entry_enterprise_value - deal.entry_net_debt
        exit_equity = deal.exit_enterprise_value - deal.exit_net_debt
        total_value_created = exit_equity - entry_equity

        vb_rev = (deal.exit_revenue - deal.entry_revenue) * entry_margin * entry_multiple
        vb_margin = deal.exit_revenue * (exit_margin - entry_margin) * entry_multiple
        vb_multiple = (exit_multiple - entry_multiple) * deal.exit_ebitda
        vb_leverage = deal.entry_net_debt - deal.exit_net_debt
        vb_other = total_value_created - (vb_rev + vb_margin + vb_multiple + vb_leverage)

        m["vb_revenue_growth"] = vb_rev
        m["vb_margin_expansion"] = vb_margin
        m["vb_multiple_expansion"] = vb_multiple
        m["vb_leverage"] = vb_leverage
        m["vb_other"] = vb_other
        eq = deal.equity_invested
        m["vb_revenue_growth_moic"] = vb_rev / eq
        m["vb_margin_expansion_moic"] = vb_margin / eq
        m["vb_multiple_expansion_moic"] = vb_multiple / eq
        m["vb_leverage_moic"] = vb_leverage / eq
        m["vb_other_moic"] = vb_other / eq
    else:
        for k in ["vb_revenue_growth", "vb_margin_expansion", "vb_multiple_expansion",
                   "vb_leverage", "vb_other", "vb_revenue_growth_moic",
                   "vb_margin_expansion_moic", "vb_multiple_expansion_moic",
                   "vb_leverage_moic", "vb_other_moic"]:
            m[k] = None

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
        "entry_ev_ebitda", "entry_ev_revenue", "entry_net_debt_ebitda",
        "entry_ebitda_margin", "entry_net_debt_ev",
        "exit_ev_ebitda", "exit_ev_revenue", "exit_net_debt_ebitda",
        "exit_ebitda_margin", "exit_net_debt_ev",
        "vb_revenue_growth_moic", "vb_margin_expansion_moic",
        "vb_multiple_expansion_moic", "vb_leverage_moic", "vb_other_moic",
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


def _compute_fund_metrics(cashflows):
    """Compute DPI, RVPI, TVPI from cashflow records."""
    total_called = sum(cf.capital_called or 0 for cf in cashflows)
    total_distributed = sum(cf.distributions or 0 for cf in cashflows)

    # Latest NAV per company (last observed NAV by date)
    latest_navs = {}
    for cf in sorted(cashflows, key=lambda c: c.date):
        if cf.nav is not None:
            latest_navs[cf.company_name] = cf.nav
    total_nav = sum(latest_navs.values()) if latest_navs else 0

    if total_called > 0:
        dpi = total_distributed / total_called
        rvpi = total_nav / total_called
        tvpi = dpi + rvpi
    else:
        dpi = rvpi = tvpi = None

    return {
        "total_called": total_called,
        "total_distributed": total_distributed,
        "total_nav": total_nav,
        "dpi": dpi,
        "rvpi": rvpi,
        "tvpi": tvpi,
    }


def _compute_risk_metrics(deals):
    """Compute portfolio risk and concentration analysis."""
    result = {}

    # --- Loss Ratio ---
    deals_with_moic = [d for d in deals if d.moic is not None]
    if deals_with_moic:
        losses = [d for d in deals_with_moic if d.moic < 1.0]
        result["loss_ratio_count"] = len(losses) / len(deals_with_moic) * 100
        total_eq = sum(d.equity_invested or 0 for d in deals_with_moic)
        loss_eq = sum(d.equity_invested or 0 for d in losses)
        result["loss_ratio_capital"] = (loss_eq / total_eq * 100) if total_eq > 0 else None
        result["loss_count"] = len(losses)
        result["total_with_moic"] = len(deals_with_moic)
    else:
        result["loss_ratio_count"] = None
        result["loss_ratio_capital"] = None
        result["loss_count"] = 0
        result["total_with_moic"] = 0

    # --- MOIC Distribution Buckets ---
    buckets = [
        ("<0.5x", 0, 0.5), ("0.5-1.0x", 0.5, 1.0), ("1.0-1.5x", 1.0, 1.5),
        ("1.5-2.0x", 1.5, 2.0), ("2.0-3.0x", 2.0, 3.0), ("3.0x+", 3.0, float("inf")),
    ]
    moic_dist = []
    for label, lo, hi in buckets:
        count = sum(1 for d in deals_with_moic if lo <= d.moic < hi)
        pct = count / len(deals_with_moic) * 100 if deals_with_moic else 0
        moic_dist.append({"label": label, "count": count, "pct": pct})
    result["moic_distribution"] = moic_dist

    # --- Concentration ---
    eq_deals = [
        (d.equity_invested, d.company_name, d.sector)
        for d in deals if d.equity_invested and d.equity_invested > 0
    ]
    eq_deals.sort(key=lambda x: x[0], reverse=True)
    total_eq = sum(e for e, _, _ in eq_deals)

    if total_eq > 0 and len(eq_deals) >= 5:
        result["top5_concentration"] = sum(e for e, _, _ in eq_deals[:5]) / total_eq * 100
        result["top5_deals"] = [(n, e, e / total_eq * 100) for e, n, _ in eq_deals[:5]]
    else:
        result["top5_concentration"] = None
        result["top5_deals"] = []

    # --- Sector Breakdown ---
    sector_eq = {}
    for e, _, s in eq_deals:
        sector_eq[s or "Unknown"] = sector_eq.get(s or "Unknown", 0) + e
    if total_eq > 0:
        result["sector_breakdown"] = sorted(
            [{"sector": s, "equity": v, "pct": v / total_eq * 100}
             for s, v in sector_eq.items()],
            key=lambda x: x["equity"], reverse=True,
        )
    else:
        result["sector_breakdown"] = []

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

    # --- Fund-level metrics (DPI / RVPI / TVPI) ---
    cf_filter_query = Cashflow.query
    if current_fund:
        cf_filter_query = cf_filter_query.filter(Cashflow.fund_number == current_fund)
    if current_fund or current_status or current_sector:
        company_names_for_cf = [d.company_name for d in filtered_deals]
        if company_names_for_cf:
            cf_filter_query = cf_filter_query.filter(
                Cashflow.company_name.in_(company_names_for_cf)
            )
        else:
            cf_filter_query = cf_filter_query.filter(False)
    fund_metrics = _compute_fund_metrics(cf_filter_query.all())

    # --- Risk metrics ---
    risk = _compute_risk_metrics(filtered_deals)

    # --- Vintage Year Analysis ---
    vintage_map = {}
    for d in filtered_deals:
        yr = d.investment_date.year if d.investment_date else None
        if yr is None:
            continue
        vintage_map.setdefault(yr, []).append(d)

    vintage_years = []
    for yr in sorted(vintage_map):
        ds = vintage_map[yr]
        moics = [d.moic for d in ds if d.moic is not None]
        irrs = [d.irr for d in ds if d.irr is not None]
        vintage_years.append({
            "year": yr,
            "deal_count": len(ds),
            "total_equity": sum(d.equity_invested or 0 for d in ds),
            "avg_moic": sum(moics) / len(moics) if moics else None,
            "avg_irr": sum(irrs) / len(irrs) if irrs else None,
        })

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
        fund_metrics=fund_metrics,
        risk=risk,
        vintage_years=vintage_years,
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

import os

from flask import Flask, render_template, request, redirect, url_for, flash
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
    return redirect(url_for("upload"))


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
    return render_template("deals.html", deals=all_deals)


@app.route("/cashflows")
def cashflows():
    all_cashflows = Cashflow.query.order_by(
        Cashflow.company_name, Cashflow.date
    ).all()
    return render_template("cashflows.html", cashflows=all_cashflows)


if __name__ == "__main__":
    app.run(debug=True, port=5000)

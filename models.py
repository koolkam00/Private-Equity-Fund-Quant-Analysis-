from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect, text


db = SQLAlchemy()


class Deal(db.Model):
    __tablename__ = "deals"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # Core identification
    company_name = db.Column(db.String(255), nullable=False)
    fund_number = db.Column(db.String(50), nullable=True)
    sector = db.Column(db.String(128), nullable=True)
    geography = db.Column(db.String(128), nullable=True)
    status = db.Column(db.String(50), nullable=True, default="Unrealized")

    # Dates
    investment_date = db.Column(db.Date, nullable=True)
    year_invested = db.Column(db.Integer, nullable=True)
    exit_date = db.Column(db.Date, nullable=True)

    # Equity / ownership
    equity_invested = db.Column(db.Float, nullable=True)
    ownership_pct = db.Column(db.Float, nullable=True)  # decimal, e.g. 0.25 = 25%

    # Entry operating metrics
    entry_revenue = db.Column(db.Float, nullable=True)
    entry_ebitda = db.Column(db.Float, nullable=True)
    entry_enterprise_value = db.Column(db.Float, nullable=True)
    entry_net_debt = db.Column(db.Float, nullable=True)

    # Exit operating metrics
    exit_revenue = db.Column(db.Float, nullable=True)
    exit_ebitda = db.Column(db.Float, nullable=True)
    exit_enterprise_value = db.Column(db.Float, nullable=True)
    exit_net_debt = db.Column(db.Float, nullable=True)

    # Performance metrics
    realized_value = db.Column(db.Float, nullable=True)
    unrealized_value = db.Column(db.Float, nullable=True)
    irr = db.Column(db.Float, nullable=True)  # optional legacy uploaded IRR (ignored in analytics)

    # Metadata
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<Deal {self.company_name} ({self.status})>"


class UploadIssue(db.Model):
    __tablename__ = "upload_issues"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    issue_report_id = db.Column(db.String(36), nullable=False, index=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    file_type = db.Column(db.String(32), nullable=False, default="deals")
    row_number = db.Column(db.Integer, nullable=True)
    company_name = db.Column(db.String(255), nullable=True)
    severity = db.Column(db.String(16), nullable=False, default="warning")
    message = db.Column(db.String(500), nullable=False)
    payload = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<UploadIssue {self.file_type} row={self.row_number} severity={self.severity}>"


def _column_exists(inspector, table_name, column_name):
    return any(c["name"] == column_name for c in inspector.get_columns(table_name))


def _ensure_column(engine, inspector, table_name, column_name, sql_type, default_sql=None):
    if _column_exists(inspector, table_name, column_name):
        return
    default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type}{default_clause}"))


def _archive_legacy_cashflows(engine, inspector):
    if "cashflows" not in inspector.get_table_names():
        return
    if "cashflows_legacy_backup" in inspector.get_table_names():
        return
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE cashflows_legacy_backup AS SELECT * FROM cashflows"))


def ensure_schema_updates():
    """Additive, idempotent schema upgrade helper for SQLite deployments.

    Runtime behavior is deal-only. Legacy cashflow tables are archived but never used.
    """
    engine = db.engine
    inspector = inspect(engine)

    # Deal-only expansion
    _ensure_column(engine, inspector, "deals", "geography", "VARCHAR(128)")
    _ensure_column(engine, inspector, "deals", "year_invested", "INTEGER")
    _ensure_column(engine, inspector, "deals", "ownership_pct", "FLOAT")

    # Legacy compatibility fields (if historically present they remain optional)
    _ensure_column(engine, inspector, "deals", "irr", "FLOAT")

    if "upload_issues" not in inspector.get_table_names():
        UploadIssue.__table__.create(bind=engine)

    # Archive legacy cashflow table once if it exists.
    _archive_legacy_cashflows(engine, inspector)

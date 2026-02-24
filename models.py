from flask_login import UserMixin
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import ForeignKey, UniqueConstraint, inspect, text


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
    exit_type = db.Column(db.String(128), nullable=True)
    lead_partner = db.Column(db.String(128), nullable=True)
    security_type = db.Column(db.String(128), nullable=True)
    deal_type = db.Column(db.String(128), nullable=True)
    entry_channel = db.Column(db.String(128), nullable=True)

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
    fund_size = db.Column(db.Float, nullable=True)  # optional fund size in same units as invested values ($M)
    net_irr = db.Column(db.Float, nullable=True)  # optional fund-level net IRR (decimal form, e.g. 0.258)
    net_moic = db.Column(db.Float, nullable=True)  # optional fund-level net MOIC
    net_dpi = db.Column(db.Float, nullable=True)  # optional fund-level net DPI

    # Metadata
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=True, index=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<Deal {self.company_name} ({self.status})>"


class DealCashflowEvent(db.Model):
    __tablename__ = "deal_cashflow_events"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    deal_id = db.Column(db.Integer, ForeignKey("deals.id"), nullable=False, index=True)
    event_date = db.Column(db.Date, nullable=False)
    event_type = db.Column(db.String(64), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    notes = db.Column(db.String(500), nullable=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=True, index=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<DealCashflowEvent deal_id={self.deal_id} type={self.event_type}>"


class DealQuarterSnapshot(db.Model):
    __tablename__ = "deal_quarter_snapshots"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    deal_id = db.Column(db.Integer, ForeignKey("deals.id"), nullable=False, index=True)
    quarter_end = db.Column(db.Date, nullable=False, index=True)
    revenue = db.Column(db.Float, nullable=True)
    ebitda = db.Column(db.Float, nullable=True)
    enterprise_value = db.Column(db.Float, nullable=True)
    net_debt = db.Column(db.Float, nullable=True)
    equity_value = db.Column(db.Float, nullable=True)
    valuation_basis = db.Column(db.String(128), nullable=True)
    source = db.Column(db.String(128), nullable=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=True, index=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<DealQuarterSnapshot deal_id={self.deal_id} quarter_end={self.quarter_end}>"


class FundQuarterSnapshot(db.Model):
    __tablename__ = "fund_quarter_snapshots"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    fund_number = db.Column(db.String(50), nullable=False, index=True)
    quarter_end = db.Column(db.Date, nullable=False, index=True)
    committed_capital = db.Column(db.Float, nullable=True)
    paid_in_capital = db.Column(db.Float, nullable=True)
    distributed_capital = db.Column(db.Float, nullable=True)
    nav = db.Column(db.Float, nullable=True)
    unfunded_commitment = db.Column(db.Float, nullable=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=True, index=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<FundQuarterSnapshot fund={self.fund_number} quarter_end={self.quarter_end}>"


class DealUnderwriteBaseline(db.Model):
    __tablename__ = "deal_underwrite_baselines"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    deal_id = db.Column(db.Integer, ForeignKey("deals.id"), nullable=False, index=True)
    target_irr = db.Column(db.Float, nullable=True)
    target_moic = db.Column(db.Float, nullable=True)
    target_hold_years = db.Column(db.Float, nullable=True)
    target_exit_multiple = db.Column(db.Float, nullable=True)
    target_revenue_cagr = db.Column(db.Float, nullable=True)
    target_ebitda_cagr = db.Column(db.Float, nullable=True)
    baseline_date = db.Column(db.Date, nullable=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=True, index=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<DealUnderwriteBaseline deal_id={self.deal_id}>"


class UploadIssue(db.Model):
    __tablename__ = "upload_issues"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    issue_report_id = db.Column(db.String(36), nullable=False, index=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=True, index=True)
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


class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    email = db.Column(db.String(255), nullable=False, unique=True, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    last_login_at = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f"<User {self.email}>"


class Firm(db.Model):
    __tablename__ = "firms"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(255), nullable=False, unique=True, index=True)
    slug = db.Column(db.String(255), nullable=False, unique=True, index=True)
    base_currency = db.Column(db.String(3), nullable=False, default="USD")
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<Firm {self.name}>"


class Team(db.Model):
    __tablename__ = "teams"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), nullable=False, unique=True, index=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<Team {self.name}>"


class TeamMembership(db.Model):
    __tablename__ = "team_memberships"
    __table_args__ = (
        UniqueConstraint("team_id", "user_id", name="uq_team_memberships_team_user"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    user_id = db.Column(db.Integer, ForeignKey("users.id"), nullable=False, index=True)
    role = db.Column(db.String(16), nullable=False, default="member")
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<TeamMembership team={self.team_id} user={self.user_id} role={self.role}>"


class TeamFirmAccess(db.Model):
    __tablename__ = "team_firm_access"
    __table_args__ = (
        UniqueConstraint("team_id", "firm_id", name="uq_team_firm_access_team_firm"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=False, index=True)
    created_by_user_id = db.Column(db.Integer, ForeignKey("users.id"), nullable=True, index=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<TeamFirmAccess team={self.team_id} firm={self.firm_id}>"


class TeamInvite(db.Model):
    __tablename__ = "team_invites"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    email = db.Column(db.String(255), nullable=False, index=True)
    token_hash = db.Column(db.String(64), nullable=False, unique=True, index=True)
    expires_at = db.Column(db.DateTime, nullable=False)
    accepted_at = db.Column(db.DateTime, nullable=True)
    created_by_user_id = db.Column(db.Integer, ForeignKey("users.id"), nullable=False, index=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<TeamInvite team={self.team_id} email={self.email}>"


def _column_exists(inspector, table_name, column_name):
    return any(c["name"] == column_name for c in inspector.get_columns(table_name))


def _ensure_column(engine, inspector, table_name, column_name, sql_type, default_sql=None):
    if table_name not in inspector.get_table_names():
        return
    if _column_exists(inspector, table_name, column_name):
        return
    default_clause = f" DEFAULT {default_sql}" if default_sql is not None else ""
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type}{default_clause}"))


def _ensure_index(engine, index_name, table_name, column_name):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name})"))


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

    if "deals" not in inspector.get_table_names():
        Deal.__table__.create(bind=engine, checkfirst=True)
        inspector = inspect(engine)

    # Deal-only expansion
    _ensure_column(engine, inspector, "deals", "geography", "VARCHAR(128)")
    _ensure_column(engine, inspector, "deals", "year_invested", "INTEGER")
    _ensure_column(engine, inspector, "deals", "ownership_pct", "FLOAT")
    _ensure_column(engine, inspector, "deals", "exit_type", "VARCHAR(128)")
    _ensure_column(engine, inspector, "deals", "lead_partner", "VARCHAR(128)")
    _ensure_column(engine, inspector, "deals", "security_type", "VARCHAR(128)")
    _ensure_column(engine, inspector, "deals", "deal_type", "VARCHAR(128)")
    _ensure_column(engine, inspector, "deals", "entry_channel", "VARCHAR(128)")

    # Legacy compatibility fields (if historically present they remain optional)
    _ensure_column(engine, inspector, "deals", "irr", "FLOAT")
    _ensure_column(engine, inspector, "deals", "fund_size", "FLOAT")
    _ensure_column(engine, inspector, "deals", "net_irr", "FLOAT")
    _ensure_column(engine, inspector, "deals", "net_moic", "FLOAT")
    _ensure_column(engine, inspector, "deals", "net_dpi", "FLOAT")
    _ensure_column(engine, inspector, "deals", "firm_id", "INTEGER")
    _ensure_column(engine, inspector, "deals", "team_id", "INTEGER")

    if "upload_issues" not in inspector.get_table_names():
        UploadIssue.__table__.create(bind=engine)

    Firm.__table__.create(bind=engine, checkfirst=True)
    User.__table__.create(bind=engine, checkfirst=True)
    Team.__table__.create(bind=engine, checkfirst=True)
    TeamMembership.__table__.create(bind=engine, checkfirst=True)
    TeamFirmAccess.__table__.create(bind=engine, checkfirst=True)
    TeamInvite.__table__.create(bind=engine, checkfirst=True)
    DealCashflowEvent.__table__.create(bind=engine, checkfirst=True)
    DealQuarterSnapshot.__table__.create(bind=engine, checkfirst=True)
    FundQuarterSnapshot.__table__.create(bind=engine, checkfirst=True)
    DealUnderwriteBaseline.__table__.create(bind=engine, checkfirst=True)
    inspector = inspect(engine)

    _ensure_column(engine, inspector, "firms", "base_currency", "VARCHAR(3)", default_sql="'USD'")
    with engine.begin() as conn:
        conn.execute(text("UPDATE firms SET base_currency = UPPER(TRIM(base_currency)) WHERE base_currency IS NOT NULL"))
        conn.execute(
            text(
                "UPDATE firms SET base_currency = 'USD' "
                "WHERE base_currency IS NULL OR TRIM(base_currency) = '' OR LENGTH(TRIM(base_currency)) <> 3"
            )
        )

    _ensure_column(engine, inspector, "deal_cashflow_events", "deal_id", "INTEGER")
    _ensure_column(engine, inspector, "deal_cashflow_events", "event_date", "DATE")
    _ensure_column(engine, inspector, "deal_cashflow_events", "event_type", "VARCHAR(64)")
    _ensure_column(engine, inspector, "deal_cashflow_events", "amount", "FLOAT")
    _ensure_column(engine, inspector, "deal_cashflow_events", "notes", "VARCHAR(500)")
    _ensure_column(engine, inspector, "deal_cashflow_events", "firm_id", "INTEGER")
    _ensure_column(engine, inspector, "deal_cashflow_events", "team_id", "INTEGER")
    _ensure_column(engine, inspector, "deal_cashflow_events", "upload_batch", "VARCHAR(100)")
    _ensure_column(engine, inspector, "deal_cashflow_events", "created_at", "DATETIME")

    _ensure_column(engine, inspector, "deal_quarter_snapshots", "deal_id", "INTEGER")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "quarter_end", "DATE")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "revenue", "FLOAT")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "ebitda", "FLOAT")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "enterprise_value", "FLOAT")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "net_debt", "FLOAT")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "equity_value", "FLOAT")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "valuation_basis", "VARCHAR(128)")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "source", "VARCHAR(128)")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "firm_id", "INTEGER")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "team_id", "INTEGER")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "upload_batch", "VARCHAR(100)")
    _ensure_column(engine, inspector, "deal_quarter_snapshots", "created_at", "DATETIME")

    _ensure_column(engine, inspector, "fund_quarter_snapshots", "fund_number", "VARCHAR(50)")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "quarter_end", "DATE")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "committed_capital", "FLOAT")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "paid_in_capital", "FLOAT")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "distributed_capital", "FLOAT")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "nav", "FLOAT")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "unfunded_commitment", "FLOAT")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "firm_id", "INTEGER")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "team_id", "INTEGER")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "upload_batch", "VARCHAR(100)")
    _ensure_column(engine, inspector, "fund_quarter_snapshots", "created_at", "DATETIME")

    _ensure_column(engine, inspector, "deal_underwrite_baselines", "deal_id", "INTEGER")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "target_irr", "FLOAT")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "target_moic", "FLOAT")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "target_hold_years", "FLOAT")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "target_exit_multiple", "FLOAT")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "target_revenue_cagr", "FLOAT")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "target_ebitda_cagr", "FLOAT")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "baseline_date", "DATE")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "firm_id", "INTEGER")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "team_id", "INTEGER")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "upload_batch", "VARCHAR(100)")
    _ensure_column(engine, inspector, "deal_underwrite_baselines", "created_at", "DATETIME")

    _ensure_column(engine, inspector, "upload_issues", "firm_id", "INTEGER")
    _ensure_column(engine, inspector, "upload_issues", "team_id", "INTEGER")

    _ensure_index(engine, "ix_deals_firm_id", "deals", "firm_id")
    _ensure_index(engine, "ix_deal_cashflow_events_firm_id", "deal_cashflow_events", "firm_id")
    _ensure_index(engine, "ix_deal_quarter_snapshots_firm_id", "deal_quarter_snapshots", "firm_id")
    _ensure_index(engine, "ix_fund_quarter_snapshots_firm_id", "fund_quarter_snapshots", "firm_id")
    _ensure_index(engine, "ix_deal_underwrite_baselines_firm_id", "deal_underwrite_baselines", "firm_id")
    _ensure_index(engine, "ix_upload_issues_firm_id", "upload_issues", "firm_id")
    _ensure_index(engine, "ix_deals_team_id", "deals", "team_id")
    _ensure_index(engine, "ix_deal_cashflow_events_team_id", "deal_cashflow_events", "team_id")
    _ensure_index(engine, "ix_deal_quarter_snapshots_team_id", "deal_quarter_snapshots", "team_id")
    _ensure_index(engine, "ix_fund_quarter_snapshots_team_id", "fund_quarter_snapshots", "team_id")
    _ensure_index(engine, "ix_deal_underwrite_baselines_team_id", "deal_underwrite_baselines", "team_id")
    _ensure_index(engine, "ix_upload_issues_team_id", "upload_issues", "team_id")
    _ensure_index(engine, "ix_team_firm_access_team_id", "team_firm_access", "team_id")
    _ensure_index(engine, "ix_team_firm_access_firm_id", "team_firm_access", "firm_id")

    # Archive legacy cashflow table once if it exists.
    _archive_legacy_cashflows(engine, inspector)

from flask_login import UserMixin
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import ForeignKey, Index, UniqueConstraint, inspect, text


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
    as_of_date = db.Column(db.Date, nullable=True)

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

    # Acquired / bolt-on metrics (cumulative entry values at time of each acquisition)
    acquired_revenue = db.Column(db.Float, nullable=True)
    acquired_ebitda = db.Column(db.Float, nullable=True)
    acquired_tev = db.Column(db.Float, nullable=True)

    # Performance metrics
    realized_value = db.Column(db.Float, nullable=True)
    unrealized_value = db.Column(db.Float, nullable=True)
    irr = db.Column(db.Float, nullable=True)  # optional legacy uploaded IRR (ignored in analytics)
    fund_size = db.Column(db.Float, nullable=True)  # optional fund size in same units as invested values ($M)
    net_irr = db.Column(db.Float, nullable=True)  # optional fund-level net IRR (decimal form, e.g. 0.258)
    net_moic = db.Column(db.Float, nullable=True)  # optional fund-level net MOIC
    net_dpi = db.Column(db.Float, nullable=True)  # optional fund-level net DPI

    # Currency conversion metadata (set at upload time)
    performance_currency = db.Column(db.String(3), nullable=True)  # ISO code for equity/realized/unrealized
    financial_metric_currency = db.Column(db.String(3), nullable=True)  # ISO code for revenue/ebitda/tev/debt
    perf_fx_rate_to_usd = db.Column(db.Float, nullable=True)  # FX rate used at upload for performance values
    fin_fx_rate_to_usd = db.Column(db.Float, nullable=True)  # FX rate used at upload for financial metric values

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


class FundMetadata(db.Model):
    __tablename__ = "fund_metadata"
    __table_args__ = (
        UniqueConstraint("team_id", "firm_id", "fund_number", name="uq_fund_metadata_team_firm_fund"),
        Index("ix_fund_metadata_firm_fund", "firm_id", "fund_number"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    fund_number = db.Column(db.String(50), nullable=False, index=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=False, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    vintage_year = db.Column(db.Integer, nullable=True, index=True)
    strategy = db.Column(db.String(128), nullable=True, index=True)
    region_focus = db.Column(db.String(128), nullable=True)
    fund_size = db.Column(db.Float, nullable=True)
    first_close_date = db.Column(db.Date, nullable=True)
    final_close_date = db.Column(db.Date, nullable=True)
    manager_name = db.Column(db.String(255), nullable=True, index=True)
    benchmark_peer_group = db.Column(db.String(128), nullable=True)
    status = db.Column(db.String(64), nullable=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<FundMetadata fund={self.fund_number} firm={self.firm_id}>"


class FundCashflow(db.Model):
    __tablename__ = "fund_cashflows"
    __table_args__ = (
        Index("ix_fund_cashflows_firm_fund_event", "firm_id", "fund_number", "event_date"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    fund_number = db.Column(db.String(50), nullable=False, index=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=False, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    event_date = db.Column(db.Date, nullable=False, index=True)
    event_type = db.Column(db.String(64), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    nav_after_event = db.Column(db.Float, nullable=True)
    currency_code = db.Column(db.String(3), nullable=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<FundCashflow fund={self.fund_number} event_date={self.event_date}>"


class PublicMarketIndexLevel(db.Model):
    __tablename__ = "public_market_index_levels"
    __table_args__ = (
        UniqueConstraint("team_id", "benchmark_code", "level_date", name="uq_public_market_index_levels_team_code_date"),
        Index("ix_public_market_index_levels_code_date", "benchmark_code", "level_date"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    benchmark_code = db.Column(db.String(64), nullable=False, index=True)
    level_date = db.Column(db.Date, nullable=False, index=True)
    level = db.Column(db.Float, nullable=False)
    currency_code = db.Column(db.String(3), nullable=True)
    source = db.Column(db.String(128), nullable=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<PublicMarketIndexLevel team={self.team_id} code={self.benchmark_code} level_date={self.level_date}>"


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


class BenchmarkPoint(db.Model):
    __tablename__ = "benchmark_points"
    __table_args__ = (
        UniqueConstraint(
            "team_id",
            "asset_class",
            "strategy",
            "region",
            "size_bucket",
            "vintage_year",
            "metric",
            "quartile",
            name="uq_benchmark_points_team_asset_dims_vintage_metric_quartile",
        ),
        Index(
            "ix_benchmark_points_asset_dims_vintage",
            "asset_class",
            "strategy",
            "region",
            "size_bucket",
            "vintage_year",
        ),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    asset_class = db.Column(db.String(128), nullable=False, index=True)
    strategy = db.Column(db.String(128), nullable=True, index=True)
    region = db.Column(db.String(128), nullable=True, index=True)
    size_bucket = db.Column(db.String(128), nullable=True, index=True)
    vintage_year = db.Column(db.Integer, nullable=False, index=True)
    metric = db.Column(db.String(32), nullable=False)
    quartile = db.Column(db.String(32), nullable=False)
    value = db.Column(db.Float, nullable=False)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return (
            f"<BenchmarkPoint team={self.team_id} asset={self.asset_class} "
            f"vintage={self.vintage_year} metric={self.metric} quartile={self.quartile}>"
        )


class ChartBuilderTemplate(db.Model):
    __tablename__ = "chart_builder_templates"
    __table_args__ = (
        UniqueConstraint("team_id", "name", name="uq_chart_builder_templates_team_name"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    name = db.Column(db.String(255), nullable=False)
    source = db.Column(db.String(64), nullable=False, index=True)
    config_json = db.Column(db.Text, nullable=False)
    created_by_user_id = db.Column(db.Integer, ForeignKey("users.id"), nullable=True, index=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<ChartBuilderTemplate team={self.team_id} name={self.name}>"


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
    fx_rate_to_usd = db.Column(db.Float, nullable=True)
    fx_rate_date = db.Column(db.Date, nullable=True)
    fx_rate_source = db.Column(db.String(128), nullable=True)
    fx_last_status = db.Column(db.String(32), nullable=True)
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


class MemoDocument(db.Model):
    __tablename__ = "memo_documents"
    __table_args__ = (
        Index("ix_memo_documents_team_role_status", "team_id", "document_role", "status"),
        Index("ix_memo_documents_firm_status", "firm_id", "status"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    created_by_user_id = db.Column(db.Integer, ForeignKey("users.id"), nullable=False, index=True)
    document_role = db.Column(db.String(64), nullable=False, index=True)
    file_name = db.Column(db.String(255), nullable=False)
    mime_type = db.Column(db.String(128), nullable=False)
    storage_key = db.Column(db.String(500), nullable=False, unique=True)
    sha256 = db.Column(db.String(64), nullable=False, index=True)
    page_count = db.Column(db.Integer, nullable=True)
    status = db.Column(db.String(32), nullable=False, default="uploaded", index=True)
    extraction_status = db.Column(db.String(32), nullable=False, default="pending", index=True)
    error_text = db.Column(db.Text, nullable=True)
    metadata_json = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<MemoDocument id={self.id} role={self.document_role} status={self.status}>"


class MemoStoredBlob(db.Model):
    __tablename__ = "memo_stored_blobs"
    __table_args__ = (
        Index("ix_memo_stored_blobs_created_at", "created_at"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    storage_key = db.Column(db.String(500), nullable=False, unique=True, index=True)
    content = db.Column(db.LargeBinary, nullable=False)
    size_bytes = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<MemoStoredBlob key={self.storage_key} bytes={self.size_bytes}>"


class MemoDocumentChunk(db.Model):
    __tablename__ = "memo_document_chunks"
    __table_args__ = (
        Index("ix_memo_document_chunks_doc_chunk", "document_id", "chunk_index"),
        Index("ix_memo_document_chunks_team_firm_section", "team_id", "firm_id", "section_key"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    document_id = db.Column(db.Integer, ForeignKey("memo_documents.id"), nullable=False, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    chunk_index = db.Column(db.Integer, nullable=False)
    section_key = db.Column(db.String(128), nullable=True, index=True)
    page_start = db.Column(db.Integer, nullable=True)
    page_end = db.Column(db.Integer, nullable=True)
    text = db.Column(db.Text, nullable=False)
    text_delexicalized = db.Column(db.Text, nullable=True)
    embedding_json = db.Column(db.Text, nullable=True)
    metadata_json = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(32), nullable=False, default="ready", index=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<MemoDocumentChunk document_id={self.document_id} chunk_index={self.chunk_index}>"


class MemoStyleProfile(db.Model):
    __tablename__ = "memo_style_profiles"
    __table_args__ = (
        Index("ix_memo_style_profiles_team_user_status", "team_id", "created_by_user_id", "status"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    created_by_user_id = db.Column(db.Integer, ForeignKey("users.id"), nullable=False, index=True)
    name = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(32), nullable=False, default="pending", index=True)
    profile_json = db.Column(db.Text, nullable=True)
    source_document_count = db.Column(db.Integer, nullable=False, default=0)
    approved_exemplar_count = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<MemoStyleProfile id={self.id} name={self.name} status={self.status}>"


class MemoStyleExemplar(db.Model):
    __tablename__ = "memo_style_exemplars"
    __table_args__ = (
        Index("ix_memo_style_exemplars_profile_section_rank", "style_profile_id", "section_key", "rank"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    style_profile_id = db.Column(db.Integer, ForeignKey("memo_style_profiles.id"), nullable=False, index=True)
    document_id = db.Column(db.Integer, ForeignKey("memo_documents.id"), nullable=False, index=True)
    section_key = db.Column(db.String(128), nullable=False, index=True)
    heading_text = db.Column(db.String(255), nullable=True)
    text_raw = db.Column(db.Text, nullable=False)
    text_delexicalized = db.Column(db.Text, nullable=True)
    embedding_json = db.Column(db.Text, nullable=True)
    rank = db.Column(db.Integer, nullable=False, default=0)
    status = db.Column(db.String(32), nullable=False, default="ready", index=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<MemoStyleExemplar profile_id={self.style_profile_id} section={self.section_key}>"


class MemoGenerationRun(db.Model):
    __tablename__ = "memo_generation_runs"
    __table_args__ = (
        Index("ix_memo_generation_runs_team_firm_status", "team_id", "firm_id", "status"),
        Index("ix_memo_generation_runs_user_status", "created_by_user_id", "status"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=False, index=True)
    created_by_user_id = db.Column(db.Integer, ForeignKey("users.id"), nullable=False, index=True)
    style_profile_id = db.Column(db.Integer, ForeignKey("memo_style_profiles.id"), nullable=False, index=True)
    memo_type = db.Column(db.String(64), nullable=False, default="fund_investment")
    filters_json = db.Column(db.Text, nullable=True)
    benchmark_asset_class = db.Column(db.String(128), nullable=True)
    document_ids_json = db.Column(db.Text, nullable=True)
    user_notes = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(32), nullable=False, default="queued", index=True)
    progress_stage = db.Column(db.String(64), nullable=False, default="created")
    outline_json = db.Column(db.Text, nullable=True)
    evidence_json = db.Column(db.Text, nullable=True)
    final_markdown = db.Column(db.Text, nullable=True)
    final_html = db.Column(db.Text, nullable=True)
    missing_data_json = db.Column(db.Text, nullable=True)
    conflicts_json = db.Column(db.Text, nullable=True)
    open_questions_json = db.Column(db.Text, nullable=True)
    export_status = db.Column(db.String(32), nullable=False, default="not_requested")
    approved_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<MemoGenerationRun id={self.id} status={self.status} stage={self.progress_stage}>"


class MemoGenerationSection(db.Model):
    __tablename__ = "memo_generation_sections"
    __table_args__ = (
        Index("ix_memo_generation_sections_run_order", "run_id", "section_order"),
        UniqueConstraint("run_id", "section_key", name="uq_memo_generation_sections_run_key"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    run_id = db.Column(db.Integer, ForeignKey("memo_generation_runs.id"), nullable=False, index=True)
    section_key = db.Column(db.String(128), nullable=False, index=True)
    section_order = db.Column(db.Integer, nullable=False, default=0)
    title = db.Column(db.String(255), nullable=False)
    objective = db.Column(db.Text, nullable=True)
    required_evidence_json = db.Column(db.Text, nullable=True)
    draft_json = db.Column(db.Text, nullable=True)
    draft_text = db.Column(db.Text, nullable=True)
    validation_json = db.Column(db.Text, nullable=True)
    review_status = db.Column(db.String(32), nullable=False, default="pending")
    status = db.Column(db.String(32), nullable=False, default="pending", index=True)
    approved_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<MemoGenerationSection run_id={self.run_id} key={self.section_key} status={self.status}>"


class MemoGenerationClaim(db.Model):
    __tablename__ = "memo_generation_claims"
    __table_args__ = (
        Index("ix_memo_generation_claims_run_section", "run_id", "section_id"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    run_id = db.Column(db.Integer, ForeignKey("memo_generation_runs.id"), nullable=False, index=True)
    section_id = db.Column(db.Integer, ForeignKey("memo_generation_sections.id"), nullable=False, index=True)
    claim_type = db.Column(db.String(32), nullable=False)
    claim_text = db.Column(db.Text, nullable=False)
    provenance_type = db.Column(db.String(32), nullable=True)
    provenance_id = db.Column(db.String(255), nullable=True)
    citation_json = db.Column(db.Text, nullable=True)
    validation_status = db.Column(db.String(32), nullable=False, default="pending")
    mismatch_reason = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(32), nullable=False, default="pending", index=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<MemoGenerationClaim run_id={self.run_id} section_id={self.section_id} type={self.claim_type}>"


class MemoJob(db.Model):
    __tablename__ = "memo_jobs"
    __table_args__ = (
        Index("ix_memo_jobs_status_type_created", "status", "job_type", "created_at"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=False, index=True)
    run_id = db.Column(db.Integer, ForeignKey("memo_generation_runs.id"), nullable=True, index=True)
    job_type = db.Column(db.String(64), nullable=False, index=True)
    status = db.Column(db.String(32), nullable=False, default="queued", index=True)
    attempt_count = db.Column(db.Integer, nullable=False, default=0)
    lease_expires_at = db.Column(db.DateTime, nullable=True)
    payload_json = db.Column(db.Text, nullable=True)
    error_text = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f"<MemoJob id={self.id} type={self.job_type} status={self.status}>"


# ---------------------------------------------------------------------------
# Private Credit
# ---------------------------------------------------------------------------


class CreditLoan(db.Model):
    __tablename__ = "credit_loans"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # Investment & fund details
    company_name = db.Column(db.String(255), nullable=False)
    fund_name = db.Column(db.String(255), nullable=False)
    vintage_year = db.Column(db.Integer, nullable=True)
    close_date = db.Column(db.Date, nullable=False)
    exit_date = db.Column(db.Date, nullable=True)
    status = db.Column(db.String(50), nullable=False, default="Unrealized")
    as_of_date = db.Column(db.Date, nullable=True)

    # Credit structure & terms
    instrument = db.Column(db.String(100), nullable=True)  # e.g. Term Loan B, Revolver
    tranche = db.Column(db.String(100), nullable=True)  # e.g. First Lien, Second Lien
    security_type = db.Column(db.String(100), nullable=True)  # e.g. Senior Secured, Unitranche
    issue_size = db.Column(db.Float, nullable=True)  # total issue size
    hold_size = db.Column(db.Float, nullable=True)  # PCOF's portion
    coupon_rate = db.Column(db.Float, nullable=True)  # decimal, e.g. 0.05 = 5%
    spread_bps = db.Column(db.Integer, nullable=True)  # basis points over reference rate
    floor_rate = db.Column(db.Float, nullable=True)  # rate floor, decimal
    fee_oid = db.Column(db.Float, nullable=True)  # OID as decimal
    fee_upfront = db.Column(db.Float, nullable=True)
    fee_exit = db.Column(db.Float, nullable=True)
    maturity_date = db.Column(db.Date, nullable=True)
    fixed_or_floating = db.Column(db.String(20), nullable=True)  # Fixed, Floating
    reference_rate = db.Column(db.String(20), nullable=True)  # SOFR, EURIBOR, Prime
    pik_toggle = db.Column(db.Boolean, nullable=False, default=False)
    pik_rate = db.Column(db.Float, nullable=True)  # decimal, null if pik_toggle=False
    call_protection_months = db.Column(db.Integer, nullable=True)
    make_whole_premium = db.Column(db.Float, nullable=True)  # decimal
    amortization_type = db.Column(db.String(50), nullable=True)  # Bullet, Amortizing, IO
    payment_frequency = db.Column(db.String(20), nullable=True)  # Monthly, Quarterly

    # Credit metrics
    entry_ltv = db.Column(db.Float, nullable=True)  # decimal 0-1 (0.65 = 65%)
    current_ltv = db.Column(db.Float, nullable=True)  # decimal 0-1
    entry_revenue = db.Column(db.Float, nullable=True)
    entry_ebitda = db.Column(db.Float, nullable=True)
    current_revenue = db.Column(db.Float, nullable=True)
    current_ebitda = db.Column(db.Float, nullable=True)
    interest_coverage_ratio = db.Column(db.Float, nullable=True)
    dscr = db.Column(db.Float, nullable=True)  # Debt Service Coverage Ratio
    internal_credit_rating = db.Column(db.Integer, nullable=True)  # 1 (best) to 5 (worst)
    default_status = db.Column(db.String(50), nullable=False, default="Performing")  # Performing, Watch List, Default, Restructured
    covenant_type = db.Column(db.String(50), nullable=True)  # Maintenance, Incurrence, None
    covenant_compliant = db.Column(db.Boolean, nullable=True)

    # Performance metrics
    gross_irr = db.Column(db.Float, nullable=True)  # decimal
    moic = db.Column(db.Float, nullable=True)
    realized_value = db.Column(db.Float, nullable=True)
    unrealized_value = db.Column(db.Float, nullable=True)
    cumulative_interest_income = db.Column(db.Float, nullable=True)
    cumulative_fee_income = db.Column(db.Float, nullable=True)
    fair_value = db.Column(db.Float, nullable=True)  # mark-to-market
    yield_to_maturity = db.Column(db.Float, nullable=True)  # decimal
    recovery_rate = db.Column(db.Float, nullable=True)  # decimal 0-1, for impaired loans
    original_par = db.Column(db.Float, nullable=True)  # original face amount
    current_outstanding = db.Column(db.Float, nullable=True)  # current principal balance
    accrued_interest = db.Column(db.Float, nullable=True)

    # Classification
    sector = db.Column(db.String(100), nullable=True)
    geography = db.Column(db.String(100), nullable=True)
    sponsor = db.Column(db.String(255), nullable=True)  # PE sponsor backing borrower

    # Currency
    currency = db.Column(db.String(3), nullable=True, default="USD")
    fx_rate_to_usd = db.Column(db.Float, nullable=True)

    # Metadata
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=True, index=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<CreditLoan {self.company_name} ({self.default_status})>"


class CreditLoanSnapshot(db.Model):
    __tablename__ = "credit_loan_snapshots"
    __table_args__ = (
        UniqueConstraint("credit_loan_id", "snapshot_date", "upload_batch", name="uq_credit_snapshot_loan_date_batch"),
        Index("ix_credit_loan_snapshots_loan_date", "credit_loan_id", "snapshot_date"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    credit_loan_id = db.Column(db.Integer, ForeignKey("credit_loans.id"), nullable=False, index=True)
    snapshot_date = db.Column(db.Date, nullable=False)
    current_ltv = db.Column(db.Float, nullable=True)
    fair_value = db.Column(db.Float, nullable=True)
    current_revenue = db.Column(db.Float, nullable=True)
    current_ebitda = db.Column(db.Float, nullable=True)
    interest_coverage_ratio = db.Column(db.Float, nullable=True)
    dscr = db.Column(db.Float, nullable=True)
    default_status = db.Column(db.String(50), nullable=True)
    internal_credit_rating = db.Column(db.Integer, nullable=True)
    covenant_compliant = db.Column(db.Boolean, nullable=True)
    current_outstanding = db.Column(db.Float, nullable=True)
    accrued_interest = db.Column(db.Float, nullable=True)

    # Metadata
    firm_id = db.Column(db.Integer, ForeignKey("firms.id"), nullable=True, index=True)
    team_id = db.Column(db.Integer, ForeignKey("teams.id"), nullable=True, index=True)
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<CreditLoanSnapshot loan_id={self.credit_loan_id} date={self.snapshot_date}>"


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


def _ensure_index_columns(engine, index_name, table_name, column_names):
    columns = ", ".join(column_names)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns})"))


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
    _ensure_column(engine, inspector, "deals", "as_of_date", "DATE")

    # Legacy compatibility fields (if historically present they remain optional)
    _ensure_column(engine, inspector, "deals", "irr", "FLOAT")
    _ensure_column(engine, inspector, "deals", "fund_size", "FLOAT")
    _ensure_column(engine, inspector, "deals", "net_irr", "FLOAT")
    _ensure_column(engine, inspector, "deals", "net_moic", "FLOAT")
    _ensure_column(engine, inspector, "deals", "net_dpi", "FLOAT")
    _ensure_column(engine, inspector, "deals", "firm_id", "INTEGER")
    _ensure_column(engine, inspector, "deals", "team_id", "INTEGER")

    # Acquired / bolt-on metrics
    _ensure_column(engine, inspector, "deals", "acquired_revenue", "FLOAT")
    _ensure_column(engine, inspector, "deals", "acquired_ebitda", "FLOAT")
    _ensure_column(engine, inspector, "deals", "acquired_tev", "FLOAT")

    # Currency conversion metadata
    _ensure_column(engine, inspector, "deals", "performance_currency", "VARCHAR(3)")
    _ensure_column(engine, inspector, "deals", "financial_metric_currency", "VARCHAR(3)")
    _ensure_column(engine, inspector, "deals", "perf_fx_rate_to_usd", "FLOAT")
    _ensure_column(engine, inspector, "deals", "fin_fx_rate_to_usd", "FLOAT")

    if "upload_issues" not in inspector.get_table_names():
        UploadIssue.__table__.create(bind=engine)

    Firm.__table__.create(bind=engine, checkfirst=True)
    User.__table__.create(bind=engine, checkfirst=True)
    Team.__table__.create(bind=engine, checkfirst=True)
    TeamMembership.__table__.create(bind=engine, checkfirst=True)
    TeamFirmAccess.__table__.create(bind=engine, checkfirst=True)
    TeamInvite.__table__.create(bind=engine, checkfirst=True)
    MemoDocument.__table__.create(bind=engine, checkfirst=True)
    MemoStoredBlob.__table__.create(bind=engine, checkfirst=True)
    MemoDocumentChunk.__table__.create(bind=engine, checkfirst=True)
    MemoStyleProfile.__table__.create(bind=engine, checkfirst=True)
    MemoStyleExemplar.__table__.create(bind=engine, checkfirst=True)
    MemoGenerationRun.__table__.create(bind=engine, checkfirst=True)
    MemoGenerationSection.__table__.create(bind=engine, checkfirst=True)
    MemoGenerationClaim.__table__.create(bind=engine, checkfirst=True)
    MemoJob.__table__.create(bind=engine, checkfirst=True)
    BenchmarkPoint.__table__.create(bind=engine, checkfirst=True)
    ChartBuilderTemplate.__table__.create(bind=engine, checkfirst=True)
    DealCashflowEvent.__table__.create(bind=engine, checkfirst=True)
    DealQuarterSnapshot.__table__.create(bind=engine, checkfirst=True)
    FundQuarterSnapshot.__table__.create(bind=engine, checkfirst=True)
    FundMetadata.__table__.create(bind=engine, checkfirst=True)
    FundCashflow.__table__.create(bind=engine, checkfirst=True)
    PublicMarketIndexLevel.__table__.create(bind=engine, checkfirst=True)
    DealUnderwriteBaseline.__table__.create(bind=engine, checkfirst=True)
    inspector = inspect(engine)

    _ensure_column(engine, inspector, "firms", "base_currency", "VARCHAR(3)", default_sql="'USD'")
    _ensure_column(engine, inspector, "firms", "fx_rate_to_usd", "FLOAT")
    _ensure_column(engine, inspector, "firms", "fx_rate_date", "DATE")
    _ensure_column(engine, inspector, "firms", "fx_rate_source", "VARCHAR(128)")
    _ensure_column(engine, inspector, "firms", "fx_last_status", "VARCHAR(32)")
    with engine.begin() as conn:
        conn.execute(text("UPDATE firms SET base_currency = UPPER(TRIM(base_currency)) WHERE base_currency IS NOT NULL"))
        conn.execute(
            text(
                "UPDATE firms SET base_currency = 'USD' "
                "WHERE base_currency IS NULL OR TRIM(base_currency) = '' OR LENGTH(TRIM(base_currency)) <> 3"
            )
        )
        conn.execute(
            text(
                "UPDATE firms SET fx_rate_to_usd = 1.0 "
                "WHERE base_currency = 'USD' AND (fx_rate_to_usd IS NULL OR fx_rate_to_usd <= 0)"
            )
        )
        conn.execute(
            text(
                "UPDATE firms SET fx_last_status = 'ok' "
                "WHERE base_currency = 'USD' AND (fx_last_status IS NULL OR TRIM(fx_last_status) = '')"
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

    _ensure_column(engine, inspector, "fund_metadata", "fund_number", "VARCHAR(50)")
    _ensure_column(engine, inspector, "fund_metadata", "firm_id", "INTEGER")
    _ensure_column(engine, inspector, "fund_metadata", "team_id", "INTEGER")
    _ensure_column(engine, inspector, "fund_metadata", "vintage_year", "INTEGER")
    _ensure_column(engine, inspector, "fund_metadata", "strategy", "VARCHAR(128)")
    _ensure_column(engine, inspector, "fund_metadata", "region_focus", "VARCHAR(128)")
    _ensure_column(engine, inspector, "fund_metadata", "fund_size", "FLOAT")
    _ensure_column(engine, inspector, "fund_metadata", "first_close_date", "DATE")
    _ensure_column(engine, inspector, "fund_metadata", "final_close_date", "DATE")
    _ensure_column(engine, inspector, "fund_metadata", "manager_name", "VARCHAR(255)")
    _ensure_column(engine, inspector, "fund_metadata", "benchmark_peer_group", "VARCHAR(128)")
    _ensure_column(engine, inspector, "fund_metadata", "status", "VARCHAR(64)")
    _ensure_column(engine, inspector, "fund_metadata", "upload_batch", "VARCHAR(100)")
    _ensure_column(engine, inspector, "fund_metadata", "created_at", "DATETIME")

    _ensure_column(engine, inspector, "fund_cashflows", "fund_number", "VARCHAR(50)")
    _ensure_column(engine, inspector, "fund_cashflows", "firm_id", "INTEGER")
    _ensure_column(engine, inspector, "fund_cashflows", "team_id", "INTEGER")
    _ensure_column(engine, inspector, "fund_cashflows", "event_date", "DATE")
    _ensure_column(engine, inspector, "fund_cashflows", "event_type", "VARCHAR(64)")
    _ensure_column(engine, inspector, "fund_cashflows", "amount", "FLOAT")
    _ensure_column(engine, inspector, "fund_cashflows", "nav_after_event", "FLOAT")
    _ensure_column(engine, inspector, "fund_cashflows", "currency_code", "VARCHAR(3)")
    _ensure_column(engine, inspector, "fund_cashflows", "upload_batch", "VARCHAR(100)")
    _ensure_column(engine, inspector, "fund_cashflows", "created_at", "DATETIME")

    _ensure_column(engine, inspector, "public_market_index_levels", "team_id", "INTEGER")
    _ensure_column(engine, inspector, "public_market_index_levels", "benchmark_code", "VARCHAR(64)")
    _ensure_column(engine, inspector, "public_market_index_levels", "level_date", "DATE")
    _ensure_column(engine, inspector, "public_market_index_levels", "level", "FLOAT")
    _ensure_column(engine, inspector, "public_market_index_levels", "currency_code", "VARCHAR(3)")
    _ensure_column(engine, inspector, "public_market_index_levels", "source", "VARCHAR(128)")
    _ensure_column(engine, inspector, "public_market_index_levels", "upload_batch", "VARCHAR(100)")
    _ensure_column(engine, inspector, "public_market_index_levels", "created_at", "DATETIME")

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
    _ensure_column(engine, inspector, "benchmark_points", "strategy", "VARCHAR(128)")
    _ensure_column(engine, inspector, "benchmark_points", "region", "VARCHAR(128)")
    _ensure_column(engine, inspector, "benchmark_points", "size_bucket", "VARCHAR(128)")

    _ensure_index(engine, "ix_deals_firm_id", "deals", "firm_id")
    _ensure_index(engine, "ix_deal_cashflow_events_firm_id", "deal_cashflow_events", "firm_id")
    _ensure_index(engine, "ix_deal_quarter_snapshots_firm_id", "deal_quarter_snapshots", "firm_id")
    _ensure_index(engine, "ix_fund_quarter_snapshots_firm_id", "fund_quarter_snapshots", "firm_id")
    _ensure_index(engine, "ix_fund_metadata_firm_id", "fund_metadata", "firm_id")
    _ensure_index(engine, "ix_fund_cashflows_firm_id", "fund_cashflows", "firm_id")
    _ensure_index(engine, "ix_deal_underwrite_baselines_firm_id", "deal_underwrite_baselines", "firm_id")
    _ensure_index(engine, "ix_upload_issues_firm_id", "upload_issues", "firm_id")
    _ensure_index(engine, "ix_deals_team_id", "deals", "team_id")
    _ensure_index(engine, "ix_deal_cashflow_events_team_id", "deal_cashflow_events", "team_id")
    _ensure_index(engine, "ix_deal_quarter_snapshots_team_id", "deal_quarter_snapshots", "team_id")
    _ensure_index(engine, "ix_fund_quarter_snapshots_team_id", "fund_quarter_snapshots", "team_id")
    _ensure_index(engine, "ix_fund_metadata_team_id", "fund_metadata", "team_id")
    _ensure_index(engine, "ix_fund_cashflows_team_id", "fund_cashflows", "team_id")
    _ensure_index(engine, "ix_public_market_index_levels_team_id", "public_market_index_levels", "team_id")
    _ensure_index(engine, "ix_deal_underwrite_baselines_team_id", "deal_underwrite_baselines", "team_id")
    _ensure_index(engine, "ix_upload_issues_team_id", "upload_issues", "team_id")
    _ensure_index(engine, "ix_team_firm_access_team_id", "team_firm_access", "team_id")
    _ensure_index(engine, "ix_team_firm_access_firm_id", "team_firm_access", "firm_id")
    _ensure_index(engine, "ix_benchmark_points_team_id", "benchmark_points", "team_id")
    _ensure_index(engine, "ix_benchmark_points_asset_class", "benchmark_points", "asset_class")
    _ensure_index(engine, "ix_benchmark_points_strategy", "benchmark_points", "strategy")
    _ensure_index(engine, "ix_benchmark_points_region", "benchmark_points", "region")
    _ensure_index(engine, "ix_benchmark_points_size_bucket", "benchmark_points", "size_bucket")
    _ensure_index(engine, "ix_benchmark_points_vintage_year", "benchmark_points", "vintage_year")
    _ensure_index(engine, "ix_public_market_index_levels_benchmark_code", "public_market_index_levels", "benchmark_code")
    _ensure_index(engine, "ix_public_market_index_levels_level_date", "public_market_index_levels", "level_date")
    _ensure_index(engine, "ix_memo_documents_team_id", "memo_documents", "team_id")
    _ensure_index(engine, "ix_memo_documents_firm_id", "memo_documents", "firm_id")
    _ensure_index(engine, "ix_memo_documents_created_by_user_id", "memo_documents", "created_by_user_id")
    _ensure_index(engine, "ix_memo_documents_document_role", "memo_documents", "document_role")
    _ensure_index(engine, "ix_memo_documents_status", "memo_documents", "status")
    _ensure_index(engine, "ix_memo_documents_extraction_status", "memo_documents", "extraction_status")
    _ensure_index(engine, "ix_memo_documents_sha256", "memo_documents", "sha256")
    _ensure_index(engine, "ix_memo_stored_blobs_storage_key", "memo_stored_blobs", "storage_key")
    _ensure_index(engine, "ix_memo_document_chunks_document_id", "memo_document_chunks", "document_id")
    _ensure_index(engine, "ix_memo_document_chunks_team_id", "memo_document_chunks", "team_id")
    _ensure_index(engine, "ix_memo_document_chunks_firm_id", "memo_document_chunks", "firm_id")
    _ensure_index(engine, "ix_memo_document_chunks_section_key", "memo_document_chunks", "section_key")
    _ensure_index(engine, "ix_memo_document_chunks_status", "memo_document_chunks", "status")
    _ensure_index(engine, "ix_memo_style_profiles_team_id", "memo_style_profiles", "team_id")
    _ensure_index(engine, "ix_memo_style_profiles_created_by_user_id", "memo_style_profiles", "created_by_user_id")
    _ensure_index(engine, "ix_memo_style_profiles_status", "memo_style_profiles", "status")
    _ensure_index(engine, "ix_memo_style_exemplars_style_profile_id", "memo_style_exemplars", "style_profile_id")
    _ensure_index(engine, "ix_memo_style_exemplars_document_id", "memo_style_exemplars", "document_id")
    _ensure_index(engine, "ix_memo_style_exemplars_section_key", "memo_style_exemplars", "section_key")
    _ensure_index(engine, "ix_memo_style_exemplars_status", "memo_style_exemplars", "status")
    _ensure_index(engine, "ix_memo_generation_runs_team_id", "memo_generation_runs", "team_id")
    _ensure_index(engine, "ix_memo_generation_runs_firm_id", "memo_generation_runs", "firm_id")
    _ensure_index(engine, "ix_memo_generation_runs_created_by_user_id", "memo_generation_runs", "created_by_user_id")
    _ensure_index(engine, "ix_memo_generation_runs_style_profile_id", "memo_generation_runs", "style_profile_id")
    _ensure_index(engine, "ix_memo_generation_runs_status", "memo_generation_runs", "status")
    _ensure_index(engine, "ix_memo_generation_sections_run_id", "memo_generation_sections", "run_id")
    _ensure_index(engine, "ix_memo_generation_sections_section_key", "memo_generation_sections", "section_key")
    _ensure_index(engine, "ix_memo_generation_sections_status", "memo_generation_sections", "status")
    _ensure_index(engine, "ix_memo_generation_claims_run_id", "memo_generation_claims", "run_id")
    _ensure_index(engine, "ix_memo_generation_claims_section_id", "memo_generation_claims", "section_id")
    _ensure_index(engine, "ix_memo_generation_claims_status", "memo_generation_claims", "status")
    _ensure_index(engine, "ix_memo_jobs_team_id", "memo_jobs", "team_id")
    _ensure_index(engine, "ix_memo_jobs_run_id", "memo_jobs", "run_id")
    _ensure_index(engine, "ix_memo_jobs_job_type", "memo_jobs", "job_type")
    _ensure_index(engine, "ix_memo_jobs_status", "memo_jobs", "status")
    _ensure_index_columns(engine, "ix_fund_metadata_firm_fund", "fund_metadata", ["firm_id", "fund_number"])
    _ensure_index_columns(engine, "ix_fund_cashflows_firm_fund_event", "fund_cashflows", ["firm_id", "fund_number", "event_date"])
    _ensure_index_columns(
        engine,
        "ix_memo_documents_team_role_status",
        "memo_documents",
        ["team_id", "document_role", "status"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_documents_firm_status",
        "memo_documents",
        ["firm_id", "status"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_stored_blobs_created_at",
        "memo_stored_blobs",
        ["created_at"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_document_chunks_doc_chunk",
        "memo_document_chunks",
        ["document_id", "chunk_index"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_document_chunks_team_firm_section",
        "memo_document_chunks",
        ["team_id", "firm_id", "section_key"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_style_profiles_team_user_status",
        "memo_style_profiles",
        ["team_id", "created_by_user_id", "status"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_style_exemplars_profile_section_rank",
        "memo_style_exemplars",
        ["style_profile_id", "section_key", "rank"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_generation_runs_team_firm_status",
        "memo_generation_runs",
        ["team_id", "firm_id", "status"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_generation_runs_user_status",
        "memo_generation_runs",
        ["created_by_user_id", "status"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_generation_sections_run_order",
        "memo_generation_sections",
        ["run_id", "section_order"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_generation_claims_run_section",
        "memo_generation_claims",
        ["run_id", "section_id"],
    )
    _ensure_index_columns(
        engine,
        "ix_memo_jobs_status_type_created",
        "memo_jobs",
        ["status", "job_type", "created_at"],
    )
    _ensure_index_columns(
        engine,
        "ix_public_market_index_levels_code_date",
        "public_market_index_levels",
        ["benchmark_code", "level_date"],
    )
    _ensure_index_columns(
        engine,
        "ix_benchmark_points_asset_dims_vintage",
        "benchmark_points",
        ["asset_class", "strategy", "region", "size_bucket", "vintage_year"],
    )
    _ensure_index(engine, "ix_chart_builder_templates_team_id", "chart_builder_templates", "team_id")
    _ensure_index(engine, "ix_chart_builder_templates_source", "chart_builder_templates", "source")

    # Private Credit tables
    CreditLoan.__table__.create(bind=engine, checkfirst=True)
    CreditLoanSnapshot.__table__.create(bind=engine, checkfirst=True)
    _ensure_index(engine, "ix_credit_loans_firm_id", "credit_loans", "firm_id")
    _ensure_index(engine, "ix_credit_loans_team_id", "credit_loans", "team_id")
    _ensure_index(engine, "ix_credit_loans_fund_name", "credit_loans", "fund_name")
    _ensure_index(engine, "ix_credit_loans_default_status", "credit_loans", "default_status")
    _ensure_index(engine, "ix_credit_loan_snapshots_firm_id", "credit_loan_snapshots", "firm_id")
    _ensure_index(engine, "ix_credit_loan_snapshots_team_id", "credit_loan_snapshots", "team_id")
    _ensure_index(engine, "ix_credit_loan_snapshots_credit_loan_id", "credit_loan_snapshots", "credit_loan_id")
    _ensure_index_columns(engine, "ix_credit_loan_snapshots_loan_date", "credit_loan_snapshots", ["credit_loan_id", "snapshot_date"])

    # Archive legacy cashflow table once if it exists.
    _archive_legacy_cashflows(engine, inspector)

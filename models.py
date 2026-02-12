from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Deal(db.Model):
    __tablename__ = "deals"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # Core identification
    company_name = db.Column(db.String(255), nullable=False)
    fund_number = db.Column(db.String(50), nullable=True)
    sector = db.Column(db.String(128), nullable=True)
    status = db.Column(db.String(50), nullable=True, default="Unrealized")

    # Dates
    investment_date = db.Column(db.Date, nullable=True)
    exit_date = db.Column(db.Date, nullable=True)

    # Equity
    equity_invested = db.Column(db.Float, nullable=True)

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
    moic = db.Column(db.Float, nullable=True)
    irr = db.Column(db.Float, nullable=True)  # Stored as decimal: 0.15 = 15%

    # Metadata
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    # Relationship to cashflows
    cashflows = db.relationship("Cashflow", backref="deal", lazy=True)

    def __repr__(self):
        return f"<Deal {self.company_name} ({self.status})>"


class Cashflow(db.Model):
    __tablename__ = "cashflows"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    deal_id = db.Column(db.Integer, db.ForeignKey("deals.id"), nullable=True)

    # Identification
    company_name = db.Column(db.String(255), nullable=False)
    fund_number = db.Column(db.String(50), nullable=True)

    # Cashflow data
    date = db.Column(db.Date, nullable=False)
    capital_called = db.Column(db.Float, nullable=True)
    distributions = db.Column(db.Float, nullable=True)
    fees = db.Column(db.Float, nullable=True)
    nav = db.Column(db.Float, nullable=True)

    # Metadata
    upload_batch = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return f"<Cashflow {self.company_name} {self.date}>"

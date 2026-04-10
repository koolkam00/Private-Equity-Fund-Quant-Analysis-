"""Credit-specific filtering and scope queries.

Mirrors the pattern from peqa/services/filtering.py but queries
CreditLoan instead of Deal.
"""

from __future__ import annotations

from models import CreditFundPerformance, CreditLoan, CreditLoanSnapshot, TeamFirmAccess, db
from services.metrics.credit import compute_credit_loan_metrics


CREDIT_FILTER_KEYS = (
    "fund_name",
    "status",
    "default_status",
    "sector",
    "geography",
    "location",
    "sponsor",
    "security_type",
    "instrument",
    "amortization_type",
    "internal_credit_rating",
    "vintage_year",
    "sourcing_channel",
    "is_public",
)


def build_credit_scope_query(team_id=None, firm_id=None, filters=None):
    """Build a scoped query for CreditLoan, applying team/firm access and filters."""
    query = CreditLoan.query

    if firm_id is not None:
        query = query.filter(CreditLoan.firm_id == firm_id)
    if team_id is not None:
        query = query.filter(CreditLoan.team_id == team_id)

    if filters:
        for key in CREDIT_FILTER_KEYS:
            vals = filters.get(key)
            if not vals:
                continue
            if isinstance(vals, str):
                vals = [vals]

            col = getattr(CreditLoan, key, None)
            if col is not None:
                if key == "vintage_year":
                    int_vals = []
                    for v in vals:
                        try:
                            int_vals.append(int(v))
                        except (TypeError, ValueError):
                            pass
                    if int_vals:
                        query = query.filter(col.in_(int_vals))
                elif key == "internal_credit_rating":
                    int_vals = []
                    for v in vals:
                        try:
                            int_vals.append(int(v))
                        except (TypeError, ValueError):
                            pass
                    if int_vals:
                        query = query.filter(col.in_(int_vals))
                else:
                    query = query.filter(col.in_(vals))

    return query.order_by(CreditLoan.fund_name, CreditLoan.company_name)


def build_credit_filter_options(loans):
    """Enumerate available filter values from a list of CreditLoan objects."""
    options = {}
    for key in CREDIT_FILTER_KEYS:
        values = set()
        for loan in loans:
            val = getattr(loan, key, None)
            if val is not None:
                values.add(str(val))
        options[key] = sorted(values)
    return options


def build_credit_analysis_context(team_id=None, firm_id=None, filters=None):
    """Build the full credit analysis context: loans, metrics, filter options.

    Mirrors build_analysis_context() from the equity pipeline but for credit.
    """
    loans = build_credit_scope_query(team_id, firm_id, filters).all()

    # Compute per-loan metrics
    metrics_by_id = {}
    for loan in loans:
        metrics_by_id[loan.id] = compute_credit_loan_metrics(loan)

    # Batch-load snapshots to avoid N+1
    snapshots_by_loan = {}
    if loans:
        loan_ids = [l.id for l in loans]
        all_snapshots = CreditLoanSnapshot.query.filter(
            CreditLoanSnapshot.credit_loan_id.in_(loan_ids)
        ).order_by(CreditLoanSnapshot.snapshot_date.asc()).all()

        for snap in all_snapshots:
            snapshots_by_loan.setdefault(snap.credit_loan_id, []).append(snap)

    # Load fund-level net performance rows for the funds present in `loans`.
    # Scoped by firm + team so a firm only sees its own rows, and keyed by
    # fund_name so compute_credit_track_record can do a direct lookup.
    fund_performance = {}
    fund_names = {loan.fund_name for loan in loans if loan.fund_name}
    if fund_names:
        fp_query = CreditFundPerformance.query.filter(
            CreditFundPerformance.fund_name.in_(fund_names)
        )
        if firm_id is not None:
            fp_query = fp_query.filter(CreditFundPerformance.firm_id == firm_id)
        if team_id is not None:
            fp_query = fp_query.filter(
                (CreditFundPerformance.team_id == team_id)
                | (CreditFundPerformance.team_id.is_(None))
            )
        # If there are multiple rows per fund (e.g. quarterly history), pick
        # the most recent report_date so the track record shows current NAV.
        for row in fp_query.order_by(CreditFundPerformance.report_date.asc()).all():
            fund_performance[row.fund_name] = row

    filter_options = build_credit_filter_options(loans)

    return {
        "loans": loans,
        "metrics_by_id": metrics_by_id,
        "snapshots_by_loan": snapshots_by_loan,
        "fund_performance": fund_performance,
        "filter_options": filter_options,
        "loan_count": len(loans),
    }

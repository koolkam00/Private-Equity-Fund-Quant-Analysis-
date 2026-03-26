"""add deal-level currency conversion fields

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-26 15:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("deals", schema=None) as batch_op:
        batch_op.add_column(sa.Column("performance_currency", sa.String(3), nullable=True))
        batch_op.add_column(sa.Column("financial_metric_currency", sa.String(3), nullable=True))
        batch_op.add_column(sa.Column("perf_fx_rate_to_usd", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("fin_fx_rate_to_usd", sa.Float(), nullable=True))


def downgrade():
    with op.batch_alter_table("deals", schema=None) as batch_op:
        batch_op.drop_column("fin_fx_rate_to_usd")
        batch_op.drop_column("perf_fx_rate_to_usd")
        batch_op.drop_column("financial_metric_currency")
        batch_op.drop_column("performance_currency")

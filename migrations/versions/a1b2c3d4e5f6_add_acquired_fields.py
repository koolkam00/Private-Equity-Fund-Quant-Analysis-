"""add acquired fields

Revision ID: a1b2c3d4e5f6
Revises: 0f09f8f4f7e9
Create Date: 2026-03-26 10:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "a1b2c3d4e5f6"
down_revision = "0f09f8f4f7e9"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("deals", schema=None) as batch_op:
        batch_op.add_column(sa.Column("acquired_revenue", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("acquired_ebitda", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("acquired_tev", sa.Float(), nullable=True))


def downgrade():
    with op.batch_alter_table("deals", schema=None) as batch_op:
        batch_op.drop_column("acquired_tev")
        batch_op.drop_column("acquired_ebitda")
        batch_op.drop_column("acquired_revenue")

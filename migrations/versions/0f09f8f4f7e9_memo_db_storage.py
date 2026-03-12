"""memo db storage

Revision ID: 0f09f8f4f7e9
Revises: 6f4c4d0f5e4a
Create Date: 2026-03-12 09:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "0f09f8f4f7e9"
down_revision = "6f4c4d0f5e4a"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "memo_stored_blobs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("storage_key", sa.String(length=500), nullable=False),
        sa.Column("content", sa.LargeBinary(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("storage_key"),
    )
    with op.batch_alter_table("memo_stored_blobs") as batch_op:
        batch_op.create_index(batch_op.f("ix_memo_stored_blobs_storage_key"), ["storage_key"], unique=False)
        batch_op.create_index("ix_memo_stored_blobs_created_at", ["created_at"], unique=False)


def downgrade():
    with op.batch_alter_table("memo_stored_blobs") as batch_op:
        batch_op.drop_index("ix_memo_stored_blobs_created_at")
        batch_op.drop_index(batch_op.f("ix_memo_stored_blobs_storage_key"))
    op.drop_table("memo_stored_blobs")

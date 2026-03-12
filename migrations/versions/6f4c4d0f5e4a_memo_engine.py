"""memo engine

Revision ID: 6f4c4d0f5e4a
Revises: 4a62775748c8
Create Date: 2026-03-11 21:15:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = "6f4c4d0f5e4a"
down_revision = "4a62775748c8"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "memo_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=False),
        sa.Column("firm_id", sa.Integer(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=False),
        sa.Column("document_role", sa.String(length=64), nullable=False),
        sa.Column("file_name", sa.String(length=255), nullable=False),
        sa.Column("mime_type", sa.String(length=128), nullable=False),
        sa.Column("storage_key", sa.String(length=500), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("page_count", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("extraction_status", sa.String(length=32), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["firm_id"], ["firms.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("storage_key"),
    )
    with op.batch_alter_table("memo_documents") as batch_op:
        batch_op.create_index(batch_op.f("ix_memo_documents_team_id"), ["team_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_documents_firm_id"), ["firm_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_documents_created_by_user_id"), ["created_by_user_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_documents_document_role"), ["document_role"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_documents_sha256"), ["sha256"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_documents_status"), ["status"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_documents_extraction_status"), ["extraction_status"], unique=False)
        batch_op.create_index("ix_memo_documents_team_role_status", ["team_id", "document_role", "status"], unique=False)
        batch_op.create_index("ix_memo_documents_firm_status", ["firm_id", "status"], unique=False)

    op.create_table(
        "memo_document_chunks",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("document_id", sa.Integer(), nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=False),
        sa.Column("firm_id", sa.Integer(), nullable=True),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("section_key", sa.String(length=128), nullable=True),
        sa.Column("page_start", sa.Integer(), nullable=True),
        sa.Column("page_end", sa.Integer(), nullable=True),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("text_delexicalized", sa.Text(), nullable=True),
        sa.Column("embedding_json", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.ForeignKeyConstraint(["document_id"], ["memo_documents.id"]),
        sa.ForeignKeyConstraint(["firm_id"], ["firms.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("memo_document_chunks") as batch_op:
        batch_op.create_index(batch_op.f("ix_memo_document_chunks_document_id"), ["document_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_document_chunks_team_id"), ["team_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_document_chunks_firm_id"), ["firm_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_document_chunks_section_key"), ["section_key"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_document_chunks_status"), ["status"], unique=False)
        batch_op.create_index("ix_memo_document_chunks_doc_chunk", ["document_id", "chunk_index"], unique=False)
        batch_op.create_index("ix_memo_document_chunks_team_firm_section", ["team_id", "firm_id", "section_key"], unique=False)

    op.create_table(
        "memo_style_profiles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("profile_json", sa.Text(), nullable=True),
        sa.Column("source_document_count", sa.Integer(), nullable=False),
        sa.Column("approved_exemplar_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("memo_style_profiles") as batch_op:
        batch_op.create_index(batch_op.f("ix_memo_style_profiles_team_id"), ["team_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_style_profiles_created_by_user_id"), ["created_by_user_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_style_profiles_status"), ["status"], unique=False)
        batch_op.create_index("ix_memo_style_profiles_team_user_status", ["team_id", "created_by_user_id", "status"], unique=False)

    op.create_table(
        "memo_style_exemplars",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("style_profile_id", sa.Integer(), nullable=False),
        sa.Column("document_id", sa.Integer(), nullable=False),
        sa.Column("section_key", sa.String(length=128), nullable=False),
        sa.Column("heading_text", sa.String(length=255), nullable=True),
        sa.Column("text_raw", sa.Text(), nullable=False),
        sa.Column("text_delexicalized", sa.Text(), nullable=True),
        sa.Column("embedding_json", sa.Text(), nullable=True),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.ForeignKeyConstraint(["document_id"], ["memo_documents.id"]),
        sa.ForeignKeyConstraint(["style_profile_id"], ["memo_style_profiles.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("memo_style_exemplars") as batch_op:
        batch_op.create_index(batch_op.f("ix_memo_style_exemplars_style_profile_id"), ["style_profile_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_style_exemplars_document_id"), ["document_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_style_exemplars_section_key"), ["section_key"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_style_exemplars_status"), ["status"], unique=False)
        batch_op.create_index("ix_memo_style_exemplars_profile_section_rank", ["style_profile_id", "section_key", "rank"], unique=False)

    op.create_table(
        "memo_generation_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=False),
        sa.Column("firm_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=False),
        sa.Column("style_profile_id", sa.Integer(), nullable=False),
        sa.Column("memo_type", sa.String(length=64), nullable=False),
        sa.Column("filters_json", sa.Text(), nullable=True),
        sa.Column("benchmark_asset_class", sa.String(length=128), nullable=True),
        sa.Column("document_ids_json", sa.Text(), nullable=True),
        sa.Column("user_notes", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("progress_stage", sa.String(length=64), nullable=False),
        sa.Column("outline_json", sa.Text(), nullable=True),
        sa.Column("evidence_json", sa.Text(), nullable=True),
        sa.Column("final_markdown", sa.Text(), nullable=True),
        sa.Column("final_html", sa.Text(), nullable=True),
        sa.Column("missing_data_json", sa.Text(), nullable=True),
        sa.Column("conflicts_json", sa.Text(), nullable=True),
        sa.Column("open_questions_json", sa.Text(), nullable=True),
        sa.Column("export_status", sa.String(length=32), nullable=False),
        sa.Column("approved_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["firm_id"], ["firms.id"]),
        sa.ForeignKeyConstraint(["style_profile_id"], ["memo_style_profiles.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("memo_generation_runs") as batch_op:
        batch_op.create_index(batch_op.f("ix_memo_generation_runs_team_id"), ["team_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_generation_runs_firm_id"), ["firm_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_generation_runs_created_by_user_id"), ["created_by_user_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_generation_runs_style_profile_id"), ["style_profile_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_generation_runs_status"), ["status"], unique=False)
        batch_op.create_index("ix_memo_generation_runs_team_firm_status", ["team_id", "firm_id", "status"], unique=False)
        batch_op.create_index("ix_memo_generation_runs_user_status", ["created_by_user_id", "status"], unique=False)

    op.create_table(
        "memo_generation_sections",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("section_key", sa.String(length=128), nullable=False),
        sa.Column("section_order", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("objective", sa.Text(), nullable=True),
        sa.Column("required_evidence_json", sa.Text(), nullable=True),
        sa.Column("draft_json", sa.Text(), nullable=True),
        sa.Column("draft_text", sa.Text(), nullable=True),
        sa.Column("validation_json", sa.Text(), nullable=True),
        sa.Column("review_status", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("approved_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["memo_generation_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "section_key", name="uq_memo_generation_sections_run_key"),
    )
    with op.batch_alter_table("memo_generation_sections") as batch_op:
        batch_op.create_index(batch_op.f("ix_memo_generation_sections_run_id"), ["run_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_generation_sections_section_key"), ["section_key"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_generation_sections_status"), ["status"], unique=False)
        batch_op.create_index("ix_memo_generation_sections_run_order", ["run_id", "section_order"], unique=False)

    op.create_table(
        "memo_generation_claims",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("section_id", sa.Integer(), nullable=False),
        sa.Column("claim_type", sa.String(length=32), nullable=False),
        sa.Column("claim_text", sa.Text(), nullable=False),
        sa.Column("provenance_type", sa.String(length=32), nullable=True),
        sa.Column("provenance_id", sa.String(length=255), nullable=True),
        sa.Column("citation_json", sa.Text(), nullable=True),
        sa.Column("validation_status", sa.String(length=32), nullable=False),
        sa.Column("mismatch_reason", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["memo_generation_runs.id"]),
        sa.ForeignKeyConstraint(["section_id"], ["memo_generation_sections.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("memo_generation_claims") as batch_op:
        batch_op.create_index(batch_op.f("ix_memo_generation_claims_run_id"), ["run_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_generation_claims_section_id"), ["section_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_generation_claims_status"), ["status"], unique=False)
        batch_op.create_index("ix_memo_generation_claims_run_section", ["run_id", "section_id"], unique=False)

    op.create_table(
        "memo_jobs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("payload_json", sa.Text(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["memo_generation_runs.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("memo_jobs") as batch_op:
        batch_op.create_index(batch_op.f("ix_memo_jobs_team_id"), ["team_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_jobs_run_id"), ["run_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_jobs_job_type"), ["job_type"], unique=False)
        batch_op.create_index(batch_op.f("ix_memo_jobs_status"), ["status"], unique=False)
        batch_op.create_index("ix_memo_jobs_status_type_created", ["status", "job_type", "created_at"], unique=False)


def downgrade():
    with op.batch_alter_table("memo_jobs") as batch_op:
        batch_op.drop_index("ix_memo_jobs_status_type_created")
        batch_op.drop_index(batch_op.f("ix_memo_jobs_status"))
        batch_op.drop_index(batch_op.f("ix_memo_jobs_job_type"))
        batch_op.drop_index(batch_op.f("ix_memo_jobs_run_id"))
        batch_op.drop_index(batch_op.f("ix_memo_jobs_team_id"))
    op.drop_table("memo_jobs")

    with op.batch_alter_table("memo_generation_claims") as batch_op:
        batch_op.drop_index("ix_memo_generation_claims_run_section")
        batch_op.drop_index(batch_op.f("ix_memo_generation_claims_status"))
        batch_op.drop_index(batch_op.f("ix_memo_generation_claims_section_id"))
        batch_op.drop_index(batch_op.f("ix_memo_generation_claims_run_id"))
    op.drop_table("memo_generation_claims")

    with op.batch_alter_table("memo_generation_sections") as batch_op:
        batch_op.drop_index("ix_memo_generation_sections_run_order")
        batch_op.drop_index(batch_op.f("ix_memo_generation_sections_status"))
        batch_op.drop_index(batch_op.f("ix_memo_generation_sections_section_key"))
        batch_op.drop_index(batch_op.f("ix_memo_generation_sections_run_id"))
    op.drop_table("memo_generation_sections")

    with op.batch_alter_table("memo_generation_runs") as batch_op:
        batch_op.drop_index("ix_memo_generation_runs_user_status")
        batch_op.drop_index("ix_memo_generation_runs_team_firm_status")
        batch_op.drop_index(batch_op.f("ix_memo_generation_runs_status"))
        batch_op.drop_index(batch_op.f("ix_memo_generation_runs_style_profile_id"))
        batch_op.drop_index(batch_op.f("ix_memo_generation_runs_created_by_user_id"))
        batch_op.drop_index(batch_op.f("ix_memo_generation_runs_firm_id"))
        batch_op.drop_index(batch_op.f("ix_memo_generation_runs_team_id"))
    op.drop_table("memo_generation_runs")

    with op.batch_alter_table("memo_style_exemplars") as batch_op:
        batch_op.drop_index("ix_memo_style_exemplars_profile_section_rank")
        batch_op.drop_index(batch_op.f("ix_memo_style_exemplars_status"))
        batch_op.drop_index(batch_op.f("ix_memo_style_exemplars_section_key"))
        batch_op.drop_index(batch_op.f("ix_memo_style_exemplars_document_id"))
        batch_op.drop_index(batch_op.f("ix_memo_style_exemplars_style_profile_id"))
    op.drop_table("memo_style_exemplars")

    with op.batch_alter_table("memo_style_profiles") as batch_op:
        batch_op.drop_index("ix_memo_style_profiles_team_user_status")
        batch_op.drop_index(batch_op.f("ix_memo_style_profiles_status"))
        batch_op.drop_index(batch_op.f("ix_memo_style_profiles_created_by_user_id"))
        batch_op.drop_index(batch_op.f("ix_memo_style_profiles_team_id"))
    op.drop_table("memo_style_profiles")

    with op.batch_alter_table("memo_document_chunks") as batch_op:
        batch_op.drop_index("ix_memo_document_chunks_team_firm_section")
        batch_op.drop_index("ix_memo_document_chunks_doc_chunk")
        batch_op.drop_index(batch_op.f("ix_memo_document_chunks_status"))
        batch_op.drop_index(batch_op.f("ix_memo_document_chunks_section_key"))
        batch_op.drop_index(batch_op.f("ix_memo_document_chunks_firm_id"))
        batch_op.drop_index(batch_op.f("ix_memo_document_chunks_team_id"))
        batch_op.drop_index(batch_op.f("ix_memo_document_chunks_document_id"))
    op.drop_table("memo_document_chunks")

    with op.batch_alter_table("memo_documents") as batch_op:
        batch_op.drop_index("ix_memo_documents_firm_status")
        batch_op.drop_index("ix_memo_documents_team_role_status")
        batch_op.drop_index(batch_op.f("ix_memo_documents_extraction_status"))
        batch_op.drop_index(batch_op.f("ix_memo_documents_status"))
        batch_op.drop_index(batch_op.f("ix_memo_documents_sha256"))
        batch_op.drop_index(batch_op.f("ix_memo_documents_document_role"))
        batch_op.drop_index(batch_op.f("ix_memo_documents_created_by_user_id"))
        batch_op.drop_index(batch_op.f("ix_memo_documents_firm_id"))
        batch_op.drop_index(batch_op.f("ix_memo_documents_team_id"))
    op.drop_table("memo_documents")

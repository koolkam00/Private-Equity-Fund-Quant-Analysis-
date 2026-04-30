import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "instance", "deals.db")


def _env_flag(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class Config:
    FLASK_ENV = os.environ.get("FLASK_ENV", "development")
    IS_PRODUCTION = FLASK_ENV.lower() == "production"
    _memo_max_document_mb = int(os.environ.get("MEMO_MAX_DOCUMENT_MB", "25"))
    _max_content_length_mb = int(
        os.environ.get("MAX_CONTENT_LENGTH_MB", str(max(16, _memo_max_document_mb)))
    )
    _memo_inline_jobs = _env_flag("MEMO_INLINE_JOBS", default=not IS_PRODUCTION)

    _secret = os.environ.get("SECRET_KEY")
    if IS_PRODUCTION and (not _secret or _secret == "dev-secret-key-change-in-prod"):
        raise RuntimeError("SECRET_KEY must be set to a strong value in production.")
    SECRET_KEY = _secret or "dev-secret-key-change-in-prod"

    _db_url = os.environ.get("SQLALCHEMY_DATABASE_URI") or os.environ.get("DATABASE_URL")
    if _db_url and _db_url.startswith("postgres://"):
        _db_url = _db_url.replace("postgres://", "postgresql+psycopg://", 1)
    elif _db_url and _db_url.startswith("postgresql://"):
        _db_url = _db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    SQLALCHEMY_DATABASE_URI = _db_url or f"sqlite:///{DEFAULT_DB_PATH}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True}

    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE = IS_PRODUCTION
    WTF_CSRF_TIME_LIMIT = None
    RATELIMIT_HEADERS_ENABLED = True
    AUTO_SCHEMA_UPDATE = _env_flag("AUTO_SCHEMA_UPDATE", default=not IS_PRODUCTION)

    UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
    MAX_CONTENT_LENGTH = _max_content_length_mb * 1024 * 1024
    ALLOWED_EXTENSIONS = {".xlsx", ".xls"}

    MEMO_STORAGE_BACKEND = os.environ.get("MEMO_STORAGE_BACKEND", "local").strip().lower() or "local"
    MEMO_STORAGE_LOCAL_ROOT = os.environ.get(
        "MEMO_STORAGE_LOCAL_ROOT",
        os.path.join(BASE_DIR, "instance", "memo_storage"),
    )
    MEMO_ALLOWED_EXTENSIONS = {
        ".txt",
        ".md",
        ".pdf",
        ".docx",
        ".pptx",
    }
    MEMO_MAX_DOCUMENT_MB = _memo_max_document_mb
    MEMO_INLINE_JOBS = _memo_inline_jobs
    MEMO_WEB_ASYNC_JOBS = _env_flag("MEMO_WEB_ASYNC_JOBS", default=IS_PRODUCTION)
    MEMO_ENABLE_OCR = _env_flag("MEMO_ENABLE_OCR", default=False)
    MEMO_OCR_MODEL = os.environ.get("MEMO_OCR_MODEL", "gpt-4.1-mini")
    MEMO_OCR_MAX_PAGES = int(os.environ.get("MEMO_OCR_MAX_PAGES", "25"))
    MEMO_OCR_MIN_PAGE_TEXT_CHARS = int(os.environ.get("MEMO_OCR_MIN_PAGE_TEXT_CHARS", "80"))
    MEMO_OCR_RENDER_DPI = int(os.environ.get("MEMO_OCR_RENDER_DPI", "180"))
    MEMO_S3_BUCKET = os.environ.get("MEMO_S3_BUCKET")
    MEMO_S3_REGION = os.environ.get("MEMO_S3_REGION")
    MEMO_S3_ENDPOINT_URL = os.environ.get("MEMO_S3_ENDPOINT_URL")
    MEMO_S3_ACCESS_KEY_ID = os.environ.get("MEMO_S3_ACCESS_KEY_ID")
    MEMO_S3_SECRET_ACCESS_KEY = os.environ.get("MEMO_S3_SECRET_ACCESS_KEY")
    MEMO_LLM_PROVIDER = (os.environ.get("MEMO_LLM_PROVIDER") or "disabled").strip().lower()
    MEMO_LLM_MODEL_STYLE = os.environ.get("MEMO_LLM_MODEL_STYLE", "gpt-4.1-mini")
    MEMO_LLM_MODEL_OUTLINE = os.environ.get("MEMO_LLM_MODEL_OUTLINE", "gpt-4.1-mini")
    MEMO_LLM_MODEL_DRAFT = os.environ.get("MEMO_LLM_MODEL_DRAFT", "gpt-4.1")
    MEMO_LLM_MODEL_VALIDATE = os.environ.get("MEMO_LLM_MODEL_VALIDATE", "gpt-4.1-mini")
    MEMO_LLM_MODEL_INSIGHTS = os.environ.get("MEMO_LLM_MODEL_INSIGHTS", "gpt-4.1")
    MEMO_LLM_TIMEOUT_SECONDS = float(os.environ.get("MEMO_LLM_TIMEOUT_SECONDS", "90"))

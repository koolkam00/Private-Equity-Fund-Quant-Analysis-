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

    UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16 MB max upload
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
    MEMO_MAX_DOCUMENT_MB = int(os.environ.get("MEMO_MAX_DOCUMENT_MB", "25"))
    MEMO_INLINE_JOBS = _env_flag("MEMO_INLINE_JOBS", default=not IS_PRODUCTION)
    MEMO_ENABLE_OCR = _env_flag("MEMO_ENABLE_OCR", default=False)
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

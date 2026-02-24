import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "instance", "deals.db")


class Config:
    FLASK_ENV = os.environ.get("FLASK_ENV", "development")
    IS_PRODUCTION = FLASK_ENV.lower() == "production"

    _secret = os.environ.get("SECRET_KEY")
    if IS_PRODUCTION and (not _secret or _secret == "dev-secret-key-change-in-prod"):
        raise RuntimeError("SECRET_KEY must be set to a strong value in production.")
    SECRET_KEY = _secret or "dev-secret-key-change-in-prod"

    _db_url = os.environ.get("SQLALCHEMY_DATABASE_URI") or os.environ.get("DATABASE_URL")
    if _db_url and _db_url.startswith("postgres://"):
        _db_url = _db_url.replace("postgres://", "postgresql://", 1)
    SQLALCHEMY_DATABASE_URI = _db_url or f"sqlite:///{DEFAULT_DB_PATH}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True}

    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE = IS_PRODUCTION

    UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16 MB max upload
    ALLOWED_EXTENSIONS = {".xlsx", ".xls"}

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any

from flask import Flask, g, request, session
from flask_login import current_user

from config import Config
from legacy_app import app as legacy_binder
from legacy_app import ROUTE_BLUEPRINTS
from peqa.blueprints import get_blueprints
from peqa.extensions import csrf, db, limiter, login_manager, migrate


logger = logging.getLogger(__name__)
SLOW_REQUEST_MS = 750.0


def _json_log(message: str, **payload: Any):
    logger.info("%s %s", message, json.dumps(payload, default=str, sort_keys=True))


def _configure_login_manager():
    login_manager.login_view = "login"
    login_manager.login_message = "Please sign in to continue."


def _register_request_logging(app: Flask):
    @app.before_request
    def _start_timer():
        g.request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        g.request_started_at = time.perf_counter()

    @app.after_request
    def _log_response(response):
        started_at = getattr(g, "request_started_at", None)
        latency_ms = None
        if started_at is not None:
            latency_ms = round((time.perf_counter() - started_at) * 1000.0, 2)

        user_id = current_user.get_id() if current_user.is_authenticated else None
        team_id = session.get("active_team_id")
        firm_id = session.get("active_firm_id")
        payload = {
            "request_id": getattr(g, "request_id", None),
            "method": request.method,
            "path": request.path,
            "status_code": response.status_code,
            "latency_ms": latency_ms,
            "user_id": user_id,
            "team_id": team_id,
            "firm_id": firm_id,
        }
        if latency_ms is not None and latency_ms >= SLOW_REQUEST_MS:
            _json_log("slow_request", **payload)
        else:
            _json_log("request", **payload)
        response.headers["X-Request-ID"] = payload["request_id"] or ""
        return response


def create_app(config_override: dict[str, Any] | None = None) -> Flask:
    project_root = Path(__file__).resolve().parent.parent
    app = Flask(
        __name__,
        root_path=str(project_root),
        template_folder=str(project_root / "templates"),
        static_folder=str(project_root / "static"),
    )
    app.config.from_object(Config)
    if config_override:
        app.config.update(config_override)

    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
    os.makedirs(os.path.join(app.root_path, "instance"), exist_ok=True)
    if app.config.get("MEMO_STORAGE_BACKEND") == "local":
        os.makedirs(app.config["MEMO_STORAGE_LOCAL_ROOT"], exist_ok=True)

    _configure_login_manager()
    db.init_app(app)
    login_manager.init_app(app)
    csrf.init_app(app)
    migrate.init_app(app, db)
    limiter.init_app(app)
    _register_request_logging(app)

    legacy_binder.register(app, get_blueprints(), ROUTE_BLUEPRINTS)
    return app

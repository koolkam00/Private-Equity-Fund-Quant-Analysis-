from flask import Blueprint


def get_blueprints():
    return {
        "auth": Blueprint("auth", __name__),
        "team": Blueprint("team", __name__),
        "scope": Blueprint("scope", __name__),
        "dashboard": Blueprint("dashboard", __name__),
        "analysis": Blueprint("analysis", __name__),
        "uploads": Blueprint("uploads", __name__),
        "reports": Blueprint("reports", __name__),
        "chart_builder_api": Blueprint("chart_builder_api", __name__),
        "memos": Blueprint("memos", __name__),
        "credit": Blueprint("credit", __name__),
    }

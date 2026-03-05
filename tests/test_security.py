from werkzeug.security import generate_password_hash

from models import Firm, Team, TeamFirmAccess, TeamMembership, User, db
from peqa.app_factory import create_app


def _build_app(tmp_path, *, csrf_enabled):
    db_path = tmp_path / "security.db"
    uploads_path = tmp_path / "uploads"
    return create_app(
        {
            "TESTING": True,
            "WTF_CSRF_ENABLED": csrf_enabled,
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{db_path}",
            "UPLOAD_FOLDER": str(uploads_path),
        }
    )


def _seed_authenticated_scope(app, *, user_id):
    with app.app_context():
        db.create_all()
        team = Team(name=f"Team {user_id}", slug=f"team-{user_id}")
        firm = Firm(name=f"Firm {user_id}", slug=f"firm-{user_id}")
        user = User(
            id=user_id,
            email=f"user-{user_id}@example.com",
            password_hash=generate_password_hash("password123"),
            is_active=True,
        )
        db.session.add_all([team, firm, user])
        db.session.flush()
        db.session.add(TeamMembership(team_id=team.id, user_id=user.id, role="owner"))
        db.session.add(TeamFirmAccess(team_id=team.id, firm_id=firm.id, created_by_user_id=user.id))
        db.session.commit()
        return {"user_id": user.id, "team_id": team.id, "firm_id": firm.id}


def _login_session(client, auth_scope):
    with client.session_transaction() as sess:
        sess["_user_id"] = str(auth_scope["user_id"])
        sess["_fresh"] = True
        sess["active_team_id"] = auth_scope["team_id"]
        sess["active_firm_id"] = auth_scope["firm_id"]


def test_login_post_requires_csrf_when_enabled(tmp_path):
    app = _build_app(tmp_path, csrf_enabled=True)

    with app.test_client() as client:
        response = client.post(
            "/auth/login",
            data={"email": "tester@example.com", "password": "wrong-password"},
            environ_base={"REMOTE_ADDR": "10.10.0.1"},
        )

    assert response.status_code == 400


def test_chart_builder_query_requires_csrf_when_enabled(tmp_path):
    app = _build_app(tmp_path, csrf_enabled=True)

    with app.test_client() as client:
        response = client.post(
            "/api/chart-builder/query",
            json={},
            environ_base={"REMOTE_ADDR": "10.10.0.2"},
        )

    assert response.status_code == 400


def test_login_rate_limit_applies_after_five_attempts(tmp_path):
    app = _build_app(tmp_path, csrf_enabled=False)
    _seed_authenticated_scope(app, user_id=1001)

    with app.test_client() as client:
        for _ in range(5):
            response = client.post(
                "/auth/login",
                data={"email": "user-1001@example.com", "password": "wrong-password"},
                environ_base={"REMOTE_ADDR": "10.10.1.1"},
            )
            assert response.status_code == 200

        limited = client.post(
            "/auth/login",
            data={"email": "user-1001@example.com", "password": "wrong-password"},
            environ_base={"REMOTE_ADDR": "10.10.1.1"},
        )

    assert limited.status_code == 429


def test_team_invite_rate_limit_applies_after_ten_posts(tmp_path):
    app = _build_app(tmp_path, csrf_enabled=False)
    auth_scope = _seed_authenticated_scope(app, user_id=2001)

    with app.test_client() as client:
        _login_session(client, auth_scope)
        for idx in range(10):
            response = client.post(
                "/team/invites",
                data={"email": f"invite-{idx}@example.com"},
                environ_base={"REMOTE_ADDR": "10.10.2.1"},
            )
            assert response.status_code == 302

        limited = client.post(
            "/team/invites",
            data={"email": "invite-over-limit@example.com"},
            environ_base={"REMOTE_ADDR": "10.10.2.1"},
        )

    assert limited.status_code == 429


def test_upload_deals_rate_limit_applies_after_twenty_posts(tmp_path):
    app = _build_app(tmp_path, csrf_enabled=False)
    auth_scope = _seed_authenticated_scope(app, user_id=3001)

    with app.test_client() as client:
        _login_session(client, auth_scope)
        for _ in range(20):
            response = client.post(
                "/upload/deals",
                data={},
                content_type="multipart/form-data",
                environ_base={"REMOTE_ADDR": "10.10.3.1"},
            )
            assert response.status_code == 302

        limited = client.post(
            "/upload/deals",
            data={},
            content_type="multipart/form-data",
            environ_base={"REMOTE_ADDR": "10.10.3.1"},
        )

    assert limited.status_code == 429


def test_chart_builder_query_rate_limit_applies_after_sixty_posts(tmp_path):
    app = _build_app(tmp_path, csrf_enabled=False)
    auth_scope = _seed_authenticated_scope(app, user_id=4001)

    with app.test_client() as client:
        _login_session(client, auth_scope)
        for _ in range(60):
            response = client.post(
                "/api/chart-builder/query",
                json={},
                environ_base={"REMOTE_ADDR": "10.10.4.1"},
            )
            assert response.status_code == 400

        limited = client.post(
            "/api/chart-builder/query",
            json={},
            environ_base={"REMOTE_ADDR": "10.10.4.1"},
        )

    assert limited.status_code == 429

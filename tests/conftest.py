import pytest
import os
import sys
from werkzeug.security import generate_password_hash

# Add application root to path so we can import app and models
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

TEST_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "test_runtime.db"))
os.environ["DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH}"
if os.path.exists(TEST_DB_PATH):
    os.remove(TEST_DB_PATH)

from app import app, db
from models import Firm, Team, TeamFirmAccess, TeamMembership, User


app.config.update(TESTING=True, WTF_CSRF_ENABLED=False, MEMO_INLINE_JOBS=True, MEMO_WEB_ASYNC_JOBS=False)

@pytest.fixture
def anonymous_client():
    with app.test_client() as client:
        with app.app_context():
            db.session.remove()
            db.drop_all()
            db.create_all()
            yield client
            db.session.remove()
            db.drop_all()


@pytest.fixture
def client(anonymous_client):
    with app.app_context():
        team = Team(name="Test Team", slug="test-team")
        firm = Firm(name="Test Firm", slug="test-firm")
        user = User(
            email="tester@example.com",
            password_hash=generate_password_hash("password123"),
            is_active=True,
        )
        db.session.add_all([team, firm, user])
        db.session.flush()
        db.session.add(TeamMembership(team_id=team.id, user_id=user.id, role="owner"))
        db.session.add(TeamFirmAccess(team_id=team.id, firm_id=firm.id, created_by_user_id=user.id))
        db.session.commit()

        user_id = user.id
        team_id = team.id
        firm_id = firm.id

    with anonymous_client.session_transaction() as sess:
        sess["_user_id"] = str(user_id)
        sess["_fresh"] = True
        sess["active_team_id"] = team_id
        sess["active_firm_id"] = firm_id

    return anonymous_client

@pytest.fixture
def app_context():
    with app.app_context():
        db.session.remove()
        db.drop_all()
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()

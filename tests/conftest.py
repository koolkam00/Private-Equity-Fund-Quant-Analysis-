import pytest
import os
import sys

# Add application root to path so we can import app and models
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

TEST_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "test_runtime.db"))
os.environ["DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH}"

from app import app, db


app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)

@pytest.fixture
def client():
    with app.test_client() as client:
        with app.app_context():
            db.session.remove()
            db.drop_all()
            db.create_all()
            yield client
            db.session.remove()
            db.drop_all()

@pytest.fixture
def app_context():
    with app.app_context():
        db.session.remove()
        db.drop_all()
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()

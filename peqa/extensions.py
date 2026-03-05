from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_login import LoginManager, current_user
from flask_migrate import Migrate
from flask_wtf.csrf import CSRFProtect

from models import db


def _user_or_ip():
    if current_user.is_authenticated:
        return f"user:{current_user.get_id()}"
    return get_remote_address()


login_manager = LoginManager()
csrf = CSRFProtect()
migrate = Migrate()
limiter = Limiter(key_func=_user_or_ip, default_limits=[], storage_uri="memory://")


"""
FastAPI dependency providers.
"""
from app.db.session import get_session
from app.middleware.auth import get_current_user, require_admin

__all__ = [
    "get_session",
    "get_current_user",
    "require_admin",
]

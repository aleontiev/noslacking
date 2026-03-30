"""Google Admin Directory API client for user lookup."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Generator

from noslacking.google.auth import get_admin_service
from noslacking.google.models import GoogleUser
from noslacking.utils.retry import google_retry

logger = logging.getLogger(__name__)


class GoogleAdminClient:
    """Wraps Admin Directory API for listing Google Workspace users."""

    def __init__(self, key_path: Path, admin_email: str):
        self.service = get_admin_service(key_path, admin_email)

    @google_retry
    def list_users(self, domain: str | None = None) -> Generator[GoogleUser, None, None]:
        """List all Google Workspace users, optionally filtered by domain."""
        page_token = None
        while True:
            kwargs: dict = {"customer": "my_customer", "maxResults": 200}
            if domain:
                kwargs["domain"] = domain
            if page_token:
                kwargs["pageToken"] = page_token

            result = self.service.users().list(**kwargs).execute()

            for user_data in result.get("users", []):
                yield GoogleUser.from_api(user_data)

            page_token = result.get("nextPageToken")
            if not page_token:
                break

    @google_retry
    def get_user(self, email: str) -> GoogleUser | None:
        """Get a single user by email."""
        try:
            result = self.service.users().get(userKey=email).execute()
            return GoogleUser.from_api(result)
        except Exception:
            return None

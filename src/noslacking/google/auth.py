"""Google API authentication with service account + domain-wide delegation."""

from __future__ import annotations

import logging
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource

logger = logging.getLogger(__name__)

# Scopes needed for migration
CHAT_SCOPES = [
    "https://www.googleapis.com/auth/chat.import",  # Import mode (create spaces, messages)
    "https://www.googleapis.com/auth/chat.spaces",  # Manage spaces
    "https://www.googleapis.com/auth/chat.spaces.create",
    "https://www.googleapis.com/auth/chat.memberships",
    "https://www.googleapis.com/auth/chat.messages",
    "https://www.googleapis.com/auth/chat.messages.create",
]

ADMIN_SCOPES = [
    "https://www.googleapis.com/auth/admin.directory.user.readonly",
]

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
]


def _load_credentials(
    key_path: Path, scopes: list[str], subject: str | None = None,
) -> service_account.Credentials:
    """Load service account credentials with optional impersonation."""
    creds = service_account.Credentials.from_service_account_file(
        str(key_path), scopes=scopes,
    )
    if subject:
        creds = creds.with_subject(subject)
    return creds


def get_chat_service(
    key_path: Path, impersonate_email: str | None = None,
) -> Resource:
    """Build Google Chat API v1 service, optionally impersonating a user."""
    creds = _load_credentials(key_path, CHAT_SCOPES, subject=impersonate_email)
    return build("chat", "v1", credentials=creds, cache_discovery=False)


def get_admin_service(key_path: Path, admin_email: str) -> Resource:
    """Build Admin Directory API service (requires admin impersonation)."""
    creds = _load_credentials(key_path, ADMIN_SCOPES, subject=admin_email)
    return build("admin", "directory_v1", credentials=creds, cache_discovery=False)


def get_drive_service(
    key_path: Path, impersonate_email: str | None = None,
) -> Resource:
    """Build Google Drive API v3 service."""
    creds = _load_credentials(key_path, DRIVE_SCOPES, subject=impersonate_email)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def validate_credentials(key_path: Path, admin_email: str) -> dict[str, bool]:
    """Validate that all credential configurations work."""
    results: dict[str, bool] = {}

    # Test Chat API
    try:
        svc = get_chat_service(key_path, impersonate_email=admin_email)
        svc.spaces().list(pageSize=1).execute()
        results["chat_api"] = True
    except Exception as e:
        logger.error(f"Chat API validation failed: {e}")
        results["chat_api"] = False

    # Test Admin API
    try:
        svc = get_admin_service(key_path, admin_email)
        svc.users().list(customer="my_customer", maxResults=1).execute()
        results["admin_api"] = True
    except Exception as e:
        logger.error(f"Admin API validation failed: {e}")
        results["admin_api"] = False

    return results

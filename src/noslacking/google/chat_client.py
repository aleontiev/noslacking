"""Google Chat API client for migration operations (import mode)."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from googleapiclient.errors import HttpError

from noslacking.google.auth import get_chat_service
from noslacking.utils.retry import google_retry

logger = logging.getLogger(__name__)


class GoogleChatClient:
    """Wraps Google Chat API v1 for import mode migration."""

    def __init__(
        self,
        key_path: Path,
        admin_email: str,
        messages_per_second: int = 8,
    ):
        self.key_path = key_path
        self.admin_email = admin_email
        self.messages_per_second = messages_per_second
        self._min_interval = 1.0 / messages_per_second
        self._last_call_time = 0.0

        # Default service impersonating admin
        self._admin_service = get_chat_service(key_path, impersonate_email=admin_email)
        self._service_cache: dict[str, object] = {}

    def _get_service(self, impersonate_email: str | None = None):
        """Get a Chat service, optionally impersonating a user."""
        if not impersonate_email or impersonate_email == self.admin_email:
            return self._admin_service

        if impersonate_email not in self._service_cache:
            self._service_cache[impersonate_email] = get_chat_service(
                self.key_path, impersonate_email=impersonate_email,
            )
        return self._service_cache[impersonate_email]

    def _rate_limit(self) -> None:
        """Simple rate limiter to stay under per-space write limits."""
        elapsed = time.monotonic() - self._last_call_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call_time = time.monotonic()

    @google_retry
    def create_import_space(
        self,
        display_name: str,
        description: str = "",
        create_time: datetime | None = None,
    ) -> dict:
        """Create a space in import mode."""
        self._rate_limit()
        body: dict = {
            "spaceType": "SPACE",
            "displayName": display_name,
            "importMode": True,
        }
        if description:
            body["spaceDetails"] = {"description": description}
        if create_time:
            body["createTime"] = create_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        result = self._admin_service.spaces().create(body=body).execute()
        logger.info(f"Created import space: {result.get('name')} ({display_name})")
        return result

    @google_retry
    def complete_import_space(self, space_name: str) -> dict:
        """Complete import mode on a space, making it visible to users."""
        self._rate_limit()
        result = (
            self._admin_service
            .spaces()
            .completeImport(name=space_name, body={})
            .execute()
        )
        logger.info(f"Completed import for space: {space_name}")
        return result

    @google_retry
    def create_import_membership(
        self,
        space_name: str,
        user_email: str,
        create_time: datetime | None = None,
        delete_time: datetime | None = None,
    ) -> dict:
        """Add a historical membership in import mode. Requires deleteTime."""
        self._rate_limit()
        body: dict = {
            "member": {
                "name": f"users/{user_email}",
                "type": "HUMAN",
            }
        }
        if create_time:
            body["createTime"] = create_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if delete_time:
            body["deleteTime"] = delete_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        try:
            result = self._admin_service.spaces().members().create(
                parent=space_name, body=body,
            ).execute()
            return result
        except HttpError as e:
            if e.resp.status == 409:
                logger.debug(f"Member {user_email} already exists in {space_name}")
                return {"name": f"{space_name}/members/{user_email}", "already_exists": True}
            raise

    @google_retry
    def create_membership(
        self,
        space_name: str,
        user_email: str,
        impersonate_email: str | None = None,
    ) -> dict:
        """Add an active member to a space (post-import mode)."""
        self._rate_limit()
        service = self._get_service(impersonate_email)
        body = {
            "member": {
                "name": f"users/{user_email}",
                "type": "HUMAN",
            }
        }
        try:
            result = service.spaces().members().create(
                parent=space_name, body=body,
            ).execute()
            return result
        except HttpError as e:
            if e.resp.status == 409:
                logger.debug(f"Member {user_email} already exists in {space_name}")
                return {"name": f"{space_name}/members/{user_email}", "already_exists": True}
            raise

    @google_retry
    def create_message(
        self,
        space_name: str,
        text: str,
        thread_key: str | None = None,
        create_time: datetime | None = None,
        impersonate_email: str | None = None,
        cards: list[dict] | None = None,
    ) -> dict:
        """Create a message in a space (import mode preserves createTime)."""
        self._rate_limit()
        service = self._get_service(impersonate_email)

        body: dict = {"text": text}
        if thread_key:
            body["thread"] = {"threadKey": thread_key}
        if create_time:
            body["createTime"] = create_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if cards:
            body["cardsV2"] = cards

        kwargs: dict = {"parent": space_name, "body": body}
        if thread_key:
            kwargs["messageReplyOption"] = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"

        result = service.spaces().messages().create(**kwargs).execute()
        return result

    @google_retry
    def create_reaction(
        self,
        message_name: str,
        emoji: str,
        impersonate_email: str | None = None,
    ) -> dict | None:
        """Add a reaction to a message. Returns None if unsupported emoji."""
        self._rate_limit()
        service = self._get_service(impersonate_email)
        body = {
            "emoji": {"unicode": emoji},
        }
        try:
            return service.spaces().messages().reactions().create(
                parent=message_name, body=body,
            ).execute()
        except HttpError as e:
            if e.resp.status == 400:
                logger.debug(f"Unsupported emoji reaction: {emoji}")
                return None
            raise

    @google_retry
    def upload_attachment(
        self,
        space_name: str,
        file_path: Path,
        filename: str,
    ) -> dict:
        """Upload a file attachment to a space."""
        from googleapiclient.http import MediaFileUpload

        self._rate_limit()
        media = MediaFileUpload(str(file_path), resumable=True)
        result = (
            self._admin_service
            .media()
            .upload(
                parent=space_name,
                body={"filename": filename},
                media_body=media,
            )
            .execute()
        )
        return result

    @google_retry
    def delete_space(self, space_name: str) -> None:
        """Delete a space (useful for cleanup during testing)."""
        self._admin_service.spaces().delete(name=space_name).execute()
        logger.info(f"Deleted space: {space_name}")

    def list_spaces(self, page_size: int = 100) -> list[dict]:
        """List all spaces visible to the admin."""
        spaces = []
        page_token = None
        while True:
            kwargs: dict = {"pageSize": page_size}
            if page_token:
                kwargs["pageToken"] = page_token
            result = self._admin_service.spaces().list(**kwargs).execute()
            spaces.extend(result.get("spaces", []))
            page_token = result.get("nextPageToken")
            if not page_token:
                break
        return spaces

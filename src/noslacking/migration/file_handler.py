"""Handle file downloads from Slack and uploads to Google Chat/Drive."""

from __future__ import annotations

import logging
from pathlib import Path

from noslacking.config import Settings
from noslacking.google.chat_client import GoogleChatClient
from noslacking.slack.client import SlackClient

logger = logging.getLogger(__name__)


class FileHandler:
    """Download files from Slack and upload to Google Chat or Drive."""

    def __init__(
        self,
        slack_client: SlackClient,
        chat_client: GoogleChatClient,
        settings: Settings,
    ):
        self.slack = slack_client
        self.chat = chat_client
        self.settings = settings
        self.cache_dir = settings.cache_path / "files"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_size = settings.slack.max_file_size_mb * 1024 * 1024

    def download_file(self, file_id: str, url: str, filename: str | None = None, size: int | None = None) -> Path | None:
        """Download a file from Slack to local cache. Returns local path.

        Does NOT touch the database — caller is responsible for DB updates.
        """
        if not url:
            logger.warning(f"No download URL for file {file_id}")
            return None

        if size and size > self.max_size:
            logger.warning(f"Skipping file {filename} — {size / 1024 / 1024:.1f}MB exceeds limit")
            return None

        # Check if already downloaded
        local_path = self.cache_dir / f"{file_id}_{filename}" if filename else self.cache_dir / file_id
        if local_path.exists():
            return local_path

        try:
            data = self.slack.download_file_url(url)
        except Exception as e:
            logger.error(f"Failed to download file {file_id}: {e}")
            return None

        local_path.write_bytes(data)
        return local_path

    def upload_to_chat(self, local_path: Path, space_name: str, filename: str) -> str | None:
        """Upload a file to Google Chat. Returns attachment resource name."""
        try:
            result = self.chat.upload_attachment(space_name, local_path, filename)
            return result.get("name", "")
        except Exception as e:
            logger.error(f"Failed to upload {filename} to Chat: {e}")
            return None

    def upload_to_drive(
        self,
        local_path: Path,
        filename: str,
        commenter_emails: list[str] | None = None,
    ) -> str | None:
        """Upload a file to Google Drive. Returns the Drive file URL.

        If commenter_emails is provided, each gets commenter access on the
        resulting file (the admin uploader keeps owner/edit access).
        """
        from googleapiclient.http import MediaFileUpload
        from noslacking.google.auth import get_drive_service

        try:
            drive = get_drive_service(
                self.settings.service_account_key_path,
                impersonate_email=self.settings.google.admin_email,
            )
            file_metadata: dict = {"name": filename}
            media = MediaFileUpload(str(local_path), resumable=True)
            result = drive.files().create(
                body=file_metadata, media_body=media, fields="id,webViewLink",
            ).execute()
            file_id = result.get("id")
            if file_id and commenter_emails:
                self._grant_commenters(drive, file_id, commenter_emails)
            return result.get("webViewLink", "")
        except Exception as e:
            logger.error(f"Failed to upload {filename} to Drive: {e}")
            return None

    def _grant_commenters(self, drive, file_id: str, emails: list[str]) -> None:
        """Grant commenter role to each email on the given Drive file."""
        admin_email = self.settings.google.admin_email
        for email in {e for e in emails if e and e != admin_email}:
            try:
                drive.permissions().create(
                    fileId=file_id,
                    body={"role": "commenter", "type": "user", "emailAddress": email},
                    sendNotificationEmail=False,
                    fields="id",
                ).execute()
            except Exception as e:
                logger.debug(f"Failed to grant commenter to {email} on {file_id}: {e}")

"""Slack WebClient wrapper with rate limiting and pagination."""

from __future__ import annotations

import logging
from typing import Generator

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

from noslacking.slack.models import SlackChannel, SlackFile, SlackMessage, SlackUser

logger = logging.getLogger(__name__)


class SlackClient:
    """Wraps slack_sdk.WebClient with rate limiting and paginated helpers.

    Prefers the user token for broader access (private channels, DMs),
    falls back to bot token if user token is unavailable or fails.
    """

    def __init__(self, bot_token: str, user_token: str | None = None):
        self.bot = WebClient(token=bot_token)
        self.bot.retry_handlers.append(RateLimitErrorRetryHandler(max_retry_count=5))

        self.user: WebClient | None = None
        if user_token:
            self.user = WebClient(token=user_token)
            self.user.retry_handlers.append(RateLimitErrorRetryHandler(max_retry_count=5))

    @property
    def _primary(self) -> WebClient:
        """Return user token client if available, otherwise bot."""
        return self.user or self.bot

    def _call_with_fallback(self, method_name: str, **kwargs):
        """Try user token first, fall back to bot token on failure."""
        if self.user:
            try:
                return getattr(self.user, method_name)(**kwargs)
            except SlackApiError as e:
                error = e.response.get("error", "")
                if error in ("missing_scope", "not_in_channel", "channel_not_found"):
                    logger.debug(f"User token failed ({error}), falling back to bot token")
                else:
                    raise
        return getattr(self.bot, method_name)(**kwargs)

    def test_auth(self) -> dict:
        """Validate bot token and return auth info."""
        resp = self.bot.auth_test()
        return resp.data

    def test_user_auth(self) -> dict | None:
        """Validate user token if available."""
        if self.user:
            resp = self.user.auth_test()
            return resp.data
        return None

    def list_channels(
        self, types: list[str] | None = None, exclude_archived: bool = True,
    ) -> Generator[SlackChannel, None, None]:
        """List all channels with pagination. Uses user token for broader access."""
        types_str = ",".join(types or ["public_channel", "private_channel"])
        cursor = None
        client = self._primary

        while True:
            try:
                resp = client.conversations_list(
                    types=types_str,
                    exclude_archived=exclude_archived,
                    limit=200,
                    cursor=cursor,
                )
            except SlackApiError as e:
                logger.error(f"Error listing channels: {e.response['error']}")
                raise

            for ch in resp.get("channels", []):
                yield SlackChannel.from_api(ch)

            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

    def get_channel_info(self, channel_id: str) -> SlackChannel:
        resp = self._call_with_fallback("conversations_info", channel=channel_id)
        return SlackChannel.from_api(resp["channel"])

    def get_channel_members(self, channel_id: str) -> Generator[str, None, None]:
        """Yield member user IDs for a channel."""
        cursor = None
        while True:
            resp = self._call_with_fallback(
                "conversations_members", channel=channel_id, limit=200, cursor=cursor,
            )
            for uid in resp.get("members", []):
                yield uid

            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

    def get_history(
        self, channel_id: str, limit: int = 200,
        oldest: str | None = None, latest: str | None = None,
        cursor: str | None = None,
    ) -> Generator[SlackMessage, None, None]:
        """Yield all messages in a channel. Uses user token for private channel access."""
        client = self._primary

        while True:
            kwargs: dict = {"channel": channel_id, "limit": limit}
            if oldest:
                kwargs["oldest"] = oldest
            if latest:
                kwargs["latest"] = latest
            if cursor:
                kwargs["cursor"] = cursor

            try:
                resp = client.conversations_history(**kwargs)
            except SlackApiError as e:
                # Fall back to bot if user token can't access this channel
                if self.user and client is self.user and e.response.get("error") in (
                    "missing_scope", "not_in_channel", "channel_not_found",
                ):
                    logger.debug(f"User token failed for history, falling back to bot")
                    client = self.bot
                    continue
                logger.error(f"Error fetching history for {channel_id}: {e.response['error']}")
                raise

            for msg in resp.get("messages", []):
                yield SlackMessage.from_api(msg)

            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor or not resp.get("has_more"):
                break

    def get_thread_replies(
        self, channel_id: str, thread_ts: str,
    ) -> Generator[SlackMessage, None, None]:
        """Yield all replies in a thread (including parent)."""
        cursor = None
        client = self._primary

        while True:
            try:
                resp = client.conversations_replies(
                    channel=channel_id, ts=thread_ts, limit=200, cursor=cursor,
                )
            except SlackApiError as e:
                if self.user and client is self.user and e.response.get("error") in (
                    "missing_scope", "not_in_channel", "channel_not_found",
                ):
                    logger.debug(f"User token failed for thread, falling back to bot")
                    client = self.bot
                    continue
                logger.error(f"Error fetching thread {thread_ts}: {e.response['error']}")
                raise

            for msg in resp.get("messages", []):
                yield SlackMessage.from_api(msg)

            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor or not resp.get("has_more"):
                break

    def list_users(self) -> Generator[SlackUser, None, None]:
        """List all workspace users."""
        cursor = None
        client = self._primary

        while True:
            try:
                resp = client.users_list(limit=200, cursor=cursor)
            except SlackApiError as e:
                if self.user and client is self.user:
                    logger.debug("User token failed for users_list, falling back to bot")
                    client = self.bot
                    continue
                logger.error(f"Error listing users: {e.response['error']}")
                raise

            for user in resp.get("members", []):
                yield SlackUser.from_api(user)

            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

    def get_file_info(self, file_id: str) -> SlackFile:
        resp = self._call_with_fallback("files_info", file=file_id)
        return SlackFile.from_api(resp["file"])

    def download_file_url(self, url: str) -> bytes:
        """Download a file from Slack. Tries user token first for broader access."""
        import httpx

        token = self._primary.token
        headers = {"Authorization": f"Bearer {token}"}
        with httpx.Client(follow_redirects=True, timeout=120) as http:
            resp = http.get(url, headers=headers)
            if resp.status_code in (401, 403) and self.user and token != self.bot.token:
                # Retry with bot token
                headers = {"Authorization": f"Bearer {self.bot.token}"}
                resp = http.get(url, headers=headers)
            resp.raise_for_status()
            return resp.content

"""Full Slack workspace extraction via API."""

from __future__ import annotations

import json
import logging
import signal
import uuid
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from noslacking.config import Settings
from noslacking.db.engine import get_session
from noslacking.db.operations import (
    claim_channel,
    now_utc,
    release_channel,
    upsert_channel,
    upsert_file,
    upsert_membership,
    upsert_message,
    upsert_user,
    update_channel_status,
    get_channel,
)
from noslacking.slack.client import SlackClient
from noslacking.slack.models import SlackMessage

logger = logging.getLogger(__name__)


class SlackExtractor:
    """Orchestrates extraction of all Slack data into local cache + SQLite."""

    def __init__(self, client: SlackClient, settings: Settings, worker_id: str | None = None):
        self.client = client
        self.settings = settings
        self.worker_id = worker_id or str(uuid.uuid4())
        self.cache_dir = settings.cache_path
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._current_channel_id: str | None = None
        self._install_signal_handlers()

    def _install_signal_handlers(self) -> None:
        """Register handlers to release channel claims on kill/interrupt."""
        def _cleanup(signum, frame):
            if self._current_channel_id:
                logger.info(f"Signal {signum} received — releasing claim on {self._current_channel_id}")
                try:
                    with get_session() as session:
                        update_channel_status(session, self._current_channel_id, "pending")
                        release_channel(session, self._current_channel_id, self.worker_id)
                except Exception:
                    pass  # Best effort
            raise SystemExit(1)

        signal.signal(signal.SIGTERM, _cleanup)
        signal.signal(signal.SIGINT, _cleanup)

    def extract_all(
        self,
        channel_filter: list[str] | None = None,
        channel_types: list[str] | None = None,
        since: str | None = None,
        skip_files: bool = False,
        skip_threads: bool = False,
        resume: bool = True,
        force: bool = False,
    ) -> dict:
        """Run full extraction. Returns summary stats."""
        stats = {"channels": 0, "messages": 0, "threads": 0, "users": 0, "files": 0}

        # Extract users first
        stats["users"] = self._extract_users()

        # List and filter channels
        types_to_list = channel_types or self.settings.slack.channel_types
        channels = list(self.client.list_channels(
            types=types_to_list,
            exclude_archived=not self.settings.slack.include_archived,
        ))

        if channel_filter:
            channels = [c for c in channels if c.name in channel_filter]
        if self.settings.slack.include_channels:
            channels = [c for c in channels if c.name in self.settings.slack.include_channels]
        if self.settings.slack.exclude_channels:
            channels = [c for c in channels if c.name not in self.settings.slack.exclude_channels]

        # Store channels in DB
        with get_session() as session:
            for ch in channels:
                upsert_channel(
                    session,
                    slack_channel_id=ch.id,
                    name=ch.name,
                    channel_type=ch.channel_type,
                    is_archived=ch.is_archived,
                    member_count=ch.num_members,
                    topic=ch.topic,
                    purpose=ch.purpose,
                )
            stats["channels"] = len(channels)

        # Extract each channel
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[green]{task.completed}/{task.total}"),
        ) as progress:
            overall = progress.add_task("Extracting channels", total=len(channels))

            for ch in channels:
                progress.update(overall, description=f"Extracting #{ch.name}")

                # Check resume state
                if resume:
                    with get_session() as session:
                        db_ch = get_channel(session, ch.id)
                        if db_ch and db_ch.migration_status in ("extracted", "migrating_messages", "completed"):
                            logger.info(f"Skipping #{ch.name} — already extracted")
                            progress.advance(overall)
                            continue

                # Try to claim the channel (atomic — safe with parallel workers)
                with get_session() as session:
                    claimed = claim_channel(session, ch.id, self.worker_id)

                if not claimed:
                    if force:
                        # Force-clear the stale lock and reclaim
                        with get_session() as session:
                            update_channel_status(session, ch.id, "pending")
                            release_channel(session, ch.id, self.worker_id)
                        with get_session() as session:
                            claimed = claim_channel(session, ch.id, self.worker_id)
                    if not claimed:
                        from noslacking.utils.logging import console
                        console.print(
                            f"  [yellow]Skipping #{ch.name} — locked by another worker. "
                            f"If the other process is dead, re-run with --force to clear the lock.[/yellow]"
                        )
                        progress.advance(overall)
                        continue

                self._current_channel_id = ch.id
                from noslacking.utils.logging import console
                slack_url = f"https://asaak.slack.com/archives/{ch.id}"
                console.print(f"  [dim]Slack: {slack_url}[/dim]")
                # Show Google Chat link if already migrated previously
                with get_session() as session:
                    db_ch = get_channel(session, ch.id)
                    if db_ch and db_ch.google_space_name:
                        space_id = db_ch.google_space_name.split("/")[-1]
                        console.print(f"  [dim]GChat: https://chat.google.com/room/{space_id}[/dim]")
                try:
                    ch_stats = self._extract_channel(
                        ch.id, ch.name, since=since,
                        skip_files=skip_files, skip_threads=skip_threads,
                        progress=progress,
                    )
                    stats["messages"] += ch_stats["messages"]
                    stats["threads"] += ch_stats["threads"]
                    stats["files"] += ch_stats["files"]
                except Exception:
                    # On failure, release claim so another worker can retry
                    with get_session() as session:
                        update_channel_status(session, ch.id, "pending")
                        release_channel(session, ch.id, self.worker_id)
                    raise
                finally:
                    self._current_channel_id = None
                    progress.advance(overall)

        return stats

    def _extract_users(self) -> int:
        """Extract all users and store in DB."""
        count = 0
        logger.info("Extracting users...")

        # Cache raw user data
        users_file = self.cache_dir / "users.json"
        all_users = []

        with get_session() as session:
            for user in self.client.list_users():
                all_users.append(user.model_dump())
                upsert_user(
                    session,
                    slack_user_id=user.id,
                    slack_email=user.email or None,
                    slack_display_name=user.display_name,
                    slack_real_name=user.real_name,
                    is_bot=user.is_bot,
                    is_deleted=user.is_deleted,
                )
                count += 1

        with open(users_file, "w") as f:
            json.dump(all_users, f, indent=2)

        logger.info(f"Extracted {count} users")
        return count

    def _extract_channel(
        self, channel_id: str, channel_name: str,
        since: str | None = None,
        skip_files: bool = False,
        skip_threads: bool = False,
        progress: Progress | None = None,
    ) -> dict:
        """Extract a single channel's messages, threads, members, and files."""
        stats = {"messages": 0, "threads": 0, "files": 0}
        ch_task = None
        if progress:
            ch_task = progress.add_task(f"#{channel_name}", total=None)

        # Extract members
        self._extract_members(channel_id)

        # Extract messages
        messages: list[SlackMessage] = []
        thread_parents: list[str] = []

        for msg in self.client.get_history(
            channel_id,
            limit=self.settings.slack.messages_per_page,
            oldest=since,
        ):
            messages.append(msg)
            stats["messages"] += 1

            if msg.is_thread_parent and not skip_threads:
                thread_parents.append(msg.ts)

            if not skip_files:
                for f in msg.files:
                    stats["files"] += 1

            if ch_task is not None and stats["messages"] % 50 == 0:
                progress.update(ch_task, description=f"#{channel_name} msgs", completed=stats["messages"])

        if ch_task is not None:
            progress.update(ch_task, description=f"#{channel_name} storing", completed=stats["messages"])

        # Store messages in DB
        with get_session() as session:
            for msg in messages:
                upsert_message(
                    session,
                    slack_channel_id=channel_id,
                    slack_ts=msg.ts,
                    slack_thread_ts=msg.thread_ts if msg.is_thread_reply else None,
                    slack_user_id=msg.user,
                    message_type=msg.subtype or msg.msg_type,
                    text_preview=msg.text[:500] if msg.text else None,
                    has_files=len(msg.files) > 0,
                    has_reactions=len(msg.reactions) > 0,
                    raw_json=json.dumps(msg.raw),
                )

                if not skip_files:
                    for f in msg.files:
                        upsert_file(
                            session,
                            slack_file_id=f.id,
                            slack_channel_id=channel_id,
                            slack_message_ts=msg.ts,
                            filename=f.name,
                            mimetype=f.mimetype,
                            size_bytes=f.size,
                            slack_url_private=f.url_private_download or f.url_private,
                        )

        # Extract thread replies
        if not skip_threads and thread_parents:
            if ch_task is not None:
                progress.update(ch_task, description=f"#{channel_name} threads", total=len(thread_parents), completed=0)

            for i, thread_ts in enumerate(thread_parents):
                thread_msgs = list(self.client.get_thread_replies(channel_id, thread_ts))
                replies = [m for m in thread_msgs if m.ts != thread_ts]
                stats["threads"] += 1

                with get_session() as session:
                    for msg in replies:
                        upsert_message(
                            session,
                            slack_channel_id=channel_id,
                            slack_ts=msg.ts,
                            slack_thread_ts=thread_ts,
                            slack_user_id=msg.user,
                            message_type=msg.subtype or msg.msg_type,
                            text_preview=msg.text[:500] if msg.text else None,
                            has_files=len(msg.files) > 0,
                            has_reactions=len(msg.reactions) > 0,
                            raw_json=json.dumps(msg.raw),
                        )
                        stats["messages"] += 1

                        if not skip_files:
                            for f in msg.files:
                                upsert_file(
                                    session,
                                    slack_file_id=f.id,
                                    slack_channel_id=channel_id,
                                    slack_message_ts=msg.ts,
                                    filename=f.name,
                                    mimetype=f.mimetype,
                                    size_bytes=f.size,
                                    slack_url_private=f.url_private_download or f.url_private,
                                )
                                stats["files"] += 1

                if ch_task is not None:
                    progress.update(ch_task, completed=i + 1)

        if ch_task is not None:
            progress.remove_task(ch_task)

        # Cache raw data
        ch_cache = self.cache_dir / "channels" / channel_id
        ch_cache.mkdir(parents=True, exist_ok=True)
        with open(ch_cache / "messages.json", "w") as f:
            json.dump([m.raw for m in messages], f)

        # Mark extracted and release claim
        with get_session() as session:
            update_channel_status(
                session, channel_id, "extracted",
                extracted_at=now_utc(),
                message_count=stats["messages"],
            )
            release_channel(session, channel_id, self.worker_id)

        logger.info(
            f"#{channel_name}: {stats['messages']} messages, "
            f"{stats['threads']} threads, {stats['files']} files"
        )
        return stats

    def _extract_members(self, channel_id: str) -> None:
        """Extract channel members."""
        with get_session() as session:
            for uid in self.client.get_channel_members(channel_id):
                upsert_membership(
                    session,
                    slack_channel_id=channel_id,
                    slack_user_id=uid,
                )

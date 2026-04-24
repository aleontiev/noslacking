"""Main migration executor — orchestrates the full Slack → Google Chat migration."""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from noslacking.config import Settings
from noslacking.db.engine import get_session
from noslacking.db.models import Channel, File, Membership, Message, User
from sqlalchemy import and_, func, select
from noslacking.db.operations import (
    get_channels,
    get_pending_memberships,
    get_pending_messages,
    get_pending_thread_messages,
    get_message_stats,
    log_skipped_feature,
    now_utc,
    update_channel_status,
)
from noslacking.google.chat_client import GoogleChatClient
from noslacking.migration.file_handler import FileHandler
from noslacking.migration.message_transform import (
    build_attribution_text,
    build_file_card,
    slack_ts_to_datetime,
    transform_message_text,
)
from noslacking.slack.client import SlackClient
from noslacking.utils.logging import console

logger = logging.getLogger(__name__)

# Slack emoji names that map to Unicode
EMOJI_MAP: dict[str, str] = {
    "+1": "👍", "thumbsup": "👍", "-1": "👎", "thumbsdown": "👎",
    "heart": "❤️", "joy": "😂", "fire": "🔥", "eyes": "👀",
    "rocket": "🚀", "tada": "🎉", "pray": "🙏", "100": "💯",
    "wave": "👋", "clap": "👏", "thinking_face": "🤔", "white_check_mark": "✅",
    "x": "❌", "raised_hands": "🙌", "sob": "😭", "muscle": "💪",
}


class MigrationExecutor:
    """Orchestrates the full migration from Slack to Google Chat."""

    def __init__(
        self,
        slack_client: SlackClient,
        chat_client: GoogleChatClient,
        file_handler: FileHandler,
        settings: Settings,
    ):
        self.slack = slack_client
        self.chat = chat_client
        self.files = file_handler
        self.settings = settings

    def migrate_all(
        self,
        channel_filter: list[str] | None = None,
        dry_run: bool = False,
        resume: bool = True,
        skip_files: bool = False,
        skip_members: bool = False,
        complete_import: bool = True,
        max_channels: int | None = None,
    ) -> dict:
        """Run the full migration. Returns stats."""
        stats = {
            "channels_created": 0,
            "channels_completed": 0,
            "messages_migrated": 0,
            "messages_skipped": 0,
            "messages_failed": 0,
            "members_added": 0,
            "files_uploaded": 0,
        }

        # Get channels to migrate
        with get_session() as session:
            if resume:
                # Get channels not yet completed
                channels = [
                    ch for ch in get_channels(session)
                    if ch.migration_status not in ("completed",)
                ]
            else:
                channels = get_channels(session, status="extracted")
            # Detach from session so we can use them outside
            for ch in channels:
                session.expunge(ch)

        if channel_filter:
            channels = [ch for ch in channels if ch.name in channel_filter]

        # Sort by message count (smallest first for quick wins)
        channels.sort(key=lambda c: c.message_count or 0)

        if max_channels:
            channels = channels[:max_channels]

        if not channels:
            console.print("[yellow]No channels to migrate.[/yellow]")
            return stats

        console.print(f"[bold]Migrating {len(channels)} channels...[/bold]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[green]{task.completed}/{task.total}"),
        ) as progress:
            overall = progress.add_task("Channels", total=len(channels))

            for ch in channels:
                progress.update(overall, description=f"Migrating #{ch.name}")
                try:
                    ch_stats = self._migrate_channel(
                        ch, dry_run=dry_run, resume=resume,
                        skip_files=skip_files, skip_members=skip_members,
                        complete_import=complete_import,
                    )
                    for k, v in ch_stats.items():
                        stats[k] = stats.get(k, 0) + v
                except Exception as e:
                    logger.error(f"Failed to migrate #{ch.name}: {e}", exc_info=True)
                    with get_session() as session:
                        update_channel_status(session, ch.slack_channel_id, "failed")

                progress.advance(overall)

        return stats

    def _resolve_channel_display_name(self, channel: Channel) -> str:
        """Resolve a human-readable display name for DMs and group DMs."""
        if channel.channel_type == "im":
            # 1:1 DM — look up the other user's name via Slack API
            try:
                info = self.slack._primary.conversations_info(channel=channel.slack_channel_id)
                other_user_id = info["channel"].get("user")
                if other_user_id:
                    with get_session() as session:
                        user = session.get(User, other_user_id)
                        if user and user.slack_real_name:
                            other_name = user.slack_real_name.split()[0]  # First name only
                        elif user and user.slack_display_name:
                            other_name = user.slack_display_name
                        else:
                            other_name = other_user_id
                    # Look up admin's name — prefer display_name (e.g., "Ant")
                    admin_user = session.query(User).filter(User.slack_email == self.settings.google.admin_email).first()
                    if admin_user and admin_user.slack_display_name:
                        admin_name = admin_user.slack_display_name.capitalize()
                    elif admin_user and admin_user.slack_real_name:
                        admin_name = admin_user.slack_real_name.split()[0]
                    else:
                        admin_name = self.settings.google.admin_email.split("@")[0].capitalize()
                    return f"{admin_name} & {other_name}"
            except Exception:
                logger.debug(f"Could not resolve DM name for {channel.slack_channel_id}")
            return channel.name
        elif channel.channel_type == "mpim":
            # Group DM — parse usernames from the mpdm- name format
            name = channel.name
            if name.startswith("mpdm-") and name.endswith(("-1", "-2", "-3")):
                parts = name[5:].rsplit("-", 1)[0].split("--")
                with get_session() as session:
                    resolved = []
                    for part in parts:
                        # Try to find user by matching slack display name or email prefix
                        user = session.query(User).filter(
                            (User.slack_display_name == part) |
                            (User.slack_email.like(f"{part}@%"))
                        ).first()
                        if user:
                            name = user.slack_real_name.split()[0] if user.slack_real_name else (user.slack_display_name or part)
                            resolved.append(name)
                        else:
                            resolved.append(part.replace(".", " ").title())
                return " & ".join(resolved) if resolved else channel.name
            return channel.name
        else:
            return channel.name

    def _migrate_channel(
        self,
        channel: Channel,
        dry_run: bool = False,
        resume: bool = True,
        skip_files: bool = False,
        skip_members: bool = False,
        complete_import: bool = True,
    ) -> dict:
        """Migrate a single channel."""
        stats = {
            "channels_created": 0,
            "channels_completed": 0,
            "messages_migrated": 0,
            "messages_skipped": 0,
            "messages_failed": 0,
            "members_added": 0,
            "files_uploaded": 0,
        }

        space_name = channel.google_space_name

        # Step 1: Create import-mode space (if not already created)
        if not space_name:
            # Resolve human-readable names for DMs and group DMs
            friendly_name = self._resolve_channel_display_name(channel)
            # Apply type-specific prefix
            if channel.channel_type == "im":
                prefix = self.settings.migration.dm_space_prefix
            elif channel.channel_type == "mpim":
                prefix = self.settings.migration.group_dm_space_prefix
            else:
                prefix = ""
            display_name = prefix + self.settings.migration.space_name_template.format(name=friendly_name)
            description = self.settings.migration.space_description_template.format(
                name=friendly_name
            )

            if dry_run:
                console.print(f"  [dim][DRY RUN] Would create space: {display_name}[/dim]")
                space_name = f"spaces/DRY_RUN_{channel.slack_channel_id}"
            else:
                console.print(f"  [cyan]Creating import space:[/cyan] {display_name}")
                # Set createTime to an old date so historical messages/memberships work
                result = self.chat.create_import_space(
                    display_name=display_name,
                    description=description,
                    create_time=datetime(2013, 1, 1, tzinfo=timezone.utc),
                )
                space_name = result["name"]
                console.print(f"  [green]✓ Space created:[/green] {space_name}")

                with get_session() as session:
                    update_channel_status(
                        session, channel.slack_channel_id, "creating_space",
                        google_space_name=space_name,
                        google_space_created_at=now_utc(),
                    )

            stats["channels_created"] = 1

        # Print links for tracking
        slack_url = f"https://asaak.slack.com/archives/{channel.slack_channel_id}"
        space_id = space_name.split("/")[-1] if space_name else ""
        gchat_url = f"https://chat.google.com/room/{space_id}" if space_id else ""
        console.print(f"  [dim]Slack: {slack_url}[/dim]")
        console.print(f"  [dim]GChat: {gchat_url}[/dim]")

        # Step 2: Skip historical members — impersonation works without them.
        # Active members are added after completeImport (step 4).

        # Step 3: Migrate messages
        with get_session() as session:
            update_channel_status(session, channel.slack_channel_id, "migrating_messages")
            msg_count = session.scalar(
                select(func.count(Message.id)).where(
                    Message.slack_channel_id == channel.slack_channel_id,
                    Message.migration_status == "pending",
                )
            )
        console.print(f"  [cyan]Migrating {msg_count} messages...[/cyan]")

        msg_stats = self._migrate_messages(
            channel.slack_channel_id, space_name,
            dry_run=dry_run, skip_files=skip_files,
        )
        stats.update(msg_stats)
        console.print(
            f"  [green]✓ Messages:[/green] "
            f"{msg_stats.get('messages_migrated', 0)} migrated, "
            f"{msg_stats.get('messages_skipped', 0)} skipped, "
            f"{msg_stats.get('messages_failed', 0)} failed"
        )

        # Step 4: Complete import + re-add active members
        if complete_import and not dry_run:
            try:
                console.print(f"  [cyan]Completing import...[/cyan]")
                self.chat.complete_import_space(space_name)

                # Re-add members as active (they were historical during import)
                if not skip_members:
                    console.print(f"  [cyan]Re-adding active members...[/cyan]")
                    readded = self._readd_members_active(
                        channel.slack_channel_id, space_name,
                        channel_type=channel.channel_type,
                    )
                    console.print(f"  [green]✓ Re-added {readded} active members[/green]")

                with get_session() as session:
                    # Set last_sync_ts to latest migrated message for future syncs
                    latest_ts = session.scalar(
                        select(func.max(Message.slack_ts)).where(
                            Message.slack_channel_id == channel.slack_channel_id,
                            Message.migration_status == "migrated",
                        )
                    )
                    update_channel_status(
                        session, channel.slack_channel_id, "completed",
                        import_completed_at=now_utc(),
                        last_sync_ts=latest_ts,
                    )
                stats["channels_completed"] = 1
            except Exception as e:
                logger.error(f"Failed to complete import for #{channel.name}: {e}")
                with get_session() as session:
                    update_channel_status(session, channel.slack_channel_id, "failed")
        elif dry_run:
            logger.info(f"[DRY RUN] Would complete import for #{channel.name}")

        return stats

    def _migrate_members_import(self, channel_id: str, space_name: str) -> int:
        """Add historical memberships during import mode.

        In import mode, all memberships must have createTime + deleteTime.
        createTime must be after space createTime (2017-01-01).
        deleteTime must be in the past.
        """
        count = 0
        create_time = datetime(2017, 1, 2, tzinfo=timezone.utc)
        delete_time = now_utc() - timedelta(minutes=5)

        with get_session() as session:
            memberships = get_pending_memberships(session, channel_id)

            for mem in memberships:
                user = session.get(User, mem.slack_user_id)
                if not user or not user.google_email:
                    mem.migration_status = "skipped"
                    continue

                try:
                    result = self.chat.create_import_membership(
                        space_name, user.google_email,
                        create_time=create_time,
                        delete_time=delete_time,
                    )
                    mem.google_space_name = space_name
                    mem.google_member_name = result.get("name", "")
                    mem.migrated_at = now_utc()
                    mem.migration_status = "migrated"
                    count += 1
                except Exception as e:
                    logger.warning(f"Failed to add import member {user.google_email}: {e}")
                    mem.migration_status = "failed"

        return count

    def _readd_members_active(
        self, channel_id: str, space_name: str, channel_type: str = "",
    ) -> int:
        """Add members as active after completeImport.

        For 1:1 DMs (im) and group DMs (mpim), members are added as
        ROLE_MANAGER so both/all parties own the space equally.
        """
        role = "ROLE_MANAGER" if channel_type in ("im", "mpim") else "ROLE_MEMBER"
        count = 0
        with get_session() as session:
            all_mems = session.scalars(
                select(Membership).where(
                    Membership.slack_channel_id == channel_id,
                )
            ).all()

            for mem in all_mems:
                user = session.get(User, mem.slack_user_id)
                if not user or not user.google_email:
                    mem.migration_status = "skipped"
                    continue
                try:
                    self.chat.create_membership(space_name, user.google_email, role=role)
                    mem.migration_status = "migrated"
                    mem.migrated_at = now_utc()
                    mem.google_space_name = space_name
                    count += 1
                except Exception as e:
                    logger.debug(f"Add member {user.google_email}: {e}")
                    mem.migration_status = "failed"

        return count

    def _migrate_messages(
        self,
        channel_id: str,
        space_name: str,
        dry_run: bool = False,
        skip_files: bool = False,
    ) -> dict:
        """Migrate all pending messages for a channel."""
        stats = {"messages_migrated": 0, "messages_skipped": 0, "messages_failed": 0, "files_uploaded": 0}
        msg_count = 0

        # Build channel name lookup for @channel mentions
        channel_names: dict[str, str] = {}
        with get_session() as session:
            from noslacking.db.models import Channel as ChannelModel
            for ch in session.scalars(select(ChannelModel)).all():
                channel_names[ch.slack_channel_id] = ch.name

        # Pass 1: Process all top-level messages in batches
        console.print(f"    [dim]Pass 1: top-level messages...[/dim]")
        while True:
            with get_session() as session:
                messages = get_pending_messages(session, channel_id, limit=500)
                if not messages:
                    break

                for msg in messages:
                    result = self._migrate_single_message(
                        session, msg, space_name, channel_id, channel_names,
                        dry_run=dry_run, skip_files=skip_files,
                    )
                    stats[f"messages_{result}"] = stats.get(f"messages_{result}", 0) + 1
                    msg_count += 1
                    if msg_count % 50 == 0:
                        m = stats.get("messages_migrated", 0)
                        f = stats.get("messages_failed", 0)
                        console.print(f"    [dim]{msg_count} processed ({m} ok, {f} failed)[/dim]")

        console.print(f"    [dim]Pass 1 done: {msg_count} top-level messages[/dim]")

        # Pass 2: Process all thread replies — get thread keys from migrated parents
        with get_session() as session:
            parent_rows = session.execute(
                select(Message.slack_ts, Message.google_thread_key).where(
                    and_(
                        Message.slack_channel_id == channel_id,
                        Message.migration_status == "migrated",
                        Message.google_thread_key.isnot(None),
                        Message.slack_thread_ts.is_(None),
                    )
                )
            ).all()
        thread_key_map = {ts: key for ts, key in parent_rows}
        console.print(f"    [dim]Pass 2: {len(thread_key_map)} threads to process...[/dim]")

        # Now process replies in batches
        if thread_key_map:
            with get_session() as session:
                pending_replies = session.scalars(
                    select(Message).where(
                        and_(
                            Message.slack_channel_id == channel_id,
                            Message.migration_status == "pending",
                            Message.slack_thread_ts.isnot(None),
                        )
                    ).order_by(Message.slack_ts)
                ).all()

                for reply in pending_replies:
                    thread_key = thread_key_map.get(reply.slack_thread_ts)
                    if not thread_key:
                        reply.migration_status = "skipped"
                        reply.skip_reason = "parent_not_migrated"
                        stats["messages_skipped"] = stats.get("messages_skipped", 0) + 1
                        continue

                    result = self._migrate_single_message(
                        session, reply, space_name, channel_id, channel_names,
                        thread_key=thread_key,
                        dry_run=dry_run, skip_files=skip_files,
                    )
                    stats[f"messages_{result}"] = stats.get(f"messages_{result}", 0) + 1

        return stats

    def _migrate_single_message(
        self,
        session,
        msg: Message,
        space_name: str,
        channel_id: str,
        channel_names: dict[str, str],
        thread_key: str | None = None,
        dry_run: bool = False,
        skip_files: bool = False,
    ) -> str:
        """Migrate a single message. Returns status: 'migrated', 'skipped', or 'failed'."""
        # Skip system messages if configured
        system_subtypes = {
            "channel_join", "channel_leave", "channel_topic", "channel_purpose",
            "channel_name", "channel_archive", "channel_unarchive",
        }
        if msg.message_type in system_subtypes and not self.settings.migration.include_system_messages:
            msg.migration_status = "skipped"
            msg.skip_reason = "system_message"
            return "skipped"

        # Get the raw message data
        raw = json.loads(msg.raw_json) if msg.raw_json else {}

        # Transform message text
        text = transform_message_text(
            raw.get("text", msg.text_preview or ""),
            session,
            channel_names=channel_names,
        )

        # Determine who to impersonate
        impersonate_email = None
        user = session.get(User, msg.slack_user_id) if msg.slack_user_id else None
        if user and user.google_email:
            impersonate_email = user.google_email
        elif user and self.settings.user_mapping.unmapped_action == "attribute":
            # Post as admin with attribution
            user_name = user.slack_real_name or user.slack_display_name or msg.slack_user_id
            text = build_attribution_text(
                text, user_name, slack_ts_to_datetime(msg.slack_ts),
            )
            impersonate_email = self.settings.google.admin_email
        elif self.settings.user_mapping.unmapped_action == "skip":
            msg.migration_status = "skipped"
            msg.skip_reason = "unmapped_user"
            return "skipped"

        if not text and not msg.has_files:
            msg.migration_status = "skipped"
            msg.skip_reason = "empty_message"
            return "skipped"

        # Use message timestamp as thread key for top-level messages
        msg_thread_key = thread_key or msg.slack_ts

        if dry_run:
            logger.debug(f"[DRY RUN] Would post: {text[:100]}...")
            msg.migration_status = "migrated"
            msg.google_thread_key = msg_thread_key
            return "migrated"

        try:
            # Handle file attachments
            file_cards: list[dict] = []
            file_text = ""
            if msg.has_files and not skip_files:
                file_cards, file_text = self._handle_message_files(session, msg, space_name, channel_id)

            msg_text = text or ""
            if not msg_text and not file_cards and not file_text:
                msg_text = "(empty message)"

            msg_kwargs: dict = dict(
                space_name=space_name,
                text=msg_text or "(file attachment)",
                thread_key=msg_thread_key if thread_key else None,
                create_time=slack_ts_to_datetime(msg.slack_ts),
                impersonate_email=impersonate_email,
            )
            if file_cards:
                msg_kwargs["cards"] = file_cards

            try:
                result = self.chat.create_message(**msg_kwargs)
            except Exception as first_err:
                # Fallback 1: if cards failed, retry without cards using text links
                if "cards" in msg_kwargs:
                    logger.debug(f"Cards failed, falling back to text links")
                    del msg_kwargs["cards"]
                    if file_text:
                        msg_kwargs["text"] = f"{msg_text}\n{file_text}" if msg_text else file_text
                    try:
                        result = self.chat.create_message(**msg_kwargs)
                    except Exception:
                        pass  # fall through to impersonation fallback
                    else:
                        # Cards fallback succeeded
                        msg.google_message_name = result.get("name", "")
                        msg.google_thread_key = msg_thread_key
                        msg.migrated_at = now_utc()
                        msg.migration_status = "migrated"
                        return "migrated"

                # Fallback 2: if impersonation fails, retry as admin with attribution
                if impersonate_email and impersonate_email != self.settings.google.admin_email:
                    user_name = ""
                    if user:
                        user_name = user.slack_real_name or user.slack_display_name or msg.slack_user_id
                    logger.debug(f"Impersonation failed for {impersonate_email}, using admin fallback")
                    msg_kwargs.pop("cards", None)
                    msg_kwargs["impersonate_email"] = self.settings.google.admin_email
                    final_text = msg_text
                    if file_text:
                        final_text = f"{msg_text}\n{file_text}" if msg_text else file_text
                    if user_name:
                        final_text = build_attribution_text(
                            final_text, user_name, slack_ts_to_datetime(msg.slack_ts),
                        )
                    msg_kwargs["text"] = final_text
                    result = self.chat.create_message(**msg_kwargs)
                else:
                    raise

            msg.google_message_name = result.get("name", "")
            msg.google_thread_key = msg_thread_key
            msg.migrated_at = now_utc()
            msg.migration_status = "migrated"

            # Handle reactions
            reactions = raw.get("reactions", [])
            if reactions:
                self._migrate_reactions(
                    session, result["name"], reactions, channel_id, msg.slack_ts,
                )

            return "migrated"

        except Exception as e:
            # Treat "Message already exists" as success (resume after interruption)
            err_str = str(e)
            if "already exists" in err_str.lower():
                msg.migration_status = "migrated"
                msg.google_thread_key = msg_thread_key
                msg.migrated_at = now_utc()
                return "migrated"
            logger.error(f"Failed to migrate message {msg.slack_ts}: {e}")
            msg.migration_status = "failed"
            msg.error_message = err_str[:500]
            return "failed"

    def _handle_message_files(
        self, session, msg: Message, space_name: str, channel_id: str,
    ) -> tuple[list[dict], str]:
        """Download and upload files for a message.

        Returns (cards, fallback_text) — caller tries cards first, falls back to text.
        All DB operations happen on the caller's session — no nested sessions.
        """
        from sqlalchemy import select, and_

        files = session.scalars(
            select(File).where(
                and_(
                    File.slack_channel_id == channel_id,
                    File.slack_message_ts == msg.slack_ts,
                )
            )
        ).all()

        # Channel member emails — granted commenter access on any Drive uploads
        commenter_emails = [
            email for (email,) in session.execute(
                select(User.google_email)
                .join(Membership, Membership.slack_user_id == User.slack_user_id)
                .where(Membership.slack_channel_id == channel_id)
                .where(User.google_email.isnot(None))
            ).all()
        ]

        cards = []
        file_lines = []
        for f in files:
            local_path = self.files.download_file(
                f.slack_file_id, f.slack_url_private or "",
                filename=f.filename, size=f.size_bytes,
            )
            if not local_path:
                f.migration_status = "failed"
                f.error_message = "download_failed"
                continue

            f.local_path = str(local_path)
            f.downloaded_at = now_utc()

            fname = f.filename or local_path.name
            if self.settings.google.file_upload_method == "google_drive":
                url = self.files.upload_to_drive(
                    local_path, fname, commenter_emails=commenter_emails,
                )
                if url:
                    f.google_attachment_name = url
                    f.uploaded_at = now_utc()
                    f.migration_status = "uploaded"
                    cards.append(build_file_card(fname, url=url))
                    file_lines.append(f"📎 {fname}: {url}")
                else:
                    f.migration_status = "failed"
                    f.error_message = "drive_upload_failed"
            else:
                att_name = self.files.upload_to_chat(local_path, space_name, fname)
                if att_name:
                    f.google_attachment_name = att_name
                    f.uploaded_at = now_utc()
                    f.migration_status = "uploaded"
                    cards.append(build_file_card(fname))
                    file_lines.append(f"📎 {fname}")
                else:
                    f.migration_status = "failed"
                    f.error_message = "chat_upload_failed"

        return cards, "\n".join(file_lines)

    def _migrate_reactions(
        self,
        session,
        google_message_name: str,
        reactions: list[dict],
        channel_id: str,
        message_ts: str,
    ) -> None:
        """Migrate reactions from a Slack message to Google Chat."""
        for reaction in reactions:
            emoji_name = reaction.get("name", "")
            unicode_emoji = EMOJI_MAP.get(emoji_name)

            if not unicode_emoji:
                # Log as skipped feature
                log_skipped_feature(
                    session, channel_id, "reaction",
                    message_ts=message_ts,
                    detail={"emoji": emoji_name, "count": reaction.get("count", 0)},
                )
                continue

            for user_id in reaction.get("users", []):
                user = session.get(User, user_id)
                if user and user.google_email:
                    try:
                        self.chat.create_reaction(
                            google_message_name,
                            unicode_emoji,
                            impersonate_email=user.google_email,
                        )
                    except Exception:
                        pass  # Best effort for reactions


def complete_stuck_spaces(chat_client: GoogleChatClient, settings: Settings) -> int:
    """Complete import mode on any spaces that are stuck."""
    count = 0
    with get_session() as session:
        channels = get_channels(session)
        for ch in channels:
            if (
                ch.google_space_name
                and ch.migration_status in ("migrating_messages", "creating_space")
                and not ch.import_completed_at
            ):
                try:
                    chat_client.complete_import_space(ch.google_space_name)
                    update_channel_status(
                        session, ch.slack_channel_id, "completed",
                        import_completed_at=now_utc(),
                    )
                    count += 1
                    logger.info(f"Completed stuck import: #{ch.name}")
                except Exception as e:
                    logger.error(f"Failed to complete #{ch.name}: {e}")
    return count

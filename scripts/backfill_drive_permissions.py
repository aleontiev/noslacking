"""Backfill Drive commenter permissions for files in DMs/groupDMs.

Many Drive uploads from earlier migration runs were created without granting
access to the Slack-channel members. This script retroactively grants commenter
access to every member of every DM/group-DM that includes the given user.

Usage:
    uv run python scripts/backfill_drive_permissions.py --user U2P9KC0E8
    uv run python scripts/backfill_drive_permissions.py --user ant@asaak.co --dry-run
"""

from __future__ import annotations

import argparse
import logging
import re
import sys

from googleapiclient.errors import HttpError
from sqlalchemy import and_, select

from noslacking.config import load_config
from noslacking.db.engine import get_session, init_db
from noslacking.db.models import Channel, File, Membership, User
from noslacking.google.auth import get_drive_service
from noslacking.utils.logging import setup_logging
from noslacking.utils.retry import google_retry

logger = logging.getLogger(__name__)

DRIVE_ID_RE = re.compile(r"/d/([A-Za-z0-9_-]+)")


def extract_drive_id(url: str) -> str | None:
    m = DRIVE_ID_RE.search(url or "")
    return m.group(1) if m else None


@google_retry
def _grant(drive, file_id: str, email: str) -> None:
    drive.permissions().create(
        fileId=file_id,
        body={"role": "commenter", "type": "user", "emailAddress": email},
        sendNotificationEmail=False,
        fields="id",
    ).execute()


def resolve_user(session, ident: str) -> User | None:
    if ident.startswith("U") and len(ident) <= 12:
        user = session.get(User, ident)
        if user:
            return user
    return session.scalars(
        select(User).where(
            (User.slack_email == ident) | (User.google_email == ident)
        )
    ).first()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--user", required=True, help="Slack user ID or email")
    parser.add_argument("--config", help="Path to config YAML")
    parser.add_argument("--data-dir", help="Override data directory")
    parser.add_argument("--dry-run", action="store_true", help="Plan only; no API calls")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    settings = load_config(config_path=args.config, data_dir=args.data_dir)
    setup_logging("DEBUG" if args.verbose else settings.log_level, logs_dir=settings.logs_path)
    init_db(settings.db_path)

    admin_email = settings.google.admin_email

    with get_session() as session:
        target = resolve_user(session, args.user)
        if not target:
            print(f"User not found: {args.user}", file=sys.stderr)
            return 1
        print(f"Target: {target.slack_user_id} ({target.google_email or target.slack_email})")

        member_channel_ids = session.scalars(
            select(Membership.slack_channel_id).where(
                Membership.slack_user_id == target.slack_user_id
            )
        ).all()
        channels = session.scalars(
            select(Channel).where(
                and_(
                    Channel.slack_channel_id.in_(member_channel_ids),
                    Channel.channel_type.in_(["im", "mpim"]),
                )
            )
        ).all()

        plan: list[tuple[str, str, list[str]]] = []  # (slack_file_id, drive_id, emails)
        for channel in channels:
            emails = [
                e for (e,) in session.execute(
                    select(User.google_email)
                    .join(Membership, Membership.slack_user_id == User.slack_user_id)
                    .where(Membership.slack_channel_id == channel.slack_channel_id)
                    .where(User.google_email.isnot(None))
                ).all()
            ]
            recipients = sorted({e for e in emails if e and e != admin_email})
            if not recipients:
                continue
            files = session.scalars(
                select(File).where(
                    and_(
                        File.slack_channel_id == channel.slack_channel_id,
                        File.migration_status == "uploaded",
                        File.google_attachment_name.like("https://drive.google.com%"),
                    )
                )
            ).all()
            for f in files:
                drive_id = extract_drive_id(f.google_attachment_name)
                if not drive_id:
                    continue
                plan.append((f.slack_file_id, drive_id, recipients))

    total_grants = sum(len(emails) for *_, emails in plan)
    print(f"Channels: {len(channels)}")
    print(f"Files to update: {len(plan)}")
    print(f"Permission grants: {total_grants}")

    if args.dry_run:
        return 0
    if not plan:
        return 0

    drive = get_drive_service(settings.service_account_key_path, impersonate_email=admin_email)

    granted = skipped = failed = 0
    for i, (slack_file_id, drive_id, emails) in enumerate(plan, 1):
        for email in emails:
            try:
                _grant(drive, drive_id, email)
                granted += 1
            except HttpError as e:
                if e.resp.status in (400, 409) and b"already" in (e.content or b"").lower():
                    skipped += 1
                else:
                    failed += 1
                    logger.warning(f"grant failed drive={drive_id} email={email}: {e}")
            except Exception as e:
                failed += 1
                logger.warning(f"grant failed drive={drive_id} email={email}: {e}")
        if i % 50 == 0 or i == len(plan):
            print(f"  {i}/{len(plan)} files (granted={granted} skipped={skipped} failed={failed})")

    print(f"Done. granted={granted} skipped={skipped} failed={failed}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

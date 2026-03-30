"""CRUD helpers for migration state tracking."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.orm import Session

from noslacking.db.models import (
    Channel,
    File,
    Membership,
    Message,
    MigrationRun,
    SkippedFeature,
    User,
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# --- Migration Runs ---


def create_run(session: Session, run_id: str, command: str) -> MigrationRun:
    run = MigrationRun(id=run_id, command=command, started_at=now_utc(), status="running")
    session.add(run)
    session.flush()
    return run


def complete_run(
    session: Session, run_id: str, status: str = "completed", error: str | None = None,
    stats: dict | None = None,
) -> None:
    run = session.get(MigrationRun, run_id)
    if run:
        run.completed_at = now_utc()
        run.status = status
        run.error_message = error
        if stats:
            run.stats_json = json.dumps(stats)


# --- Channels ---


def upsert_channel(session: Session, **kwargs) -> Channel:
    channel_id = kwargs["slack_channel_id"]
    channel = session.get(Channel, channel_id)
    if channel:
        for k, v in kwargs.items():
            if k != "slack_channel_id":
                setattr(channel, k, v)
    else:
        channel = Channel(**kwargs)
        session.add(channel)
    session.flush()
    return channel


def get_channels(
    session: Session, status: str | None = None, channel_type: str | None = None,
) -> list[Channel]:
    stmt = select(Channel)
    if status:
        stmt = stmt.where(Channel.migration_status == status)
    if channel_type:
        stmt = stmt.where(Channel.channel_type == channel_type)
    return list(session.scalars(stmt).all())


def get_channel(session: Session, channel_id: str) -> Channel | None:
    return session.get(Channel, channel_id)


def update_channel_status(session: Session, channel_id: str, status: str, **kwargs) -> None:
    channel = session.get(Channel, channel_id)
    if channel:
        channel.migration_status = status
        for k, v in kwargs.items():
            setattr(channel, k, v)


# --- Channel Claims (parallel extraction) ---


def claim_channel(
    session: Session,
    channel_id: str,
    worker_id: str,
    stale_timeout_minutes: int = 30,
) -> bool:
    """Atomically claim a channel for extraction. Returns True if claimed."""
    stale_cutoff = now_utc() - timedelta(minutes=stale_timeout_minutes)

    result = session.execute(
        update(Channel)
        .where(
            Channel.slack_channel_id == channel_id,
            or_(
                Channel.migration_status == "pending",
                and_(
                    Channel.migration_status == "extracting",
                    Channel.extract_claimed_at < stale_cutoff,
                ),
            ),
        )
        .values(
            migration_status="extracting",
            extract_worker_id=worker_id,
            extract_claimed_at=now_utc(),
        )
    )
    session.flush()
    return result.rowcount > 0


def release_channel(session: Session, channel_id: str, worker_id: str) -> None:
    """Release a channel claim after extraction completes or fails."""
    channel = session.get(Channel, channel_id)
    if channel and channel.extract_worker_id == worker_id:
        channel.extract_worker_id = None
        channel.extract_claimed_at = None


# --- Users ---


def upsert_user(session: Session, **kwargs) -> User:
    user_id = kwargs["slack_user_id"]
    user = session.get(User, user_id)
    if user:
        for k, v in kwargs.items():
            if k != "slack_user_id":
                setattr(user, k, v)
    else:
        user = User(**kwargs)
        session.add(user)
    session.flush()
    return user


def get_users(session: Session, mapped_only: bool = False) -> list[User]:
    stmt = select(User)
    if mapped_only:
        stmt = stmt.where(User.google_email.isnot(None))
    return list(session.scalars(stmt).all())


def get_unmapped_users(session: Session) -> list[User]:
    return list(
        session.scalars(
            select(User).where(
                User.google_email.is_(None),
                User.is_bot.is_(False),
                User.is_deleted.is_(False),
            )
        ).all()
    )


# --- Messages ---


def upsert_message(session: Session, **kwargs) -> Message:
    channel_id = kwargs["slack_channel_id"]
    ts = kwargs["slack_ts"]
    msg = session.scalars(
        select(Message).where(
            Message.slack_channel_id == channel_id, Message.slack_ts == ts
        )
    ).first()
    if msg:
        for k, v in kwargs.items():
            if k not in ("slack_channel_id", "slack_ts"):
                setattr(msg, k, v)
    else:
        msg = Message(**kwargs)
        session.add(msg)
    session.flush()
    return msg


def get_pending_messages(session: Session, channel_id: str, limit: int = 500) -> list[Message]:
    return list(
        session.scalars(
            select(Message)
            .where(
                Message.slack_channel_id == channel_id,
                Message.migration_status == "pending",
                Message.slack_thread_ts.is_(None),  # top-level first
            )
            .order_by(Message.slack_ts)
            .limit(limit)
        ).all()
    )


def get_pending_thread_messages(
    session: Session, channel_id: str, thread_ts: str,
) -> list[Message]:
    return list(
        session.scalars(
            select(Message)
            .where(
                Message.slack_channel_id == channel_id,
                Message.slack_thread_ts == thread_ts,
                Message.slack_ts != thread_ts,  # exclude parent
                Message.migration_status == "pending",
            )
            .order_by(Message.slack_ts)
        ).all()
    )


def get_message_stats(session: Session, channel_id: str | None = None) -> dict:
    """Get message count by status."""
    stmt = select(Message.migration_status, func.count(Message.id))
    if channel_id:
        stmt = stmt.where(Message.slack_channel_id == channel_id)
    stmt = stmt.group_by(Message.migration_status)
    rows = session.execute(stmt).all()
    return {status: count for status, count in rows}


# --- Files ---


def upsert_file(session: Session, **kwargs) -> File:
    file_id = kwargs["slack_file_id"]
    f = session.get(File, file_id)
    if f:
        for k, v in kwargs.items():
            if k != "slack_file_id":
                setattr(f, k, v)
    else:
        f = File(**kwargs)
        session.add(f)
    session.flush()
    return f


def get_pending_files(session: Session, channel_id: str) -> list[File]:
    return list(
        session.scalars(
            select(File).where(
                File.slack_channel_id == channel_id,
                File.migration_status == "pending",
            )
        ).all()
    )


# --- Memberships ---


def upsert_membership(session: Session, **kwargs) -> Membership:
    channel_id = kwargs["slack_channel_id"]
    user_id = kwargs["slack_user_id"]
    mem = session.scalars(
        select(Membership).where(
            Membership.slack_channel_id == channel_id,
            Membership.slack_user_id == user_id,
        )
    ).first()
    if mem:
        for k, v in kwargs.items():
            if k not in ("slack_channel_id", "slack_user_id"):
                setattr(mem, k, v)
    else:
        mem = Membership(**kwargs)
        session.add(mem)
    session.flush()
    return mem


def get_pending_memberships(session: Session, channel_id: str) -> list[Membership]:
    return list(
        session.scalars(
            select(Membership).where(
                Membership.slack_channel_id == channel_id,
                Membership.migration_status == "pending",
            )
        ).all()
    )


# --- Skipped Features ---


def log_skipped_feature(
    session: Session, channel_id: str, feature_type: str,
    message_ts: str | None = None, detail: dict | None = None,
) -> None:
    session.add(
        SkippedFeature(
            slack_channel_id=channel_id,
            slack_message_ts=message_ts,
            feature_type=feature_type,
            detail_json=json.dumps(detail) if detail else None,
            logged_at=now_utc(),
        )
    )

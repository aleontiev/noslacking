"""SQLAlchemy ORM models for migration state tracking."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class MigrationRun(Base):
    __tablename__ = "migration_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    command: Mapped[str] = mapped_column(String(50), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")
    error_message: Mapped[str | None] = mapped_column(Text)
    stats_json: Mapped[str | None] = mapped_column(Text)


class Channel(Base):
    __tablename__ = "channels"

    slack_channel_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    channel_type: Mapped[str] = mapped_column(String(20), nullable=False)
    is_archived: Mapped[bool] = mapped_column(Boolean, default=False)
    member_count: Mapped[int | None] = mapped_column(Integer)
    message_count: Mapped[int | None] = mapped_column(Integer)
    topic: Mapped[str | None] = mapped_column(Text)
    purpose: Mapped[str | None] = mapped_column(Text)

    # Extraction state
    extracted_at: Mapped[datetime | None] = mapped_column(DateTime)
    extract_cursor: Mapped[str | None] = mapped_column(Text)

    # Google Chat state
    google_space_name: Mapped[str | None] = mapped_column(String(255))
    google_space_created_at: Mapped[datetime | None] = mapped_column(DateTime)
    import_completed_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Parallel extraction claim
    extract_worker_id: Mapped[str | None] = mapped_column(String(36))
    extract_claimed_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Overall status
    migration_status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="pending"
    )
    last_sync_ts: Mapped[str | None] = mapped_column(String(20))


class User(Base):
    __tablename__ = "users"

    slack_user_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    slack_email: Mapped[str | None] = mapped_column(String(255))
    slack_display_name: Mapped[str | None] = mapped_column(String(255))
    slack_real_name: Mapped[str | None] = mapped_column(String(255))
    is_bot: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)

    # Google mapping
    google_email: Mapped[str | None] = mapped_column(String(255))
    google_user_id: Mapped[str | None] = mapped_column(String(255))
    mapping_method: Mapped[str | None] = mapped_column(String(20))
    mapped_at: Mapped[datetime | None] = mapped_column(DateTime)


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("slack_channel_id", "slack_ts", name="uq_channel_ts"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    slack_channel_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    slack_ts: Mapped[str] = mapped_column(String(20), nullable=False)
    slack_thread_ts: Mapped[str | None] = mapped_column(String(20))
    slack_user_id: Mapped[str | None] = mapped_column(String(20))
    message_type: Mapped[str | None] = mapped_column(String(30))
    text_preview: Mapped[str | None] = mapped_column(Text)
    has_files: Mapped[bool] = mapped_column(Boolean, default=False)
    has_reactions: Mapped[bool] = mapped_column(Boolean, default=False)
    raw_json: Mapped[str | None] = mapped_column(Text)

    # Google Chat state
    google_message_name: Mapped[str | None] = mapped_column(String(255))
    google_thread_key: Mapped[str | None] = mapped_column(String(255))
    migrated_at: Mapped[datetime | None] = mapped_column(DateTime)
    migration_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )
    skip_reason: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)


class File(Base):
    __tablename__ = "files"

    slack_file_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    slack_channel_id: Mapped[str] = mapped_column(String(20), nullable=False)
    slack_message_ts: Mapped[str] = mapped_column(String(20), nullable=False)
    filename: Mapped[str | None] = mapped_column(String(255))
    mimetype: Mapped[str | None] = mapped_column(String(100))
    size_bytes: Mapped[int | None] = mapped_column(Integer)
    slack_url_private: Mapped[str | None] = mapped_column(Text)
    local_path: Mapped[str | None] = mapped_column(Text)
    google_attachment_name: Mapped[str | None] = mapped_column(String(255))
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime)
    uploaded_at: Mapped[datetime | None] = mapped_column(DateTime)
    migration_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )
    error_message: Mapped[str | None] = mapped_column(Text)


class Membership(Base):
    __tablename__ = "memberships"
    __table_args__ = (
        UniqueConstraint("slack_channel_id", "slack_user_id", name="uq_channel_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    slack_channel_id: Mapped[str] = mapped_column(String(20), nullable=False)
    slack_user_id: Mapped[str] = mapped_column(String(20), nullable=False)
    google_space_name: Mapped[str | None] = mapped_column(String(255))
    google_member_name: Mapped[str | None] = mapped_column(String(255))
    migrated_at: Mapped[datetime | None] = mapped_column(DateTime)
    migration_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )


class SkippedFeature(Base):
    __tablename__ = "skipped_features"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    slack_channel_id: Mapped[str] = mapped_column(String(20), nullable=False)
    slack_message_ts: Mapped[str | None] = mapped_column(String(20))
    feature_type: Mapped[str] = mapped_column(String(50), nullable=False)
    detail_json: Mapped[str | None] = mapped_column(Text)
    logged_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

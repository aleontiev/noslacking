"""SQLite engine and session management."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from noslacking.db.models import Base

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def init_db(db_path: Path) -> Engine:
    """Initialize the database engine and create all tables."""
    global _engine, _session_factory

    db_path.parent.mkdir(parents=True, exist_ok=True)
    _engine = create_engine(
        f"sqlite:///{db_path}", echo=False,
        connect_args={"timeout": 30},  # Wait up to 30s for DB lock
    )

    # Enable WAL mode + tuning for concurrent multi-process access
    @event.listens_for(_engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

    Base.metadata.create_all(_engine)
    _migrate_schema(_engine)
    _session_factory = sessionmaker(bind=_engine)
    return _engine


def _migrate_schema(engine: Engine) -> None:
    """Add columns that may not exist in older databases."""
    import sqlite3
    with engine.connect() as conn:
        raw = conn.connection.dbapi_connection
        cursor = raw.cursor()
        for col, coltype in [
            ("extract_worker_id", "VARCHAR(36)"),
            ("extract_claimed_at", "DATETIME"),
        ]:
            try:
                cursor.execute(f"ALTER TABLE channels ADD COLUMN {col} {coltype}")
            except sqlite3.OperationalError:
                pass  # Column already exists
        raw.commit()


def get_engine() -> Engine:
    if _engine is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _engine


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Provide a transactional session scope."""
    if _session_factory is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    session = _session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

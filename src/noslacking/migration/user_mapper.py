"""Map Slack users to Google Workspace users."""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from rich.table import Table

from noslacking.config import Settings
from noslacking.db.engine import get_session
from noslacking.db.models import User
from noslacking.db.operations import get_unmapped_users, get_users, upsert_user, now_utc
from noslacking.google.admin_client import GoogleAdminClient
from noslacking.utils.logging import console

logger = logging.getLogger(__name__)


class UserMapper:
    """Maps Slack users to Google Workspace users by email."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._google_users: dict[str, dict] = {}  # email -> user data

    def load_google_users(self) -> int:
        """Fetch all Google Workspace users and index by email."""
        admin = GoogleAdminClient(
            self.settings.service_account_key_path,
            self.settings.google.admin_email,
        )
        count = 0
        for user in admin.list_users(domain=self.settings.google.domain):
            self._google_users[user.primary_email.lower()] = {
                "email": user.primary_email,
                "id": user.id,
                "name": user.full_name,
                "suspended": user.is_suspended,
            }
            count += 1
        logger.info(f"Loaded {count} Google Workspace users")
        return count

    def map_all(self) -> dict:
        """Run mapping for all Slack users. Returns stats."""
        stats = {"matched": 0, "unmatched": 0, "bots": 0, "deleted": 0, "overridden": 0}

        overrides = self.settings.user_mapping.overrides

        with get_session() as session:
            users = get_users(session)
            for user in users:
                if user.is_bot:
                    stats["bots"] += 1
                    continue
                if user.is_deleted:
                    stats["deleted"] += 1
                    continue

                # Check manual override first
                if user.slack_user_id in overrides:
                    google_email = overrides[user.slack_user_id]
                    google_data = self._google_users.get(google_email.lower(), {})
                    user.google_email = google_email
                    user.google_user_id = google_data.get("id", "")
                    user.mapping_method = "manual_override"
                    user.mapped_at = now_utc()
                    stats["overridden"] += 1
                    continue

                # Try email match
                if user.slack_email:
                    email_lower = user.slack_email.lower()
                    if email_lower in self._google_users:
                        gdata = self._google_users[email_lower]
                        user.google_email = gdata["email"]
                        user.google_user_id = gdata.get("id", "")
                        user.mapping_method = "auto_email"
                        user.mapped_at = now_utc()
                        stats["matched"] += 1
                        continue

                user.mapping_method = "unmatched"
                stats["unmatched"] += 1

        return stats

    def print_mapping_table(self) -> None:
        """Print a Rich table of the current user mapping."""
        table = Table(title="User Mapping")
        table.add_column("Slack Name", style="cyan")
        table.add_column("Slack Email", style="dim")
        table.add_column("Google Email", style="green")
        table.add_column("Status", style="bold")

        with get_session() as session:
            users = get_users(session)
            for user in sorted(users, key=lambda u: u.slack_real_name or ""):
                if user.is_bot:
                    continue

                status = user.mapping_method or "pending"
                style = {
                    "auto_email": "green",
                    "manual_override": "yellow",
                    "unmatched": "red",
                    "pending": "dim",
                }.get(status, "dim")

                table.add_row(
                    user.slack_real_name or user.slack_display_name or user.slack_user_id,
                    user.slack_email or "",
                    user.google_email or "—",
                    f"[{style}]{status}[/{style}]",
                )

        console.print(table)

    def export_csv(self, path: Path) -> None:
        """Export current mapping to CSV for manual review/editing."""
        with get_session() as session:
            users = get_users(session)

        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "slack_user_id", "slack_name", "slack_email",
                "google_email", "mapping_method",
            ])
            for user in users:
                if not user.is_bot:
                    writer.writerow([
                        user.slack_user_id,
                        user.slack_real_name or user.slack_display_name,
                        user.slack_email or "",
                        user.google_email or "",
                        user.mapping_method or "",
                    ])
        logger.info(f"Exported mapping to {path}")

    def import_csv(self, path: Path) -> int:
        """Import user mapping from CSV. Returns count of updated mappings."""
        count = 0
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            with get_session() as session:
                for row in reader:
                    if row.get("google_email"):
                        upsert_user(
                            session,
                            slack_user_id=row["slack_user_id"],
                            google_email=row["google_email"],
                            mapping_method="csv_import",
                            mapped_at=now_utc(),
                        )
                        count += 1
        logger.info(f"Imported {count} user mappings from {path}")
        return count

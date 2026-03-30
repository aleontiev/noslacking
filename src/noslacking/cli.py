"""CLI entry point — all subcommands."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.panel import Panel
from rich.table import Table

from noslacking.utils.logging import console

app = typer.Typer(
    name="noslacking",
    help="Migrate a Slack workspace to Google Chat using APIs.",
    rich_markup_mode="rich",
)

# --- Global options ---

ConfigOption = Annotated[
    Optional[str],
    typer.Option("--config", "-c", help="Path to config YAML"),
]
DataDirOption = Annotated[
    Optional[str],
    typer.Option("--data-dir", help="Override data directory"),
]
VerboseOption = Annotated[
    bool,
    typer.Option("--verbose", "-v", help="Enable debug logging"),
]


def _init(config: str | None, data_dir: str | None, verbose: bool) -> "Settings":
    """Common init: load config, setup logging, init DB."""
    from noslacking.config import load_config
    from noslacking.db.engine import init_db
    from noslacking.utils.logging import setup_logging

    settings = load_config(config_path=config, data_dir=data_dir)
    log_level = "DEBUG" if verbose else settings.log_level
    setup_logging(log_level, logs_dir=settings.logs_path)
    init_db(settings.db_path)
    return settings


# ─── setup ────────────────────────────────────────────────────────────────────


@app.command()
def setup(
    config: ConfigOption = None,
    data_dir: DataDirOption = None,
    verbose: VerboseOption = False,
    reset: bool = typer.Option(False, help="Wipe existing config and re-run"),
):
    """Interactive setup wizard for first-time configuration."""
    from noslacking.config import Settings, write_config
    from noslacking.db.engine import init_db
    from noslacking.utils.logging import setup_logging

    setup_logging("DEBUG" if verbose else "INFO")

    console.print(Panel("[bold]Slack → Google Chat Migration Setup[/bold]", style="blue"))

    # Determine data dir
    data_directory = data_dir or "~/.noslacking"
    data_path = Path(data_directory).expanduser()
    data_path.mkdir(parents=True, exist_ok=True)

    config_path = Path(config or f"{data_directory}/config.yaml").expanduser()

    if config_path.exists() and not reset:
        console.print(f"[yellow]Config already exists at {config_path}[/yellow]")
        if not typer.confirm("Overwrite?"):
            raise typer.Exit()

    # Collect Slack tokens
    console.print("\n[bold cyan]Step 1: Slack Configuration[/bold cyan]")
    console.print("You need a Slack app with bot and user tokens.")
    console.print("Required bot scopes: channels:read, channels:history, groups:read, "
                   "groups:history, users:read, users:read.email, files:read, reactions:read")

    bot_token = typer.prompt("Slack bot token (xoxb-...)")
    user_token = typer.prompt("Slack user token (xoxp-..., optional)", default="")

    # Validate Slack token
    try:
        from noslacking.slack.client import SlackClient
        slack = SlackClient(bot_token, user_token or None)
        auth = slack.test_auth()
        console.print(f"  [green]✓[/green] Connected to workspace: [bold]{auth.get('team', '')}[/bold]")
    except Exception as e:
        console.print(f"  [red]✗[/red] Slack auth failed: {e}")
        raise typer.Exit(1)

    # Google configuration
    console.print("\n[bold cyan]Step 2: Google Configuration[/bold cyan]")
    console.print("You need a GCP service account with domain-wide delegation.")
    console.print("Required APIs: Google Chat API, Admin SDK API")
    console.print("Required scopes: chat.import, chat.spaces, admin.directory.user.readonly")

    sa_key_path = typer.prompt(
        "Path to service account JSON key",
        default=str(data_path / "service-account.json"),
    )
    domain = typer.prompt("Google Workspace domain (e.g., company.com)")
    admin_email = typer.prompt(f"Workspace admin email (e.g., admin@{domain})")

    # Validate Google credentials
    try:
        from noslacking.google.auth import validate_credentials
        results = validate_credentials(Path(sa_key_path).expanduser(), admin_email)
        for api, ok in results.items():
            status = "[green]✓[/green]" if ok else "[red]✗[/red]"
            console.print(f"  {status} {api}")
        if not all(results.values()):
            console.print("[yellow]Some Google APIs failed validation. Check your setup.[/yellow]")
    except Exception as e:
        console.print(f"  [red]✗[/red] Google auth failed: {e}")
        console.print("[yellow]Continuing anyway — you can fix this later.[/yellow]")

    # Write config
    settings = Settings(
        data_dir=data_directory,
        config_path=str(config_path),
        slack_bot_token=bot_token,
        slack_user_token=user_token,
    )
    settings.google.service_account_key = sa_key_path
    settings.google.domain = domain
    settings.google.admin_email = admin_email

    write_config(settings, config_path)

    # Write .env for tokens
    env_path = data_path / ".env"
    env_path.write_text(
        f"SLACK_BOT_TOKEN={bot_token}\n"
        f"SLACK_USER_TOKEN={user_token}\n"
    )

    # Init DB
    init_db(data_path / "migration.db")

    console.print(f"\n[green]✓ Config written to {config_path}[/green]")
    console.print(f"[green]✓ Tokens written to {env_path}[/green]")
    console.print(f"[green]✓ Database initialized at {data_path / 'migration.db'}[/green]")
    console.print("\n[bold]Next steps:[/bold]")
    console.print("  1. noslacking extract     # Pull data from Slack")
    console.print("  2. noslacking map-users    # Map Slack → Google users")
    console.print("  3. noslacking validate     # Pre-flight checks")
    console.print("  4. noslacking migrate      # Execute migration")


# ─── extract ──────────────────────────────────────────────────────────────────


@app.command()
def extract(
    config: ConfigOption = None,
    data_dir: DataDirOption = None,
    verbose: VerboseOption = False,
    channels: str = typer.Option("", help="Comma-separated channel names to extract"),
    channel_types: str = typer.Option("", help="Comma-separated types: public_channel,private_channel,im,mpim"),
    since: str = typer.Option("", help="Only messages newer than this (ISO 8601)"),
    skip_files: bool = typer.Option(False, help="Skip file metadata extraction"),
    skip_threads: bool = typer.Option(False, help="Skip thread replies"),
    resume: bool = typer.Option(True, help="Resume from last position"),
    force: bool = typer.Option(False, help="Clear stale locks from crashed workers"),
):
    """Extract all Slack data via API into local cache.

    Supports parallel execution — run multiple processes with different --channel-types
    to extract DMs and public channels simultaneously.
    """
    settings = _init(config, data_dir, verbose)

    if not settings.slack_bot_token:
        console.print("[red]SLACK_BOT_TOKEN not set. Run 'noslacking setup' first.[/red]")
        raise typer.Exit(1)

    from noslacking.slack.client import SlackClient
    from noslacking.slack.extractor import SlackExtractor

    client = SlackClient(settings.slack_bot_token, settings.slack_user_token or None)

    run_id = str(uuid.uuid4())
    extractor = SlackExtractor(client, settings, worker_id=run_id)

    channel_filter = [c.strip() for c in channels.split(",") if c.strip()] or None
    type_filter = [t.strip() for t in channel_types.split(",") if t.strip()] or None

    from noslacking.db.engine import get_session
    from noslacking.db.operations import create_run, complete_run

    with get_session() as session:
        create_run(session, run_id, "extract")

    try:
        stats = extractor.extract_all(
            channel_filter=channel_filter,
            channel_types=type_filter,
            since=since or None,
            skip_files=skip_files,
            skip_threads=skip_threads,
            resume=resume,
            force=force,
        )

        with get_session() as session:
            complete_run(session, run_id, "completed", stats=stats)

        # Print summary
        table = Table(title="Extraction Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        for k, v in stats.items():
            table.add_row(k.replace("_", " ").title(), str(v))
        console.print(table)

    except Exception as e:
        with get_session() as session:
            complete_run(session, run_id, "failed", error=str(e))
        console.print(f"[red]Extraction failed: {e}[/red]")
        raise typer.Exit(1)


# ─── map-users ────────────────────────────────────────────────────────────────


@app.command("map-users")
def map_users(
    config: ConfigOption = None,
    data_dir: DataDirOption = None,
    verbose: VerboseOption = False,
    export: str = typer.Option("", help="Export mapping to CSV path"),
    import_csv: str = typer.Option("", "--import", help="Import mapping from CSV path"),
):
    """Map Slack users to Google Workspace users by email."""
    settings = _init(config, data_dir, verbose)

    from noslacking.migration.user_mapper import UserMapper

    mapper = UserMapper(settings)

    if import_csv:
        count = mapper.import_csv(Path(import_csv))
        console.print(f"[green]Imported {count} user mappings[/green]")
    else:
        console.print("Loading Google Workspace users...")
        mapper.load_google_users()
        stats = mapper.map_all()

        table = Table(title="User Mapping Results")
        table.add_column("Status", style="cyan")
        table.add_column("Count", justify="right", style="green")
        for k, v in stats.items():
            table.add_row(k.replace("_", " ").title(), str(v))
        console.print(table)

    mapper.print_mapping_table()

    if export:
        mapper.export_csv(Path(export))
        console.print(f"[green]Exported to {export}[/green]")


# ─── validate ─────────────────────────────────────────────────────────────────


@app.command()
def validate(
    config: ConfigOption = None,
    data_dir: DataDirOption = None,
    verbose: VerboseOption = False,
    strict: bool = typer.Option(False, help="Treat warnings as errors"),
):
    """Pre-flight validation of credentials, data, and mappings."""
    settings = _init(config, data_dir, verbose)
    warnings = 0
    errors = 0

    console.print(Panel("[bold]Pre-flight Validation[/bold]", style="blue"))

    # Check Slack auth
    console.print("\n[bold]Slack API[/bold]")
    try:
        from noslacking.slack.client import SlackClient
        client = SlackClient(settings.slack_bot_token, settings.slack_user_token or None)
        auth = client.test_auth()
        console.print(f"  [green]✓[/green] Bot authenticated: {auth.get('user', '')} @ {auth.get('team', '')}")
    except Exception as e:
        console.print(f"  [red]✗[/red] Slack bot auth failed: {e}")
        errors += 1

    # Check Google auth
    console.print("\n[bold]Google APIs[/bold]")
    try:
        from noslacking.google.auth import validate_credentials
        results = validate_credentials(
            settings.service_account_key_path, settings.google.admin_email,
        )
        for api, ok in results.items():
            if ok:
                console.print(f"  [green]✓[/green] {api}")
            else:
                console.print(f"  [red]✗[/red] {api}")
                errors += 1
    except Exception as e:
        console.print(f"  [red]✗[/red] Google auth error: {e}")
        errors += 1

    # Check extraction data
    console.print("\n[bold]Extracted Data[/bold]")
    from noslacking.db.engine import get_session
    from noslacking.db.operations import get_channels, get_users, get_unmapped_users, get_message_stats

    with get_session() as session:
        channels = get_channels(session)
        extracted = [c for c in channels if c.extracted_at]
        users = get_users(session)
        unmapped = get_unmapped_users(session)
        msg_stats = get_message_stats(session)

    total_msgs = sum(msg_stats.values())
    console.print(f"  Channels: {len(channels)} total, {len(extracted)} extracted")
    console.print(f"  Messages: {total_msgs}")
    console.print(f"  Users: {len(users)} total")

    if not channels:
        console.print("  [yellow]⚠ No channels extracted yet. Run 'extract' first.[/yellow]")
        warnings += 1

    if unmapped:
        console.print(f"  [yellow]⚠ {len(unmapped)} users not mapped to Google accounts[/yellow]")
        warnings += 1

    # Summary
    console.print()
    if errors:
        console.print(f"[red]✗ {errors} error(s) found[/red]")
    if warnings:
        console.print(f"[yellow]⚠ {warnings} warning(s)[/yellow]")
    if not errors and not warnings:
        console.print("[green]✓ All checks passed![/green]")

    if errors or (strict and warnings):
        raise typer.Exit(1)


# ─── migrate ──────────────────────────────────────────────────────────────────


@app.command()
def migrate(
    config: ConfigOption = None,
    data_dir: DataDirOption = None,
    verbose: VerboseOption = False,
    channels: str = typer.Option("", help="Comma-separated channel names"),
    dry_run: bool = typer.Option(False, help="Log actions without executing"),
    resume: bool = typer.Option(True, help="Resume from last position"),
    skip_files: bool = typer.Option(False, help="Skip file migration"),
    skip_members: bool = typer.Option(False, help="Skip member addition"),
    complete_import: bool = typer.Option(True, "--complete-import/--no-complete-import",
                                          help="Call completeImport after migration"),
    complete_only: bool = typer.Option(False, "--complete", help="Only complete stuck imports"),
    max_channels: int = typer.Option(0, help="Limit channels to migrate (0 = all)"),
):
    """Execute the migration to Google Chat."""
    settings = _init(config, data_dir, verbose)

    from noslacking.google.chat_client import GoogleChatClient
    from noslacking.migration.executor import MigrationExecutor, complete_stuck_spaces
    from noslacking.migration.file_handler import FileHandler
    from noslacking.slack.client import SlackClient

    chat = GoogleChatClient(
        settings.service_account_key_path,
        settings.google.admin_email,
        messages_per_second=settings.google.messages_per_second,
    )

    # Complete-only mode
    if complete_only:
        count = complete_stuck_spaces(chat, settings)
        console.print(f"[green]Completed {count} stuck space(s)[/green]")
        raise typer.Exit()

    slack = SlackClient(settings.slack_bot_token, settings.slack_user_token or None)
    files = FileHandler(slack, chat, settings)

    executor = MigrationExecutor(slack, chat, files, settings)

    channel_filter = [c.strip() for c in channels.split(",") if c.strip()] or None

    run_id = str(uuid.uuid4())
    from noslacking.db.engine import get_session
    from noslacking.db.operations import create_run, complete_run

    with get_session() as session:
        create_run(session, run_id, "migrate")

    try:
        stats = executor.migrate_all(
            channel_filter=channel_filter,
            dry_run=dry_run,
            resume=resume,
            skip_files=skip_files,
            skip_members=skip_members,
            complete_import=complete_import,
            max_channels=max_channels or None,
        )

        with get_session() as session:
            complete_run(session, run_id, "completed", stats=stats)

        # Print summary
        table = Table(title="Migration Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        for k, v in stats.items():
            table.add_row(k.replace("_", " ").title(), str(v))
        console.print(table)

    except Exception as e:
        with get_session() as session:
            complete_run(session, run_id, "failed", error=str(e))
        console.print(f"[red]Migration failed: {e}[/red]")
        raise typer.Exit(1)


# ─── status ───────────────────────────────────────────────────────────────────


@app.command()
def status(
    config: ConfigOption = None,
    data_dir: DataDirOption = None,
    verbose: VerboseOption = False,
    detail: bool = typer.Option(False, help="Show per-channel breakdown"),
    errors_only: bool = typer.Option(False, "--errors", help="Show only errors"),
):
    """Show migration progress and status."""
    settings = _init(config, data_dir, verbose)

    from noslacking.db.engine import get_session
    from noslacking.db.operations import get_channels, get_message_stats, get_users

    with get_session() as session:
        channels_raw = get_channels(session)
        users = get_users(session)
        msg_stats = get_message_stats(session)

        # Build DM display names: resolve counterparty names for im/mpim
        from noslacking.db.models import Membership, User
        admin_email = settings.google.admin_email
        admin_user = session.query(User).filter(User.slack_email == admin_email).first()
        admin_id = admin_user.slack_user_id if admin_user else None

        dm_labels: dict[str, str] = {}
        for ch in channels_raw:
            if ch.channel_type in ("im", "mpim"):
                members = session.query(Membership).filter(
                    Membership.slack_channel_id == ch.slack_channel_id
                ).all()
                others = []
                for m in members:
                    if m.slack_user_id == admin_id:
                        continue
                    u = session.get(User, m.slack_user_id)
                    if u and u.slack_real_name:
                        others.append(u.slack_real_name.split()[0])
                    elif u and u.slack_display_name:
                        others.append(u.slack_display_name)
                    else:
                        others.append(m.slack_user_id)
                if others:
                    dm_labels[ch.slack_channel_id] = " + ".join(others)

        # Materialize attributes while session is open
        channels = [
            {
                "name": ch.name,
                "channel_type": ch.channel_type,
                "channel_id": ch.slack_channel_id,
                "message_count": ch.message_count,
                "migration_status": ch.migration_status,
                "google_space_name": ch.google_space_name,
                "dm_label": dm_labels.get(ch.slack_channel_id),
            }
            for ch in channels_raw
        ]
        mapped_count = sum(1 for u in users if u.google_email)
        user_count = len(users)

    # Aggregate channel stats
    status_counts: dict[str, int] = {}
    for ch in channels:
        status_counts[ch["migration_status"]] = status_counts.get(ch["migration_status"], 0) + 1

    # Overall summary
    console.print(Panel("[bold]Migration Status[/bold]", style="blue"))

    summary = Table()
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value", justify="right", style="green")
    summary.add_row("Total Channels", str(len(channels)))
    for s, c in sorted(status_counts.items()):
        summary.add_row(f"  {s}", str(c))
    summary.add_row("Total Users", str(user_count))
    summary.add_row("  Mapped", str(mapped_count))
    for s, c in sorted(msg_stats.items()):
        summary.add_row(f"Messages ({s})", str(c))
    console.print(summary)

    # Per-channel detail
    if detail or errors_only:
        table = Table(title="Channel Details", expand=True)
        table.add_column("Channel", style="cyan", no_wrap=True, ratio=3)
        table.add_column("Type", style="dim", ratio=1)
        table.add_column("Messages", justify="right", ratio=1)
        table.add_column("Status", style="bold", ratio=1)
        table.add_column("Google Space", style="dim", ratio=2)

        type_order = {"im": 0, "mpim": 1, "private_channel": 2, "public_channel": 3}
        for ch in sorted(channels, key=lambda c: (type_order.get(c["channel_type"], 9), c["name"])):
            if errors_only and ch["migration_status"] != "failed":
                continue

            style = {
                "completed": "green",
                "failed": "red",
                "pending": "dim",
                "extracted": "yellow",
                "extracting": "blue",
                "migrating_messages": "blue",
            }.get(ch["migration_status"], "")

            if ch.get("dm_label"):
                display = f"#{ch['channel_id']} ({ch['dm_label']})"
            else:
                display = f"#{ch['name']}"

            table.add_row(
                display,
                ch["channel_type"],
                str(ch["message_count"] or 0),
                f"[{style}]{ch['migration_status']}[/{style}]" if style else ch["migration_status"],
                ch["google_space_name"] or "—",
            )

        console.print(table)


# ─── sync ─────────────────────────────────────────────────────────────────────


@app.command()
def sync(
    config: ConfigOption = None,
    data_dir: DataDirOption = None,
    verbose: VerboseOption = False,
    channels: str = typer.Option("", help="Comma-separated channel names"),
    since: str = typer.Option("", help="Override sync start time (ISO 8601)"),
    dry_run: bool = typer.Option(False, help="Log without executing"),
):
    """Incremental sync — fetch new messages since last migration."""
    settings = _init(config, data_dir, verbose)

    from noslacking.db.engine import get_session
    from noslacking.db.operations import get_channels, upsert_message, now_utc, update_channel_status
    from noslacking.google.chat_client import GoogleChatClient
    from noslacking.migration.message_transform import transform_message_text, slack_ts_to_datetime
    from noslacking.slack.client import SlackClient
    from noslacking.db.models import User
    import json

    slack = SlackClient(settings.slack_bot_token, settings.slack_user_token or None)
    chat = GoogleChatClient(
        settings.service_account_key_path,
        settings.google.admin_email,
        messages_per_second=settings.google.messages_per_second,
    )

    channel_filter = [c.strip() for c in channels.split(",") if c.strip()] or None
    new_messages = 0

    # Load channel data within session, extract what we need
    sync_targets: list[dict] = []
    with get_session() as session:
        completed_channels = get_channels(session, status="completed")
        if channel_filter:
            completed_channels = [c for c in completed_channels if c.name in channel_filter]
        for ch in completed_channels:
            sync_targets.append({
                "channel_id": ch.slack_channel_id,
                "name": ch.name,
                "space_name": ch.google_space_name,
                "last_sync_ts": ch.last_sync_ts,
            })

    for ch in sync_targets:
        oldest = since or ch["last_sync_ts"]
        if not oldest:
            console.print(f"[yellow]#{ch['name']}: no sync timestamp, skipping[/yellow]")
            continue

        console.print(f"Syncing #{ch['name']} since {oldest}...")
        latest_ts = oldest

        for msg in slack.get_history(ch["channel_id"], oldest=oldest):
            new_messages += 1
            latest_ts = max(latest_ts, msg.ts)

            if dry_run:
                console.print(f"  [dim][DRY RUN] New message: {msg.text[:80]}...[/dim]")
                continue

            # Post to Google Chat
            with get_session() as session:
                text = transform_message_text(msg.raw.get("text", ""), session)
                user = session.get(User, msg.user) if msg.user else None
                impersonate = user.google_email if user and user.google_email else settings.google.admin_email

            try:
                chat.create_message(
                    space_name=ch["space_name"],
                    text=text or "(empty message)",
                    impersonate_email=impersonate,
                )
            except Exception as e:
                console.print(f"  [red]Failed: {e}[/red]")

        # Update sync timestamp
        if not dry_run and latest_ts != oldest:
            with get_session() as session:
                update_channel_status(session, ch["channel_id"], "completed", last_sync_ts=latest_ts)

    console.print(f"\n[green]Sync complete. {new_messages} new message(s).[/green]")


# ─── run (extract + migrate) ─────────────────────────────────────────────────


@app.command()
def run(
    config: ConfigOption = None,
    data_dir: DataDirOption = None,
    verbose: VerboseOption = False,
    channels: str = typer.Option(..., help="Comma-separated channel names/IDs"),
    skip_files: bool = typer.Option(False, help="Skip file migration"),
    force: bool = typer.Option(False, help="Clear stale locks from crashed workers"),
    dry_run: bool = typer.Option(False, help="Log without executing migration"),
):
    """Extract and migrate channels in one shot."""
    settings = _init(config, data_dir, verbose)

    if not settings.slack_bot_token:
        console.print("[red]SLACK_BOT_TOKEN not set. Run 'noslacking setup' first.[/red]")
        raise typer.Exit(1)

    from noslacking.db.engine import get_session
    from noslacking.db.operations import create_run, complete_run
    from noslacking.google.chat_client import GoogleChatClient
    from noslacking.migration.executor import MigrationExecutor
    from noslacking.migration.file_handler import FileHandler
    from noslacking.slack.client import SlackClient
    from noslacking.slack.extractor import SlackExtractor

    channel_filter = [c.strip() for c in channels.split(",") if c.strip()]
    if not channel_filter:
        console.print("[red]No channels specified.[/red]")
        raise typer.Exit(1)

    client = SlackClient(settings.slack_bot_token, settings.slack_user_token or None)

    # Step 1: Extract
    console.print("[bold blue]Step 1: Extract[/bold blue]")
    run_id = str(uuid.uuid4())
    extractor = SlackExtractor(client, settings, worker_id=run_id)

    with get_session() as session:
        create_run(session, run_id, "run-extract")

    try:
        extract_stats = extractor.extract_all(
            channel_filter=channel_filter,
            since=None,
            skip_files=skip_files,
            skip_threads=False,
            resume=True,
            force=force,
        )
        with get_session() as session:
            complete_run(session, run_id, "completed", stats=extract_stats)
    except Exception as e:
        with get_session() as session:
            complete_run(session, run_id, "failed", error=str(e))
        console.print(f"[red]Extraction failed: {e}[/red]")
        raise typer.Exit(1)

    console.print(f"[green]Extracted {extract_stats['messages']} messages[/green]\n")

    # Step 2: Migrate
    console.print("[bold blue]Step 2: Migrate[/bold blue]")
    chat = GoogleChatClient(
        settings.service_account_key_path,
        settings.google.admin_email,
        messages_per_second=settings.google.messages_per_second,
    )
    files = FileHandler(client, chat, settings)
    executor = MigrationExecutor(client, chat, files, settings)

    run_id2 = str(uuid.uuid4())
    with get_session() as session:
        create_run(session, run_id2, "run-migrate")

    try:
        migrate_stats = executor.migrate_all(
            channel_filter=channel_filter,
            dry_run=dry_run,
            resume=True,
            skip_files=skip_files,
        )
        with get_session() as session:
            complete_run(session, run_id2, "completed", stats=migrate_stats)

        console.print(f"\n[green]Done! {migrate_stats.get('messages_migrated', 0)} messages migrated.[/green]")
    except Exception as e:
        with get_session() as session:
            complete_run(session, run_id2, "failed", error=str(e))
        console.print(f"[red]Migration failed: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()

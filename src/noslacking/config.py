"""Configuration loading from YAML + environment variables."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class SlackConfig(BaseModel):
    channel_types: list[str] = ["public_channel", "private_channel"]
    include_channels: list[str] = []
    exclude_channels: list[str] = []
    include_archived: bool = False
    messages_per_page: int = 200
    extract_files: bool = True
    extract_threads: bool = True
    max_file_size_mb: int = 100


class GoogleConfig(BaseModel):
    service_account_key: str = "~/.slack-to-chat/service-account.json"
    domain: str = ""
    admin_email: str = ""
    file_upload_method: str = "google_drive"
    messages_per_second: int = 8
    spaces_per_minute: int = 50
    concurrent_spaces: int = 3


class UserMappingConfig(BaseModel):
    strategy: str = "email"
    overrides: dict[str, str] = {}
    unmapped_action: str = "attribute"


class MigrationConfig(BaseModel):
    include_system_messages: bool = False
    dry_run: bool = False
    space_name_template: str = "[Slack] {name}"
    space_description_template: str = "Migrated from Slack channel #{name}"


class Settings(BaseSettings):
    # Secrets from environment
    slack_bot_token: str = ""
    slack_user_token: str = ""
    google_service_account_key: str = ""

    # Paths
    data_dir: str = "~/.slack-to-chat"
    config_path: str = "~/.slack-to-chat/config.yaml"
    log_level: str = "INFO"

    # Nested configs (populated from YAML)
    slack: SlackConfig = Field(default_factory=SlackConfig)
    google: GoogleConfig = Field(default_factory=GoogleConfig)
    user_mapping: UserMappingConfig = Field(default_factory=UserMappingConfig)
    migration: MigrationConfig = Field(default_factory=MigrationConfig)

    model_config = {
        "env_prefix": "",
        "env_file": [".env", str(Path("~/.slack-to-chat/.env").expanduser()), str(Path("~/.noslacking/.env").expanduser())],
        "extra": "ignore",
    }

    @property
    def data_path(self) -> Path:
        return Path(self.data_dir).expanduser()

    @property
    def db_path(self) -> Path:
        return self.data_path / "migration.db"

    @property
    def cache_path(self) -> Path:
        return self.data_path / "cache"

    @property
    def logs_path(self) -> Path:
        return self.data_path / "logs"

    @property
    def service_account_key_path(self) -> Path:
        key = self.google_service_account_key or self.google.service_account_key
        return Path(key).expanduser()


def load_config(config_path: str | None = None, data_dir: str | None = None) -> Settings:
    """Load settings from YAML config file + environment variables."""
    # First load env-only settings to find config path
    env_settings = Settings()

    path = Path(config_path or env_settings.config_path).expanduser()
    yaml_data: dict[str, Any] = {}

    if path.exists():
        with open(path) as f:
            yaml_data = yaml.safe_load(f) or {}

    # Override with explicit args
    if data_dir:
        yaml_data["data_dir"] = data_dir
    if config_path:
        yaml_data["config_path"] = config_path

    return Settings(**yaml_data)


def write_config(settings: Settings, config_path: Path | None = None) -> Path:
    """Write current settings to YAML config file (excludes secrets)."""
    path = config_path or Path(settings.config_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "data_dir": settings.data_dir,
        "log_level": settings.log_level,
        "slack": settings.slack.model_dump(),
        "google": settings.google.model_dump(),
        "user_mapping": settings.user_mapping.model_dump(),
        "migration": settings.migration.model_dump(),
    }

    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    return path

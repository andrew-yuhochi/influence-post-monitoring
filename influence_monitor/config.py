"""Application settings loaded from environment variables via pydantic-settings."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Required
    anthropic_api_key: str = ""
    resend_api_key: str = ""
    twitter_username: str = ""
    twitter_email: str = ""
    twitter_password: str = ""
    recipient_email: str = ""

    # Optional with defaults
    sender_email: str = "Influence Monitor <onboarding@resend.dev>"
    twitter_source: str = "twitter_twikit"
    email_provider: str = "resend"
    database_path: str = "data/signals.db"
    cookies_path: str = "data/twitter_cookies.json"
    min_accounts_threshold: int = 13
    signal_min_score: float = 2.0
    top_n_signals: int = 10
    conviction_min: int = 2
    corroboration_multiplier: float = 1.5
    track_record_min_calls: int = 5
    log_level: str = "INFO"
    timezone: str = "America/New_York"
    alpha_vantage_api_key: str = ""

    @property
    def database_path_resolved(self) -> Path:
        """Return database_path resolved relative to the project root."""
        return Path(self.database_path)

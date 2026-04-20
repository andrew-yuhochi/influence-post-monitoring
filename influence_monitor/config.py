"""Application settings loaded from environment variables via pydantic-settings."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # LLM
    anthropic_api_key: str = ""

    # Social ingestion (twikit throwaway account)
    twikit_username: str = ""
    twikit_email: str = ""
    twikit_password: str = ""
    twitter_username: str = ""
    twitter_email: str = ""
    twitter_password: str = ""
    # Optional: full cookies JSON string (GitHub Actions secret TWITTER_COOKIES_JSON).
    # When set, the pipeline writes this to cookies_path before twikit initialises,
    # providing cookie persistence without committing the file to the repo.
    twitter_cookies_json: str = ""

    # WhatsApp — Twilio (primary delivery)
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_whatsapp_from: str = ""   # E.164 number, e.g. +14155238886
    recipient_phone_e164: str = ""   # Recipient E.164 number, e.g. +14161234567

    # WhatsApp — CallMeBot (fallback delivery)
    callmebot_phone: str = ""
    callmebot_api_key: str = ""

    # Database
    turso_url: str = ""              # libsql://<db>.turso.io  (empty = local SQLite)
    turso_token: str = ""
    database_path: str = "data/signals.db"

    # Email delivery
    resend_api_key: str = ""
    sender_email: str = ""
    recipient_email: str = ""

    # Market data
    alpha_vantage_api_key: str = ""

    # Social source selection
    social_source: str = "twitter_twikit"
    delivery_primary: str = "twilio"
    delivery_fallback: str = "callmebot"

    # Operational defaults
    cookies_path: str = "data/twitter_cookies.json"
    max_posts_per_account: int = 10
    min_accounts_threshold: int = 24
    poll_interval_hours: int = 2
    market_hours_start_et: str = "09:00"
    market_hours_end_et: str = "17:00"
    morning_send_et: str = "09:00"
    evening_send_et: str = "16:45"
    virality_views_threshold: int = 50000
    virality_reposts_threshold: int = 500
    corroboration_multiplier: float = 1.5
    conviction_min: int = 3
    signal_min_score: float = 5.0
    top_n_signals: int = 5
    track_record_min_calls: int = 5
    scorecard_min_days: int = 20
    timezone: str = "America/New_York"
    log_level: str = "INFO"

    @property
    def database_path_resolved(self) -> Path:
        """Return database_path resolved relative to the project root."""
        return Path(self.database_path)

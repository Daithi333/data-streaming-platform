from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Global DSP settings.

    Convention:
      - Environment variables use prefix DSP_*
      - Nested config uses __, e.g. DSP_KAFKA__BROKERS
    """

    model_config = SettingsConfigDict(
        env_prefix="DSP_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    # --- Kafka ---
    kafka_brokers: str = Field(default="kafka:9092")
    kafka_topic: str = Field(default="taxi_trips")
    kafka_starting_offsets: str = Field(
        default="earliest"
    )  # start from earliest for local demo repeatability

    # --- Storage (local dev paths; in prod these become abfss://... or s3://...) ---
    data_dir: str = Field(default="/opt/dsp/data")
    checkpoint_dir: str = Field(default="/opt/dsp/checkpoints")

    # --- Streaming defaults ---
    trigger_interval: str = Field(default="10 seconds")

    # --- Logging ---
    app_log_level: str = Field(default="INFO")
    spark_log_level: str = Field(default="WARN")
    log_query_progress_seconds: Optional[int] = Field(default=None)

    # --- Derived paths (shared helpers) ---
    def bronze_path(self, dataset: str) -> str:
        return f"{self.data_dir}/bronze/{dataset}"

    def bronze_checkpoint(self, dataset: str) -> str:
        return f"{self.checkpoint_dir}/bronze/{dataset}"

    def silver_path(self, dataset: str) -> str:
        return f"{self.data_dir}/silver/{dataset}"

    def silver_checkpoint(self, dataset: str) -> str:
        return f"{self.checkpoint_dir}/silver/{dataset}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # Cached so every import doesn’t re-parse env
    return Settings()

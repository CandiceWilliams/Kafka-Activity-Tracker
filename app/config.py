"""
Configuration management for Kafka Activity Tracker.
Uses environment variables for flexibility across environments.
"""

import os
from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with validation."""

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092,localhost:9093,localhost:9094"
    KAFKA_CLIENT_ID: str = "activity-tracker"

    # Topic Configuration
    TOPIC_PAGE_VIEWS: str = "page-views"
    TOPIC_BUTTON_CLICKS: str = "button-clicks"
    TOPIC_SLIDER_EVENTS: str = "slider-events"
    TOPIC_DROPDOWN_SELECTIONS: str = "dropdown-selections"
    TOPIC_TEXT_INPUTS: str = "text-inputs"
    TOPIC_TOGGLE_EVENTS: str = "toggle-events"
    TOPIC_USER_SESSIONS: str = "user-sessions"
    TOPIC_ANALYTICS_RESULTS: str = "analytics-results"

    # Partition Configuration
    PARTITIONS_HIGH_VOLUME: int = 3  # For clicks, page views
    PARTITIONS_MEDIUM_VOLUME: int = 2  # For other events
    PARTITIONS_LOW_VOLUME: int = 1  # For aggregated results

    # Replication Configuration
    REPLICATION_FACTOR: int = 3
    MIN_IN_SYNC_REPLICAS: int = 2

    # Consumer Group Configuration
    CONSUMER_GROUP_ANALYTICS: str = "analytics-processor-group"
    CONSUMER_GROUP_DASHBOARD: str = "dashboard-consumer-group"
    CONSUMER_GROUP_AUDIT: str = "audit-logger-group"

    # Performance Settings
    PRODUCER_BATCH_SIZE: int = 16384  # 16KB
    PRODUCER_LINGER_MS: int = 10  # Wait 10ms to batch messages
    PRODUCER_COMPRESSION_TYPE: str = "snappy"  # Compress messages

    CONSUMER_AUTO_OFFSET_RESET: str = "latest"
    CONSUMER_ENABLE_AUTO_COMMIT: bool = True
    CONSUMER_MAX_POLL_RECORDS: int = 500

    # Application Settings
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    LOG_LEVEL: str = "INFO"

    # ksqlDB Configuration
    KSQLDB_SERVER_URL: str = "http://localhost:8088"

    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()


# Topic mapping for easy routing
TOPIC_MAPPING = {
    "page_load": settings.TOPIC_PAGE_VIEWS,
    "button_click": settings.TOPIC_BUTTON_CLICKS,
    "slider_input": settings.TOPIC_SLIDER_EVENTS,
    "dropdown_selection": settings.TOPIC_DROPDOWN_SELECTIONS,
    "text_input": settings.TOPIC_TEXT_INPUTS,
    "toggle_switch": settings.TOPIC_TOGGLE_EVENTS,
}

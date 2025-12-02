"""
Base Kafka consumer with error handling and graceful shutdown.
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
import signal
import sys
from typing import Callable, List
from app.config import settings

logger = logging.getLogger(__name__)


class BaseConsumer:
    """
    Base consumer class with common functionality.

    Features:
    - Graceful shutdown handling
    - Error recovery
    - Offset management
    - Extensible processing
    """

    def __init__(
        self,
        topics: List[str],
        group_id: str,
        process_func: Callable,
        auto_offset_reset: str = "latest",
    ):
        """
        Initialize consumer.

        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID (for load balancing)
            process_func: Function to process each message
            auto_offset_reset: Where to start if no offset exists
        """
        self.topics = topics
        self.group_id = group_id
        self.process_func = process_func
        self.consumer = self._create_consumer(auto_offset_reset)
        self.running = True

        # Setup graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _create_consumer(self, auto_offset_reset: str) -> KafkaConsumer:
        """
        Create Kafka consumer.

        Configuration Explained:
        - group_id: Multiple consumers with same group_id share load
        - auto_offset_reset: 'latest' = only new messages, 'earliest' = all messages
        - enable_auto_commit: Automatically save position (simpler, small data loss risk)
        - max_poll_records: How many messages to fetch at once
        """
        try:
            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
                client_id=f"{settings.KAFKA_CLIENT_ID}-{self.group_id}",
                group_id=self.group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=settings.CONSUMER_ENABLE_AUTO_COMMIT,
                max_poll_records=settings.CONSUMER_MAX_POLL_RECORDS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
            logger.info(f"Consumer {self.group_id} connected to topics: {self.topics}")
            return consumer
        except KafkaError as e:
            logger.error(f"Failed to create consumer: {e}")
            raise

    def start(self):
        """
        Start consuming messages.

        This is the main consumer loop:
        1. Poll for messages
        2. Process each message
        3. Handle errors
        4. Continue until shutdown
        """
        logger.info(f"Consumer {self.group_id} starting...")

        try:
            for message in self.consumer:
                if not self.running:
                    break

                try:
                    # Extract message data
                    event_data = message.value
                    key = message.key

                    # Log for debugging
                    logger.debug(
                        f"Processing message from "
                        f"topic={message.topic} "
                        f"partition={message.partition} "
                        f"offset={message.offset}"
                    )

                    # Process the event
                    self.process_func(event_data, key)

                except Exception as e:
                    # Log error but continue processing
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            self.close()

    def _shutdown(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received shutdown signal {signum}")
        self.running = False

    def close(self):
        """Close consumer and commit offsets."""
        logger.info(f"Closing consumer {self.group_id}...")
        self.consumer.close()
        logger.info("Consumer closed successfully")


def create_analytics_consumer():
    """
    Create consumer for analytics processing.

    This consumer:
    - Subscribes to all event topics
    - Processes events for analytics
    - Writes results to analytics-results topic
    """
    from app.services.analytics_service import process_analytics_event

    topics = [
        settings.TOPIC_PAGE_VIEWS,
        settings.TOPIC_BUTTON_CLICKS,
        settings.TOPIC_SLIDER_EVENTS,
        settings.TOPIC_DROPDOWN_SELECTIONS,
        settings.TOPIC_TEXT_INPUTS,
        settings.TOPIC_TOGGLE_EVENTS,
    ]

    return BaseConsumer(
        topics=topics,
        group_id=settings.CONSUMER_GROUP_ANALYTICS,
        process_func=process_analytics_event,
        auto_offset_reset="earliest",  # Process all historical data
    )


def create_dashboard_consumer():
    """
    Create consumer for dashboard updates.

    This consumer:
    - Subscribes to analytics-results topic
    - Updates dashboard state
    - Pushes updates via WebSocket
    """
    from app.services.analytics_service import update_dashboard

    return BaseConsumer(
        topics=[settings.TOPIC_ANALYTICS_RESULTS],
        group_id=settings.CONSUMER_GROUP_DASHBOARD,
        process_func=update_dashboard,
        auto_offset_reset="latest",  # Only show current data
    )

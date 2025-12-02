"""
Kafka producer with topic routing and error handling.
Routes different event types to appropriate topics.
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from typing import Dict, Any
from app.config import settings, TOPIC_MAPPING
from app.models.events import BaseEvent

logger = logging.getLogger(__name__)


class ActivityProducer:
    """
    Producer that routes events to appropriate Kafka topics.

    Key Features:
    - Topic routing based on event type
    - Retry logic for transient failures
    - Compression for network efficiency
    - Batching for throughput
    """

    def __init__(self):
        """Initialize Kafka producer with optimal settings."""
        self.producer = self._create_producer()
        self.metrics = {"messages_sent": 0, "messages_failed": 0, "bytes_sent": 0}

    def _create_producer(self) -> KafkaProducer:
        """
        Create Kafka producer with retry logic.

        Configuration Explained:
        - bootstrap_servers: List of all brokers for redundancy
        - acks='all': Wait for all replicas to acknowledge (durability)
        - retries: Retry failed sends (handles transient failures)
        - batch_size: Group messages for efficiency
        - linger_ms: Wait briefly to fill batches
        - compression_type: Reduce network usage
        """
        for attempt in range(5):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
                    client_id=f"{settings.KAFKA_CLIENT_ID}-producer",
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",  # Wait for all in-sync replicas
                    retries=3,
                    batch_size=settings.PRODUCER_BATCH_SIZE,
                    linger_ms=settings.PRODUCER_LINGER_MS,
                    compression_type=settings.PRODUCER_COMPRESSION_TYPE,
                )
                logger.info("Kafka Producer connected successfully")
                return producer
            except KafkaError as e:
                logger.warning(f"Kafka not ready (attempt {attempt + 1}/5): {e}")
                if attempt < 4:
                    import time

                    time.sleep(2)
                else:
                    raise Exception("Failed to connect to Kafka after 5 attempts")

    def send_event(self, event: BaseEvent, key: str = None) -> bool:
        """
        Send an event to the appropriate Kafka topic.

        Args:
            event: Validated event object
            key: Optional message key for partition routing

        Returns:
            bool: True if send was successful

        Key Concepts:
        - Topic Routing: Different events go to different topics
        - Message Key: Events with same key go to same partition (ordering)
        - Async Send: Returns immediately, callback handles result
        """
        topic = TOPIC_MAPPING.get(event.event_type)
        if not topic:
            logger.error(f"Unknown event type: {event.event_type}")
            return False

        # Use session_id as key for session-based ordering
        if not key and event.session_id:
            key = event.session_id

        try:
            # Serialize event to dict
            event_data = event.model_dump()

            # Send asynchronously with callback
            future = self.producer.send(topic, value=event_data, key=key)

            # Add callback to handle result
            future.add_callback(self._on_send_success, event.event_type)
            future.add_errback(self._on_send_error, event.event_type)

            return True

        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            self.metrics["messages_failed"] += 1
            return False

    def _on_send_success(self, metadata, event_type):
        """Callback for successful send."""
        self.metrics["messages_sent"] += 1
        logger.debug(
            f"Event {event_type} sent to "
            f"topic={metadata.topic} "
            f"partition={metadata.partition} "
            f"offset={metadata.offset}"
        )

    def _on_send_error(self, exception, event_type):
        """Callback for failed send."""
        self.metrics["messages_failed"] += 1
        logger.error(f"Failed to send {event_type}: {exception}")

    def flush(self):
        """Force send all batched messages."""
        self.producer.flush()

    def close(self):
        """Close producer and release resources."""
        self.producer.close()
        logger.info(f"Producer closed. Metrics: {self.metrics}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        return self.metrics.copy()


# Global producer instance
_producer_instance = None


def get_producer() -> ActivityProducer:
    """Get or create global producer instance."""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = ActivityProducer()
    return _producer_instance

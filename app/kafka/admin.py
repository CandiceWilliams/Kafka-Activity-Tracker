"""
Kafka administration utilities.
Handles topic creation and configuration.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging
from app.config import settings

logger = logging.getLogger(__name__)


def ensure_topics_exist():
    """
    Create all required Kafka topics if they don't exist.

    This demonstrates:
    - Programmatic topic creation
    - Partition configuration
    - Replication configuration

    In production, topics are often pre-created by ops team,
    but for development/demo, auto-creation is convenient.
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
        client_id=f"{settings.KAFKA_CLIENT_ID}-admin",
    )

    # Define all topics with their configurations
    topics = [
        NewTopic(
            name=settings.TOPIC_PAGE_VIEWS,
            num_partitions=settings.PARTITIONS_HIGH_VOLUME,
            replication_factor=settings.REPLICATION_FACTOR,
            topic_configs={
                "min.insync.replicas": str(settings.MIN_IN_SYNC_REPLICAS),
                "retention.ms": "604800000",  # 7 days
            },
        ),
        NewTopic(
            name=settings.TOPIC_BUTTON_CLICKS,
            num_partitions=settings.PARTITIONS_HIGH_VOLUME,
            replication_factor=settings.REPLICATION_FACTOR,
            topic_configs={
                "min.insync.replicas": str(settings.MIN_IN_SYNC_REPLICAS),
                "retention.ms": "604800000",
            },
        ),
        NewTopic(
            name=settings.TOPIC_SLIDER_EVENTS,
            num_partitions=settings.PARTITIONS_MEDIUM_VOLUME,
            replication_factor=settings.REPLICATION_FACTOR,
            topic_configs={
                "min.insync.replicas": str(settings.MIN_IN_SYNC_REPLICAS),
                "retention.ms": "604800000",
            },
        ),
        NewTopic(
            name=settings.TOPIC_DROPDOWN_SELECTIONS,
            num_partitions=settings.PARTITIONS_MEDIUM_VOLUME,
            replication_factor=settings.REPLICATION_FACTOR,
            topic_configs={
                "min.insync.replicas": str(settings.MIN_IN_SYNC_REPLICAS),
                "retention.ms": "604800000",
            },
        ),
        NewTopic(
            name=settings.TOPIC_TEXT_INPUTS,
            num_partitions=settings.PARTITIONS_MEDIUM_VOLUME,
            replication_factor=settings.REPLICATION_FACTOR,
            topic_configs={
                "min.insync.replicas": str(settings.MIN_IN_SYNC_REPLICAS),
                "retention.ms": "604800000",
            },
        ),
        NewTopic(
            name=settings.TOPIC_TOGGLE_EVENTS,
            num_partitions=settings.PARTITIONS_MEDIUM_VOLUME,
            replication_factor=settings.REPLICATION_FACTOR,
            topic_configs={
                "min.insync.replicas": str(settings.MIN_IN_SYNC_REPLICAS),
                "retention.ms": "604800000",
            },
        ),
        NewTopic(
            name=settings.TOPIC_USER_SESSIONS,
            num_partitions=settings.PARTITIONS_MEDIUM_VOLUME,
            replication_factor=settings.REPLICATION_FACTOR,
            topic_configs={
                "min.insync.replicas": str(settings.MIN_IN_SYNC_REPLICAS),
                "retention.ms": "2592000000",  # 30 days (keep sessions longer)
            },
        ),
        NewTopic(
            name=settings.TOPIC_ANALYTICS_RESULTS,
            num_partitions=settings.PARTITIONS_LOW_VOLUME,
            replication_factor=settings.REPLICATION_FACTOR,
            topic_configs={
                "min.insync.replicas": str(settings.MIN_IN_SYNC_REPLICAS),
                "retention.ms": "86400000",  # 1 day (aggregated data)
            },
        ),
    ]

    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        logger.info(f"Created {len(topics)} topics successfully")
    except TopicAlreadyExistsError:
        logger.info("Topics already exist, skipping creation")
    except Exception as e:
        logger.error(f"Error creating topics: {e}")
        raise
    finally:
        admin_client.close()

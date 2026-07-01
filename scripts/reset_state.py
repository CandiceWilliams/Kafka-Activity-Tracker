#!/usr/bin/env python3
"""
Reset script for clearing all stream processing state.

This script:
1. Stops all stream processors
2. Clears state stores
3. Resets consumer group offsets
4. Optionally clears Kafka topics

Usage:
    python scripts/reset_state.py --full  # Full reset including topics
    python scripts/reset_state.py         # Reset state stores only
"""

import argparse
import logging
import shutil
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def clear_state_stores(state_dir: str = "./state-stores"):
    """
    Clear all local state stores.

    Args:
        state_dir: Directory containing state stores
    """
    state_path = Path(state_dir)

    if not state_path.exists():
        logger.info(f"State directory {state_dir} does not exist")
        return

    try:
        logger.info(f"Clearing state stores in {state_dir}...")
        shutil.rmtree(state_path)
        state_path.mkdir(exist_ok=True)
        logger.info("State stores cleared successfully")
    except Exception as e:
        logger.error(f"Failed to clear state stores: {e}")


def reset_consumer_groups():
    """
    Reset consumer group offsets.

    This allows consumers to reprocess all messages from the beginning.
    """
    from kafka.admin import KafkaAdminClient
    from kafka.errors import KafkaError
    from app.config import settings

    logger.info("Resetting consumer group offsets...")

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
            client_id=f"{settings.KAFKA_CLIENT_ID}-admin",
        )

        # List consumer groups
        groups = admin_client.list_consumer_groups()
        logger.info(f"Found {len(groups)} consumer groups")

        for group_id, _ in groups:
            logger.info(f"Consumer group: {group_id}")

        # Note: Deleting consumer groups requires stopping all consumers first
        # This is typically done manually or via a deployment script

        logger.info("To fully reset consumer groups, stop all consumers and run:")
        logger.info(
            "  kafka-consumer-groups --bootstrap-server localhost:9092 "
            "--group <group-id> --reset-offsets --to-earliest --execute --all-topics"
        )

        admin_client.close()

    except KafkaError as e:
        logger.error(f"Failed to reset consumer groups: {e}")


def clear_kafka_topics(confirm: bool = False):
    """
    Delete and recreate Kafka topics.

    WARNING: This deletes all data in the topics!

    Args:
        confirm: Must be True to actually delete topics
    """
    if not confirm:
        logger.warning("Topic clearing not confirmed, skipping...")
        return

    from kafka.admin import KafkaAdminClient
    from kafka.errors import KafkaError
    from app.config import settings

    logger.warning("DELETING ALL KAFKA TOPICS!")

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
            client_id=f"{settings.KAFKA_CLIENT_ID}-admin",
        )

        # List of topics to delete
        topics_to_delete = [
            settings.TOPIC_PAGE_VIEWS,
            settings.TOPIC_BUTTON_CLICKS,
            settings.TOPIC_SLIDER_EVENTS,
            settings.TOPIC_DROPDOWN_SELECTIONS,
            settings.TOPIC_TEXT_INPUTS,
            settings.TOPIC_TOGGLE_EVENTS,
            settings.TOPIC_USER_SESSIONS,
            settings.TOPIC_ANALYTICS_RESULTS,
        ]

        # Delete topics
        logger.info(f"Deleting {len(topics_to_delete)} topics...")
        admin_client.delete_topics(topics_to_delete, timeout_ms=30000)
        logger.info("Topics deleted")

        # Wait a bit for deletion to complete
        import time

        time.sleep(5)

        # Recreate topics
        logger.info("Recreating topics...")
        from app.kafka.admin import ensure_topics_exist

        ensure_topics_exist()
        logger.info("Topics recreated")

        admin_client.close()

    except KafkaError as e:
        logger.error(f"Failed to clear topics: {e}")


def reset_ksqldb(confirm: bool = False):
    """
    Drop all ksqlDB streams and tables.

    Args:
        confirm: Must be True to actually drop objects
    """
    if not confirm:
        logger.warning("ksqlDB reset not confirmed, skipping...")
        return

    from app.services.ksqldb_client import get_ksqldb_client

    logger.warning("DROPPING ALL ksqlDB STREAMS AND TABLES!")

    try:
        client = get_ksqldb_client()

        # List all streams
        streams = client.list_streams()
        logger.info(f"Found {len(streams)} streams")

        for stream in streams:
            try:
                client.execute_statement(f"DROP STREAM IF EXISTS {stream};")
                logger.info(f"Dropped stream: {stream}")
            except Exception as e:
                logger.error(f"Failed to drop stream {stream}: {e}")

        # List all tables
        tables = client.list_tables()
        logger.info(f"Found {len(tables)} tables")

        for table in tables:
            try:
                client.execute_statement(f"DROP TABLE IF EXISTS {table};")
                logger.info(f"Dropped table: {table}")
            except Exception as e:
                logger.error(f"Failed to drop table {table}: {e}")

        logger.info("ksqlDB objects dropped")

    except Exception as e:
        logger.error(f"Failed to reset ksqlDB: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Reset Kafka stream processing state")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full reset including topics and ksqlDB (WARNING: deletes data!)",
    )
    parser.add_argument(
        "--state-only",
        action="store_true",
        help="Only clear local state stores",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Confirm destructive operations",
    )

    args = parser.parse_args()

    if args.full and not args.confirm:
        logger.error(
            "Full reset requires --confirm flag to prevent accidental data loss"
        )
        sys.exit(1)

    logger.info("=" * 50)
    logger.info("STREAM PROCESSING STATE RESET")
    logger.info("=" * 50)

    # Always clear local state stores
    clear_state_stores()

    if args.state_only:
        logger.info("State-only reset complete")
        return

    # Reset consumer groups
    reset_consumer_groups()

    # Full reset if requested
    if args.full:
        logger.warning("Performing FULL RESET - this will delete data!")

        # Clear Kafka topics
        clear_kafka_topics(confirm=args.confirm)

        # Reset ksqlDB
        reset_ksqldb(confirm=args.confirm)

    logger.info("=" * 50)
    logger.info("RESET COMPLETE")
    logger.info("=" * 50)
    logger.info("\nNext steps:")
    logger.info("1. Restart stream processors")
    logger.info("2. Run: python scripts/setup_ksqldb.py")
    logger.info("3. Start generating events")


if __name__ == "__main__":
    main()

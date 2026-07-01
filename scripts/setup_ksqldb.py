#!/usr/bin/env python3
"""
Setup script for ksqlDB streams and tables.

Run this after starting the Kafka cluster and ksqlDB server
to create all necessary streams, tables, and queries.
"""

import sys
import time
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def wait_for_ksqldb(client, max_retries=30, delay=2):
    """Wait for ksqlDB server to be ready."""
    logger.info("Waiting for ksqlDB server to be ready...")

    for attempt in range(max_retries):
        try:
            # Try to execute a simple statement
            client.execute_statement("SHOW STREAMS;")
            logger.info("ksqlDB server is ready!")
            return True
        except Exception as e:
            logger.info(
                f"ksqlDB not ready yet (attempt {attempt + 1}/{max_retries}): {e}"
            )
            time.sleep(delay)

    logger.error("ksqlDB server did not become ready in time")
    return False


def main():
    """Set up ksqlDB streams and tables."""
    logger.info("Starting ksqlDB setup...")

    # Import here to avoid circular dependencies
    try:
        from app.services.ksqldb_client import get_ksqldb_client
    except ImportError:
        # If running from scripts directory
        from pathlib import Path
        import sys

        sys.path.insert(0, str(Path(__file__).parent.parent))
        from app.services.ksqldb_client import get_ksqldb_client

    client = get_ksqldb_client()

    # Wait for ksqlDB to be ready
    if not wait_for_ksqldb(client):
        logger.error("Failed to connect to ksqlDB server")
        sys.exit(1)

    # Set up streams and tables
    try:
        client.setup_activity_streams()
        logger.info("ksqlDB setup complete!")

        # List created streams and tables
        logger.info("Created streams:")
        for stream in client.list_streams():
            logger.info(f"  - {stream}")

        logger.info("Created tables:")
        for table in client.list_tables():
            logger.info(f"  - {table}")

    except Exception as e:
        logger.error(f"Failed to set up ksqlDB: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

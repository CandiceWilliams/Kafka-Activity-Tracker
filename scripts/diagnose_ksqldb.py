#!/usr/bin/env python3
"""
Diagnostic script for ksqlDB setup issues.
"""

import sys
import requests
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.config import settings


def check_ksqldb_status():
    """Check ksqlDB server status."""
    print("=" * 60)
    print("1. Checking ksqlDB Server Status")
    print("=" * 60)

    try:
        response = requests.get(f"{settings.KSQLDB_SERVER_URL}/info")
        info = response.json()
        print(f"✓ ksqlDB is accessible")
        print(f"  Version: {info['KsqlServerInfo']['version']}")
        print(f"  Status: {info['KsqlServerInfo']['serverStatus']}")
        print(f"  Cluster ID: {info['KsqlServerInfo']['kafkaClusterId']}")

        if info["KsqlServerInfo"]["serverStatus"] != "RUNNING":
            print(
                f"\n⚠️  WARNING: Server status is {info['KsqlServerInfo']['serverStatus']}"
            )
            print("   This may cause stream creation to fail")
        print()
        return True
    except Exception as e:
        print(f"✗ Failed to connect to ksqlDB: {e}\n")
        return False


def check_kafka_topics():
    """Check if Kafka topics exist."""
    print("=" * 60)
    print("2. Checking Kafka Topics")
    print("=" * 60)

    try:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
            consumer_timeout_ms=1000,
        )

        topics = consumer.topics()

        required_topics = [
            settings.TOPIC_PAGE_VIEWS,
            settings.TOPIC_BUTTON_CLICKS,
            settings.TOPIC_SLIDER_EVENTS,
        ]

        print("Required topics:")
        for topic in required_topics:
            if topic in topics:
                print(f"  ✓ {topic}")
            else:
                print(f"  ✗ {topic} - MISSING!")

        print()
        consumer.close()
        return True
    except Exception as e:
        print(f"✗ Failed to check topics: {e}\n")
        return False


def check_existing_streams():
    """Check what streams already exist."""
    print("=" * 60)
    print("3. Checking Existing ksqlDB Streams")
    print("=" * 60)

    try:
        response = requests.post(
            f"{settings.KSQLDB_SERVER_URL}/ksql",
            json={"ksql": "SHOW STREAMS;"},
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
        )

        if response.status_code == 200:
            result = response.json()
            if result and len(result) > 0:
                streams = result[0].get("streams", [])
                if streams:
                    print("Existing streams:")
                    for stream in streams:
                        print(f"  - {stream.get('name', 'unknown')}")
                else:
                    print("No streams found")
            else:
                print("No streams found")
        else:
            print(f"Failed to list streams: {response.status_code}")
            print(f"Response: {response.text}")
        print()
    except Exception as e:
        print(f"Error checking streams: {e}\n")


def check_existing_tables():
    """Check what tables already exist."""
    print("=" * 60)
    print("4. Checking Existing ksqlDB Tables")
    print("=" * 60)

    try:
        response = requests.post(
            f"{settings.KSQLDB_SERVER_URL}/ksql",
            json={"ksql": "SHOW TABLES;"},
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
        )

        if response.status_code == 200:
            result = response.json()
            if result and len(result) > 0:
                tables = result[0].get("tables", [])
                if tables:
                    print("Existing tables:")
                    for table in tables:
                        print(f"  - {table.get('name', 'unknown')}")
                else:
                    print("No tables found")
            else:
                print("No tables found")
        else:
            print(f"Failed to list tables: {response.status_code}")
            print(f"Response: {response.text}")
        print()
    except Exception as e:
        print(f"Error checking tables: {e}\n")


def test_simple_query():
    """Test a simple ksqlDB query."""
    print("=" * 60)
    print("5. Testing Simple Query")
    print("=" * 60)

    try:
        # Try to create a simple stream
        test_query = """
        CREATE STREAM IF NOT EXISTS test_stream (
            id VARCHAR,
            value INT
        ) WITH (
            KAFKA_TOPIC='test-topic',
            VALUE_FORMAT='JSON',
            PARTITIONS=1
        );
        """

        response = requests.post(
            f"{settings.KSQLDB_SERVER_URL}/ksql",
            json={"ksql": test_query},
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
        )

        if response.status_code == 200:
            print("✓ Simple stream creation successful")
            # Clean up
            cleanup = "DROP STREAM IF EXISTS test_stream;"
            requests.post(
                f"{settings.KSQLDB_SERVER_URL}/ksql",
                json={"ksql": cleanup},
                headers={"Content-Type": "application/vnd.ksql.v1+json"},
            )
        else:
            print(f"✗ Stream creation failed: {response.status_code}")
            print(f"Response: {response.text}")
        print()
    except Exception as e:
        print(f"Error testing query: {e}\n")


def check_ksqldb_logs():
    """Suggest checking ksqlDB logs."""
    print("=" * 60)
    print("6. Next Steps")
    print("=" * 60)
    print("\nIf issues persist, check ksqlDB logs:")
    print("  docker logs ksqldb-server --tail 100")
    print("\nOr follow logs in real-time:")
    print("  docker logs -f ksqldb-server")
    print()


def main():
    """Run all diagnostics."""
    print("\n" + "=" * 60)
    print("ksqlDB Diagnostic Tool")
    print("=" * 60 + "\n")

    # Run checks
    ksqldb_ok = check_ksqldb_status()
    if not ksqldb_ok:
        print("\n⚠️  ksqlDB is not accessible. Please check:")
        print("  1. Docker containers are running: docker-compose ps")
        print("  2. ksqlDB logs: docker logs ksqldb-server")
        return

    check_kafka_topics()
    check_existing_streams()
    check_existing_tables()
    test_simple_query()
    check_ksqldb_logs()

    print("=" * 60)
    print("Diagnostics Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()

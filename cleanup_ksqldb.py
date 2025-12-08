#!/usr/bin/env python3
"""
Drop all ksqlDB streams and tables to start fresh.
Run this before setup_ksqldb.py
"""

import sys
import requests
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.config import settings


def drop_all_objects():
    """Drop all ksqlDB streams and tables."""
    url = f"{settings.KSQLDB_SERVER_URL}/ksql"
    headers = {"Content-Type": "application/vnd.ksql.v1+json"}

    print("=" * 60)
    print("Dropping all ksqlDB objects...")
    print("=" * 60 + "\n")

    # Drop tables first (they depend on streams)
    tables_to_drop = [
        "SESSION_ACTIVITY",
        "BUTTON_CLICKS_PER_MINUTE",
        "PAGE_VIEWS_PER_MINUTE",
        "SLIDER_AVG_PER_MINUTE",
    ]

    # Drop streams after tables
    streams_to_drop = [
        "PAGE_VIEWS_STREAM",
        "BUTTON_CLICKS_STREAM",
        "SLIDER_EVENTS_STREAM",
    ]

    print("Dropping tables...")
    for table_name in tables_to_drop:
        statement = f"DROP TABLE IF EXISTS {table_name} DELETE TOPIC;"

        try:
            response = requests.post(url, json={"ksql": statement}, headers=headers)

            if response.status_code == 200:
                print(f"  ✓ Dropped table {table_name}")
            else:
                print(f"  ⚠ Could not drop table {table_name}: {response.status_code}")
                # Print error details
                try:
                    error = response.json()
                    if error.get("message"):
                        print(f"    Message: {error['message']}")
                except:
                    pass
        except Exception as e:
            print(f"  ✗ Error dropping table {table_name}: {e}")

    print("\nDropping streams...")
    for stream_name in streams_to_drop:
        statement = f"DROP STREAM IF EXISTS {stream_name} DELETE TOPIC;"

        try:
            response = requests.post(url, json={"ksql": statement}, headers=headers)

            if response.status_code == 200:
                print(f"  ✓ Dropped stream {stream_name}")
            else:
                print(
                    f"  ⚠ Could not drop stream {stream_name}: {response.status_code}"
                )
                try:
                    error = response.json()
                    if error.get("message"):
                        print(f"    Message: {error['message']}")
                except:
                    pass
        except Exception as e:
            print(f"  ✗ Error dropping stream {stream_name}: {e}")

    print("\n" + "=" * 60)
    print("Cleanup complete!")
    print("=" * 60)
    print("\nNow run: python scripts/setup_ksqldb.py")


if __name__ == "__main__":
    drop_all_objects()

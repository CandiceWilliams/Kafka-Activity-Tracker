#!/usr/bin/env python3
"""
Load generator for testing Kafka stream processing.

This script simulates realistic user activity:
- Random button clicks
- Page navigation
- Slider adjustments
- Dropdown selections
- Text inputs
- Toggle switches

Usage:
    python scripts/generate_load.py --events 1000 --rate 10
"""

import asyncio
import random
import time
import argparse
import logging
from datetime import datetime
from typing import List
import uuid

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class LoadGenerator:
    """Generate realistic activity events for testing."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        """Initialize load generator."""
        self.base_url = base_url
        self.session_ids = [str(uuid.uuid4()) for _ in range(10)]

        # Event type weights (more realistic distribution)
        self.event_weights = {
            "page_load": 0.15,
            "button_click": 0.35,
            "slider_input": 0.15,
            "dropdown_selection": 0.10,
            "text_input": 0.15,
            "toggle_switch": 0.10,
        }

        # Sample data
        self.button_texts = [
            "Click Me",
            "Click",
            "Don't Click?",
            "Submit",
            "Cancel",
            "Save",
            "Load More",
            "Refresh",
        ]

        self.pages = [
            "/",
            "/dashboard",
            "/analytics",
            "/settings",
            "/profile",
            "/about",
        ]

        self.dropdown_values = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ]

        self.stats = {
            "sent": 0,
            "failed": 0,
            "by_type": {},
        }

    def generate_event(self) -> dict:
        """Generate a random event."""
        event_type = random.choices(
            list(self.event_weights.keys()),
            weights=list(self.event_weights.values()),
        )[0]

        session_id = random.choice(self.session_ids)

        event = {
            "event": event_type,
            "session_id": session_id,
            "timestamp": int(datetime.now().timestamp() * 1000),
        }

        # Add event-specific details
        if event_type == "page_load":
            event["url"] = random.choice(self.pages)

        elif event_type == "button_click":
            event["details"] = {"text": random.choice(self.button_texts)}

        elif event_type == "slider_input":
            event["details"] = {"value": random.randint(0, 100)}

        elif event_type == "dropdown_selection":
            event["details"] = {"value": random.choice(self.dropdown_values)}

        elif event_type == "text_input":
            event["details"] = {"value": f"test input {random.randint(1, 1000)}"}

        elif event_type == "toggle_switch":
            event["details"] = {"checked": random.choice([True, False])}

        return event

    def send_event(self, event: dict) -> bool:
        """Send an event to the server."""
        try:
            response = requests.post(f"{self.base_url}/event", json=event, timeout=5)
            response.raise_for_status()

            self.stats["sent"] += 1
            event_type = event["event"]
            self.stats["by_type"][event_type] = (
                self.stats["by_type"].get(event_type, 0) + 1
            )

            return True

        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            self.stats["failed"] += 1
            return False

    def generate_burst(self, count: int, delay: float = 0.1):
        """Generate a burst of events."""
        logger.info(f"Generating burst of {count} events...")

        for i in range(count):
            event = self.generate_event()
            self.send_event(event)

            if delay > 0:
                time.sleep(delay)

            if (i + 1) % 100 == 0:
                logger.info(f"Sent {i + 1}/{count} events")

        logger.info(f"Burst complete. Stats: {self.stats}")

    def generate_continuous(self, rate: int, duration: int = None):
        """
        Generate events continuously at a specified rate.

        Args:
            rate: Events per second
            duration: Duration in seconds (None = infinite)
        """
        logger.info(f"Generating events at {rate}/sec...")

        delay = 1.0 / rate if rate > 0 else 0.1
        start_time = time.time()

        try:
            while True:
                event = self.generate_event()
                self.send_event(event)
                time.sleep(delay)

                # Log stats every 10 seconds
                if int(time.time() - start_time) % 10 == 0:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"Elapsed: {elapsed:.1f}s, "
                        f"Sent: {self.stats['sent']}, "
                        f"Rate: {self.stats['sent']/elapsed:.1f}/sec"
                    )

                # Check duration
                if duration and (time.time() - start_time) >= duration:
                    break

        except KeyboardInterrupt:
            logger.info("Stopped by user")

        logger.info(f"Final stats: {self.stats}")

    def generate_realistic_session(self):
        """
        Generate a realistic user session.

        This simulates actual user behavior:
        - Start with page load
        - Perform several interactions
        - Occasional pauses
        - End session
        """
        session_id = str(uuid.uuid4())
        logger.info(f"Starting session {session_id}")

        # Page load
        self.send_event(
            {
                "event": "page_load",
                "session_id": session_id,
                "url": "/",
                "timestamp": int(datetime.now().timestamp() * 1000),
            }
        )
        time.sleep(random.uniform(0.5, 2.0))

        # Random interactions (5-15 events)
        num_interactions = random.randint(5, 15)
        for _ in range(num_interactions):
            event = self.generate_event()
            event["session_id"] = session_id
            self.send_event(event)

            # Realistic delay between interactions
            time.sleep(random.uniform(0.5, 5.0))

        logger.info(
            f"Session {session_id} complete with {num_interactions} interactions"
        )

    def test_windowing(self):
        """
        Generate events to test windowing behavior.

        Sends bursts of events at different intervals to verify:
        - Tumbling windows capture events correctly
        - Window boundaries are respected
        - Aggregations are accurate
        """
        logger.info("Testing windowing behavior...")

        # Send burst every 30 seconds for 3 minutes
        for i in range(6):
            burst_size = random.randint(50, 100)
            logger.info(f"Burst {i+1}/6: {burst_size} events")

            self.generate_burst(burst_size, delay=0.05)

            if i < 5:  # Don't wait after last burst
                logger.info("Waiting 30 seconds...")
                time.sleep(30)

        logger.info("Windowing test complete")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate load for Kafka activity tracker"
    )
    parser.add_argument(
        "--mode",
        choices=["burst", "continuous", "session", "windowing"],
        default="burst",
        help="Generation mode",
    )
    parser.add_argument(
        "--events",
        type=int,
        default=100,
        help="Number of events (burst mode)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=10,
        help="Events per second (continuous mode)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Duration in seconds (continuous mode)",
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="Base URL of the application",
    )

    args = parser.parse_args()

    generator = LoadGenerator(base_url=args.url)

    # Verify server is accessible
    try:
        response = requests.get(f"{args.url}/health", timeout=5)
        response.raise_for_status()
        logger.info("Server is accessible")
    except Exception as e:
        logger.error(f"Cannot reach server at {args.url}: {e}")
        return

    # Run selected mode
    if args.mode == "burst":
        generator.generate_burst(args.events)

    elif args.mode == "continuous":
        generator.generate_continuous(args.rate, args.duration)

    elif args.mode == "session":
        logger.info("Generating realistic sessions...")
        for i in range(5):
            generator.generate_realistic_session()
            time.sleep(random.uniform(1, 5))

    elif args.mode == "windowing":
        generator.test_windowing()

    # Print final stats
    print("\n" + "=" * 50)
    print("LOAD GENERATION COMPLETE")
    print("=" * 50)
    print(f"Total events sent: {generator.stats['sent']}")
    print(f"Failed: {generator.stats['failed']}")
    print("\nEvents by type:")
    for event_type, count in sorted(generator.stats["by_type"].items()):
        print(f"  {event_type}: {count}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()

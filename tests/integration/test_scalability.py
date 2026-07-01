"""
Test the impact of Replication Factor and Partition Count on system performance
"""

import time
import threading
from tests.activity_generator import run as run_events
from app.kafka.admin import ensure_topics_exist
from app.kafka.consumer import create_analytics_consumer

# Number of events to generate per test
TOTAL_EVENTS = 2000

MAX_PARTITIONS = 6
MAX_REPLICATION = 3

def recreate_topics(partitions):
    """Rebuilds all topics with new partitions count"""
    import app.config as cfg

    cfg.settings.PARTITIONS_HIGH_VOLUME = partitions
    cfg.settings.PARTITIONS_MEDIUM_VOLUME = partitions
    cfg.settings.PARTITIONS_LOW_VOLUME = partitions

    ensure_topics_exist()


def start_extra_consumers(num_consumers):
    """Start temporary consumer threads to match partition count"""
    consumers = []
    for i in range(num_consumers):
        consumer = create_analytics_consumer()
        t = threading.Thread(
            target=consumer.start,
            daemon=True,
            name=f"scalability-consumer-{i}"
        )
        t.start()

        consumers.append(consumer)

    return consumers


def throughput_test():
    """Run the load generator and measure throughput"""
    start = time.time()
    run_events(iterations=TOTAL_EVENTS)
    end = time.time()

    duration = end - start
    throughput = TOTAL_EVENTS / duration

    return throughput, duration


def test_scalability():
    """Main scalability test function"""
    results = []

    while True:
        try:
            partitions = int(input(f"\nEnter number of partitions (1–{MAX_PARTITIONS}): "))
            if 1 <= partitions <= MAX_PARTITIONS:
                break
            print("Value out of range")
        except ValueError:
            print("Invalid number")

    print(f"Testing topics with {partitions} partitions")

    recreate_topics(partitions)

    extra = max(0, partitions - 1)
    extra_consumers = start_extra_consumers(extra)
    print(f"Started {extra} extra consumers")
    print(f"Please wait for the test to complete...")

    throughput, duration = throughput_test()

    results.append({
         "partitions": partitions,
         "throughput": round(throughput, 2),
         "duration": round(duration, 2)
    })

    print("\nFINAL RESULTS")
    for r in results:
        print(r)

    # Shut down extra consumers
    for consumer in extra_consumers:
        consumer.running = False
        try:
            consumer.consumer.close()
        except Exception as e:
            print("Error closing consumer:", e)

def main():
    while True:
        test_scalability()

        again = input("\nRun another test? (y/n): ").strip().lower()
        if again != "y":
            print("Exiting Test")
            break


if __name__ == "__main__":
    main()
# tests/integration/test_security.py
import os
import time
from tests.activity_generator import send_event

def test_invalid_event_handling():
    """Invalid events should be rejected"""
    print("\nTest: Invalid Event Validation...\n")

    # Missing required field
    status = send_event({
        "event": "button_clic", # typo in event name
        "details": {},
        "session_id": "s1",
        "timestamp": 1
    }, return_status=True)
    assert status == 400 # if received an error from Kafka then continue
    print(f"Expected Status: 400, Received Status:", status)

    # Out of range slider
    status = send_event({
        "event": "slider_input",
        "details": {"value": 150}, # out of valid range
        "session_id": "s1",
        "timestamp": 2
    }, return_status=True)
    assert status == 500
    print(f"Expected Status: 500, Received Status:", status)

    # Wrong type
    status = send_event({
        "event": "slider_input",
        "details": {"value": "oops"}, # should be int
        "session_id": "s2",
        "timestamp": 3
    }, return_status=True)
    assert status == 500
    print(f"Expected Status: 500, Received Status:", status)

    print("Test 1 passed: Validation blocked all invalid events.\n")
    print("View logs for more information\n")

def test_consumer_error_recovery():
    print("\nTest: Consumer Error Recovery...\n")

    # Valid event
    status = send_event({
        "event": "button_click",
        "details": {"text": "ok1"},
        "session_id": "session-1",
        "timestamp": 1
    })
    print(f"Expected Status: True, Received Status:", status)

    # Broken event that will cause processing error
    status = send_event({
        "event": "button_click",
        "details": None,
        "session_id": "session-err",
        "timestamp": 2
    })
    print(f"Expected Status: False, Received Status:", status)

    # Another valid event
    status = send_event({
        "event": "button_click",
        "details": {"text": "ok2"},
        "session_id": "session-2",
        "timestamp": 3
    })
    print(f"Expected Status: True, Received Status:", status)

    time.sleep(1)

    print(
        "Verify consumer logs:\n"
        " - Should be one error log for bad event in between the two \"OK\" events:\n"
        "   -  \"Error processing event: 'NoneType' object has no attribute 'get'\"\n"
        "   -  \"500 Internal Server Error\"\n"
    )

    print("Test 2 complete.\n")

def test_producer_retry():
    print("\nTest: Producer Retry Logic...\n")

    print("Pausing all Kafka brokers...")
    os.system("docker pause kafka-1")
    os.system("docker pause kafka-2")
    os.system("docker pause kafka-3")
    time.sleep(3)

    print("Sending event while brokers are paused...")
    status = send_event({
        "event": "button_click",
        "details": {"text": "retry-test"},
        "session_id": "session-retry",
        "timestamp": int(time.time() * 1000)
    }, return_status=True)

    print("Status during outage:", status)

    print("Unpausing brokers...")
    os.system("docker unpause kafka-1")
    os.system("docker unpause kafka-2")
    os.system("docker unpause kafka-3")

    print("Waiting for cluster to recover...")
    time.sleep(10)

    print("Sending event after recovery...")
    status2 = send_event({
        "event": "button_click",
        "details": {"text": "retry-test-2"},
        "session_id": "session-retry",
        "timestamp": int(time.time() * 1000)
    }, return_status=True)

    print("Status after recovery:", status2)


def main():
    print("\nSecurity Testing Menu")
    print("----------------------")
    print("1 = Invalid Event Handling")
    print("2 = Consumer Error Recovery")
    print("3 = Producer Retry Logic")
    print("q = Quit\n")

    choice = input("Select test: ").strip().lower()

    if choice == "1":
        test_invalid_event_handling()
    elif choice == "2":
        test_consumer_error_recovery()
    elif choice == "3":
        test_producer_retry()
    else:
        print("Exiting.\n")


if __name__ == "__main__":
    main()

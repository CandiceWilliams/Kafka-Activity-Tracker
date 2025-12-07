"""
Activity Generator script to help with testing
"""

import urllib.request
import urllib.error
import json
import time
import random

URL = "http://127.0.0.1:8000/event"

# CONFIGURATION
TOTAL_EVENTS = 2000
EVENTS_PER_SECOND = 500
USER_COUNT = 15
TIMEOUT = 3

def generate_event():
    """Create a random event """
    event_type = random.choice([
        "button_click",
        "slider_input",
        "toggle_switch",
        "dropdown_selection",
        "text_input"
    ])

    session_id = f"session-{random.randint(1, USER_COUNT)}"
    timestamp = int(time.time() * 1000)

    if event_type == "button_click":
        details = {"text": random.choice(["Click Me", "Click", "Don't Click?"])}

    elif event_type == "slider_input":
        details = {"value": random.randint(0, 100)}

    elif event_type == "toggle_switch":
        details = {"checked": random.choice([True, False])}

    elif event_type == "dropdown_selection":
        details = {"value": random.choice(["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"])}

    elif event_type == "text_input":
        details = {"value": random.choice(["hello", "search", "test"])}

    return {
        "event": event_type,
        "details": details,
        "session_id": session_id,
        "timestamp": timestamp
    }


def send_event(data, return_status = False):
    """Send a single event to FastAPI"""
    try:
        req = urllib.request.Request(
            URL,
            data=json.dumps(data).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        resp = urllib.request.urlopen(req, timeout=TIMEOUT)

        if return_status:
            return resp.getcode()

        return True

    except urllib.error.HTTPError as e:
        if return_status:
            return e.code
        return False

    except Exception(BaseException):
        if return_status:
            return None
        return False


def run(iterations=TOTAL_EVENTS):
    interval = 1 / EVENTS_PER_SECOND

    for i in range(iterations):
        data = generate_event()
        send_event(data)

        time.sleep(interval)

    print("Finished sending events!")


if __name__ == "__main__":
    run()
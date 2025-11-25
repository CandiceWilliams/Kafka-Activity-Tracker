from kafka import KafkaConsumer
import json
import time

from app.api import activities as a

def process_event(event):
    event_type = event.get("event")
    details = event.get("details", {})
    timestamp = event.get("timestamp", time.time())

    seconds_bucket = int(timestamp // 1000)

    a.events_per_type[event_type] += 1
    a.events_per_second[seconds_bucket] += 1

    # Process each known event type
    if event_type == "page_load":
        a.page_loads += 1

    elif event_type == "button_click":
        text = details.get("text")
        a.button_clicks[text] += 1

    elif event_type == "slider_input":
        value = details.get("value")
        a.slider_inputs.append(value)

    elif event_type == "dropdown_selection":
        value = details.get("value", "unknown")
        a.dropdown_selections[value] += 1

    elif event_type == "text_input":
        a.text_input_updates += 1

    elif event_type == "toggle_switch":
        checked = details.get("checked", False)
        a.toggle_switch_counts["on" if checked else "off"] += 1


def create_consumer():
    return KafkaConsumer(
        "events",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="activity-tracker",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
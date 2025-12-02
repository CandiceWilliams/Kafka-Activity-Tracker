"""
Analytics service for processing and storing activity metrics.
"""

from collections import Counter
import logging

logger = logging.getLogger(__name__)

# In-memory analytics state
analytics_state = {
    "page_loads": 0,
    "button_clicks": Counter(),
    "slider_inputs": [],
    "dropdown_selections": Counter(),
    "text_inputs": 0,
    "toggle_switches": {"on": 0, "off": 0},
    "events_per_type": Counter(),
    "events_per_second": Counter(),
}


def process_analytics_event(event_data, key):
    """
    Process an event for analytics aggregation.

    Args:
        event_data: Dictionary containing event information
        key: Message key (usually session_id)
    """
    event_type = event_data.get("event_type")

    # Update event type counter
    analytics_state["events_per_type"][event_type] = (
        analytics_state["events_per_type"].get(event_type, 0) + 1
    )

    # Process specific event types
    if event_type == "page_load":
        analytics_state["page_loads"] += 1

    elif event_type == "button_click":
        text = event_data.get("button_text", "Unknown")
        analytics_state["button_clicks"][text] += 1

    elif event_type == "slider_input":
        value = event_data.get("value")
        analytics_state["slider_inputs"].append(value)

    elif event_type == "dropdown_selection":
        value = event_data.get("selected_value", "unknown")
        analytics_state["dropdown_selections"][value] += 1

    elif event_type == "text_input":
        analytics_state["text_inputs"] += 1

    elif event_type == "toggle_switch":
        is_enabled = event_data.get("is_enabled", False)
        if is_enabled:
            analytics_state["toggle_switches"]["on"] += 1
        else:
            analytics_state["toggle_switches"]["off"] += 1

    logger.debug(f"Processed {event_type} event")


def update_dashboard(event_data, key):
    """
    Process analytics results for dashboard updates.

    Args:
        event_data: Analytics result data
        key: Message key
    """
    # For now, just log that we received analytics results
    logger.debug(f"Dashboard update received: {event_data}")


def get_analytics_state():
    """
    Get current analytics state for API responses.

    Returns:
        Dictionary containing current analytics metrics
    """
    return {
        "page_loads": analytics_state["page_loads"],
        "button_clicks": dict(analytics_state["button_clicks"]),
        "slider_inputs": analytics_state["slider_inputs"][-100:],  # Last 100 values
        "dropdown_selections": dict(analytics_state["dropdown_selections"]),
        "text_inputs": analytics_state["text_inputs"],
        "toggle_switches": analytics_state["toggle_switches"].copy(),
        "events_per_type": dict(analytics_state["events_per_type"]),
        "events_per_second": dict(analytics_state["events_per_second"]),
    }

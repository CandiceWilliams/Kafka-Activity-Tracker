"""
Analytics service using ksqlDB for stream processing.

All windowing, aggregations, and state management is handled by ksqlDB.
This service just queries ksqlDB results via REST API.
"""

import logging

logger = logging.getLogger(__name__)


# In-memory state for basic tracking (optional - most logic is in ksqlDB)
_analytics_state = {
    "events_per_type": {},
    "button_clicks": {},
    "total_events": 0,
}


def process_analytics_event(event_data, key):
    """
    Process event from Kafka consumer.

    All advanced stream processing is handled by ksqlDB,
    so this just does basic in-memory tracking for the /analytics endpoint.
    """
    global _analytics_state

    try:
        event_type = event_data.get("event_type")

        # Track event type counts
        if event_type:
            _analytics_state["events_per_type"][event_type] = (
                _analytics_state["events_per_type"].get(event_type, 0) + 1
            )

        # Track button clicks specifically
        if event_type == "button_click":
            button_text = event_data.get("button_text", "Unknown")
            _analytics_state["button_clicks"][button_text] = (
                _analytics_state["button_clicks"].get(button_text, 0) + 1
            )

        # Increment total
        _analytics_state["total_events"] += 1

        logger.debug(
            f"Processed {event_type} event (total: {_analytics_state['total_events']})"
        )

    except Exception as e:
        logger.error(f"Error processing analytics event: {e}")


def update_dashboard(event_data, key):
    """
    Process events from analytics-results topic for dashboard updates.

    This function is called by the dashboard consumer.
    Currently, dashboard updates are handled via WebSocket in main.py,
    so this can be a no-op or do additional processing if needed.
    """
    logger.debug(
        f"Dashboard update received: {event_data.get('metric_name', 'unknown')}"
    )
    # Dashboard updates are handled by WebSocket broadcaster in main.py
    # This is here for the consumer, but actual updates go through WebSocket


def get_analytics_state():
    """
    Get current analytics state.

    Returns both in-memory stats and ksqlDB results.
    """
    from app.services.ksqldb_client import get_ksqldb_client

    try:
        client = get_ksqldb_client()

        # Query ksqlDB for aggregated results
        popular_buttons = client.get_popular_buttons(limit=10)
        activity_summary = client.get_activity_summary()

        return {
            "stream_processing": "Powered by ksqlDB",
            "events_per_type": _analytics_state["events_per_type"],
            "button_clicks": _analytics_state["button_clicks"],
            "total_events": _analytics_state["total_events"],
            "ksqldb": {
                "popular_buttons": popular_buttons,
                "activity_summary": activity_summary,
            },
        }
    except Exception as e:
        logger.error(f"Error querying ksqlDB: {e}")
        return {
            "stream_processing": "ksqlDB unavailable",
            "events_per_type": _analytics_state["events_per_type"],
            "button_clicks": _analytics_state["button_clicks"],
            "total_events": _analytics_state["total_events"],
            "error": str(e),
        }

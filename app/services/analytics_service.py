"""
Analytics service using ksqlDB for stream processing.

All windowing, aggregations, and state management is handled by ksqlDB.
This service just queries ksqlDB results via REST API.
"""

import logging

logger = logging.getLogger(__name__)


# Keep your existing simple event processing
def process_analytics_event(event_data, key):
    """
    Process event (keep your existing simple logic).

    All advanced stream processing is handled by ksqlDB,
    so this just needs basic tracking.
    """
    logger.debug(f"Processed {event_data.get('event_type')} event")


def get_analytics_state():
    """
    Get analytics by querying ksqlDB.

    All aggregations are done by ksqlDB, we just query results.
    """
    from app.services.ksqldb_client import get_ksqldb_client

    try:
        client = get_ksqldb_client()

        # Query ksqlDB for aggregated results
        popular_buttons = client.get_popular_buttons(limit=10)
        activity_summary = client.get_activity_summary()

        return {
            "stream_processing": "Powered by ksqlDB",
            "popular_buttons": popular_buttons,
            "activity_summary": activity_summary,
        }
    except Exception as e:
        logger.error(f"Error querying ksqlDB: {e}")
        return {"error": "ksqlDB not available", "message": str(e)}

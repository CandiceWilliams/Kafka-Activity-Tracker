"""
ksqlDB client for SQL-like stream processing.

This module demonstrates ksqlDB capabilities:
- Creating streams and tables from Kafka topics
- SQL-like queries on streaming data
- Materialized views
- Continuous queries
"""

import requests
import logging
from typing import Dict, List, Any, Optional
from app.config import settings

logger = logging.getLogger(__name__)


class KsqlDBClient:
    """
    Client for interacting with ksqlDB Server.

    ksqlDB allows you to process Kafka streams using SQL syntax,
    making it easier to perform transformations and aggregations.
    """

    def __init__(self, server_url: str = None):
        """Initialize ksqlDB client."""
        self.server_url = server_url or settings.KSQLDB_SERVER_URL
        self.headers = {"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"}
        logger.info(f"ksqlDB client initialized for {self.server_url}")

    def execute_statement(self, sql: str) -> Dict[str, Any]:
        """
        Execute a ksqlDB statement.

        Args:
            sql: ksqlDB SQL statement

        Returns:
            Response from ksqlDB server
        """
        endpoint = f"{self.server_url}/ksql"
        payload = {"ksql": sql, "streamsProperties": {}}

        try:
            response = requests.post(endpoint, json=payload, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to execute ksqlDB statement: {e}")
            # Log response content for debugging
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_detail = e.response.json()
                    logger.error(f"ksqlDB error response: {error_detail}")
                except:
                    logger.error(f"ksqlDB error text: {e.response.text}")
            raise

    def query(self, sql: str, timeout: int = 30) -> List[Dict[str, Any]]:
        """
        Execute a query and return results.

        Args:
            sql: ksqlDB SELECT query
            timeout: Query timeout in seconds

        Returns:
            List of result rows
        """
        endpoint = f"{self.server_url}/query"
        payload = {
            "ksql": sql,
            "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
        }

        try:
            response = requests.post(
                endpoint,
                json=payload,
                headers=self.headers,
                timeout=timeout,
                stream=True,
            )
            response.raise_for_status()

            # Parse streaming response
            results = []
            for line in response.iter_lines():
                if line:
                    results.append(line.decode("utf-8"))

            return results
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to execute ksqlDB query: {e}")
            raise

    def setup_activity_streams(self):
        """
        Set up ksqlDB streams for activity tracking.

        This creates:
        - Streams for each event type
        - Aggregation tables
        - Materialized views
        """
        logger.info("Setting up ksqlDB streams...")

        # Create stream for page views
        # Note: timestamp is BIGINT (milliseconds since epoch)
        page_views_stream = """
        CREATE STREAM IF NOT EXISTS page_views_stream (
            event_type VARCHAR,
            timestamp BIGINT,
            session_id VARCHAR,
            user_id VARCHAR,
            url VARCHAR
        ) WITH (
            KAFKA_TOPIC='page-views',
            VALUE_FORMAT='JSON'
        );
        """

        # Create stream for button clicks
        button_clicks_stream = """
        CREATE STREAM IF NOT EXISTS button_clicks_stream (
            event_type VARCHAR,
            timestamp BIGINT,
            session_id VARCHAR,
            button_text VARCHAR,
            button_id VARCHAR
        ) WITH (
            KAFKA_TOPIC='button-clicks',
            VALUE_FORMAT='JSON'
        );
        """

        # Create stream for slider events
        slider_events_stream = """
        CREATE STREAM IF NOT EXISTS slider_events_stream (
            event_type VARCHAR,
            timestamp BIGINT,
            session_id VARCHAR,
            value INT,
            min_value INT,
            max_value INT
        ) WITH (
            KAFKA_TOPIC='slider-events',
            VALUE_FORMAT='JSON'
        );
        """

        # Create aggregation table: button clicks per minute
        button_clicks_per_minute = """
        CREATE TABLE IF NOT EXISTS button_clicks_per_minute AS
        SELECT
            button_text,
            COUNT(*) AS click_count
        FROM button_clicks_stream
        WINDOW TUMBLING (SIZE 1 MINUTE)
        GROUP BY button_text
        EMIT CHANGES;
        """

        # Create aggregation table: page views per minute
        # Note: When using GROUP BY with a constant, must include it in SELECT
        page_views_per_minute = """
        CREATE TABLE IF NOT EXISTS page_views_per_minute AS
        SELECT
            'all' AS grouping_key,
            COUNT(*) AS view_count
        FROM page_views_stream
        WINDOW TUMBLING (SIZE 1 MINUTE)
        GROUP BY 'all'
        EMIT CHANGES;
        """

        # Create aggregation table: average slider value per minute
        # Note: When using GROUP BY with a constant, must include it in SELECT
        slider_avg_per_minute = """
        CREATE TABLE IF NOT EXISTS slider_avg_per_minute AS
        SELECT
            'all' AS grouping_key,
            AVG(value) AS avg_value,
            MIN(value) AS min_value,
            MAX(value) AS max_value
        FROM slider_events_stream
        WINDOW TUMBLING (SIZE 1 MINUTE)
        GROUP BY 'all'
        EMIT CHANGES;
        """

        # Create session-based aggregation
        # Note: Simplified to avoid complex COLLECT_LIST which can cause issues
        session_activity = """
        CREATE TABLE IF NOT EXISTS session_activity AS
        SELECT
            session_id,
            COUNT(*) AS total_events
        FROM page_views_stream
        WINDOW SESSION (30 MINUTES)
        GROUP BY session_id
        EMIT CHANGES;
        """

        # Execute all statements
        statements = [
            ("Page Views Stream", page_views_stream),
            ("Button Clicks Stream", button_clicks_stream),
            ("Slider Events Stream", slider_events_stream),
            ("Button Clicks Per Minute", button_clicks_per_minute),
            ("Page Views Per Minute", page_views_per_minute),
            ("Slider Average Per Minute", slider_avg_per_minute),
            ("Session Activity", session_activity),
        ]

        for name, statement in statements:
            try:
                result = self.execute_statement(statement)
                logger.info(f"Created: {name}")
            except Exception as e:
                error_msg = str(e).lower()
                # Stream might already exist
                if "already exists" in error_msg or "duplicate" in error_msg:
                    logger.info(f"{name} already exists, skipping")
                else:
                    logger.error(f"Failed to create {name}: {e}")
                    # Try to get more details from ksqlDB response
                    try:
                        response_text = (
                            e.response.text if hasattr(e, "response") else str(e)
                        )
                        logger.error(f"ksqlDB error details: {response_text}")
                    except:
                        pass

    def get_popular_buttons(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Query the most clicked buttons using a pull query.

        Pull queries return immediately with current state, no waiting.

        Args:
            limit: Number of results to return

        Returns:
            List of button click counts
        """
        # Use pull query (no EMIT CHANGES) for immediate results
        query = f"""
        SELECT button_text, SUM(click_count) as total_clicks
        FROM button_clicks_per_minute
        GROUP BY button_text
        LIMIT {limit};
        """

        try:
            # Use execute_statement for pull queries, not query()
            result = self.execute_statement(query)

            # Parse results from pull query format
            if result and len(result) > 0:
                # Pull queries return data directly
                return result
            return []
        except Exception as e:
            logger.debug(f"No button data yet: {e}")
            return []  # Return empty list if no data

    def get_activity_summary(self) -> Dict[str, Any]:
        """
        Get a summary of current activity.

        Returns:
            Dictionary with activity metrics
        """
        try:
            # Get latest page view count with shorter timeout
            page_views_query = """
            SELECT grouping_key, view_count
            FROM page_views_per_minute
            EMIT CHANGES
            LIMIT 1;
            """
            page_views = self.query(page_views_query, timeout=2)

            # Get latest slider average with shorter timeout
            slider_query = """
            SELECT grouping_key, avg_value, min_value, max_value
            FROM slider_avg_per_minute
            EMIT CHANGES
            LIMIT 1;
            """
            slider_avg = self.query(slider_query, timeout=2)

            return {
                "page_views": page_views,
                "slider_average": slider_avg,
            }
        except Exception as e:
            logger.debug(f"No activity data yet: {e}")
            return {"page_views": [], "slider_average": []}

    def list_streams(self) -> List[str]:
        """List all ksqlDB streams."""
        try:
            result = self.execute_statement("SHOW STREAMS;")
            # ksqlDB returns list with one element containing 'streams' key
            if result and len(result) > 0 and "streams" in result[0]:
                return [stream["name"] for stream in result[0]["streams"]]
            return []
        except Exception as e:
            logger.error(f"Failed to list streams: {e}")
            return []

    def list_tables(self) -> List[str]:
        """List all ksqlDB tables."""
        try:
            result = self.execute_statement("SHOW TABLES;")
            # ksqlDB returns list with one element containing 'tables' key
            if result and len(result) > 0 and "tables" in result[0]:
                return [table["name"] for table in result[0]["tables"]]
            return []
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []

    def terminate_query(self, query_id: str):
        """
        Terminate a running query.

        Args:
            query_id: ID of the query to terminate
        """
        try:
            self.execute_statement(f"TERMINATE {query_id};")
            logger.info(f"Terminated query {query_id}")
        except Exception as e:
            logger.error(f"Failed to terminate query {query_id}: {e}")


# Global ksqlDB client instance
_ksqldb_client = None


def get_ksqldb_client() -> KsqlDBClient:
    """Get or create global ksqlDB client instance."""
    global _ksqldb_client
    if _ksqldb_client is None:
        _ksqldb_client = KsqlDBClient()
    return _ksqldb_client

"""
FastAPI application with ksqlDB integration and real-time dashboard.

This version integrates ksqlDB for stream processing and provides
WebSocket updates for live dashboard visualization.
"""

import threading
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.kafka.producer import get_producer
from app.kafka.consumer import create_analytics_consumer, create_dashboard_consumer
from app.models.events import (
    PageLoadEvent,
    ButtonClickEvent,
    SliderInputEvent,
    DropdownSelectionEvent,
    TextInputEvent,
    ToggleSwitchEvent,
)
from app.services.analytics_service import get_analytics_state

# Setup logging
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# WebSocket connection manager for real-time updates
class ConnectionManager:
    """Manages WebSocket connections for real-time dashboard updates."""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients."""
        for connection in self.active_connections[
            :
        ]:  # Copy to avoid modification during iteration
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to WebSocket: {e}")
                self.active_connections.remove(connection)


manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    # Startup
    logger.info("Starting application...")

    # Start Kafka consumers in background threads
    analytics_consumer = create_analytics_consumer()
    dashboard_consumer = create_dashboard_consumer()

    analytics_thread = threading.Thread(
        target=analytics_consumer.start, daemon=True, name="analytics-consumer"
    )
    dashboard_thread = threading.Thread(
        target=dashboard_consumer.start, daemon=True, name="dashboard-consumer"
    )

    analytics_thread.start()
    dashboard_thread.start()
    logger.info("Consumers started")

    # Initialize producer
    producer = get_producer()
    logger.info("Producer initialized")

    # Start background task to broadcast updates
    update_task = asyncio.create_task(broadcast_updates())
    logger.info("Update broadcaster started")

    yield  # Application runs

    # Shutdown
    logger.info("Shutting down application...")
    update_task.cancel()
    analytics_consumer.close()
    dashboard_consumer.close()
    producer.flush()
    producer.close()
    logger.info("Application shutdown complete")


# Background task to periodically broadcast updates
async def broadcast_updates():
    """Periodically fetch data from ksqlDB and broadcast to clients."""
    from app.services.ksqldb_client import get_ksqldb_client

    while True:
        try:
            if manager.active_connections:
                # Get analytics state
                analytics = get_analytics_state()

                # Try to get ksqlDB data
                try:
                    client = get_ksqldb_client()

                    # Get popular buttons from ksqlDB
                    popular_buttons = client.get_popular_buttons(limit=5)

                    # Get activity summary
                    activity_summary = client.get_activity_summary()

                    # Combine data
                    update = {
                        "type": "update",
                        "timestamp": int(asyncio.get_event_loop().time() * 1000),
                        "analytics": analytics,
                        "ksqldb": {
                            "popular_buttons": popular_buttons,
                            "activity_summary": activity_summary,
                        },
                    }
                except Exception as e:
                    logger.warning(f"ksqlDB not available: {e}")
                    update = {
                        "type": "update",
                        "timestamp": int(asyncio.get_event_loop().time() * 1000),
                        "analytics": analytics,
                        "ksqldb": None,
                    }

                # Broadcast to all clients
                await manager.broadcast(update)

            # Wait before next update
            await asyncio.sleep(2)  # Update every 2 seconds

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in broadcast_updates: {e}")
            await asyncio.sleep(2)


# Create FastAPI app
app = FastAPI(
    title="Kafka Activity Tracker",
    description="Real-time activity tracking using Apache Kafka and ksqlDB",
    version="2.0.0",
    lifespan=lifespan,
)

# Setup templates
templates = Jinja2Templates(directory="app/templates")


# ============================================================================
# Web Pages
# ============================================================================


@app.get("/")
def home(request: Request):
    """Render main activity tracking page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/dashboard")
def dashboard(request: Request):
    """Render real-time analytics dashboard with Plotly.js visualization."""
    return templates.TemplateResponse("dashboard_plotly.html", {"request": request})


# ============================================================================
# Event Processing Endpoints
# ============================================================================


@app.post("/event")
async def receive_event(request: Request):
    """
    Generic event endpoint for all event types.
    Routes to appropriate topic based on event type.
    """
    try:
        data = await request.json()
        event_type = data.get("event")

        producer = get_producer()

        # Route based on event type
        if event_type == "page_load":
            event = PageLoadEvent(
                url=data.get("url", "/"),
                session_id=data.get("session_id"),
            )
        elif event_type == "button_click":
            event = ButtonClickEvent(
                button_text=data.get("details", {}).get("text", "Unknown"),
                session_id=data.get("session_id"),
            )
        elif event_type == "slider_input":
            event = SliderInputEvent(
                value=int(data.get("details", {}).get("value", 50)),
                session_id=data.get("session_id"),
            )
        elif event_type == "dropdown_selection":
            event = DropdownSelectionEvent(
                selected_value=data.get("details", {}).get("value", ""),
                session_id=data.get("session_id"),
            )
        elif event_type == "text_input":
            event = TextInputEvent(
                input_value=data.get("details", {}).get("value", ""),
                session_id=data.get("session_id"),
            )
        elif event_type == "toggle_switch":
            event = ToggleSwitchEvent(
                is_enabled=data.get("details", {}).get("checked", False),
                session_id=data.get("session_id"),
            )
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": f"Unknown event type: {event_type}",
                },
            )

        success = producer.send_event(event)

        if success:
            return {"status": "success", "event_type": event_type}
        else:
            return JSONResponse(
                status_code=500,
                content={"status": "error", "message": "Failed to send event"},
            )
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return JSONResponse(
            status_code=500, content={"status": "error", "message": str(e)}
        )


# ============================================================================
# Analytics Endpoints
# ============================================================================


@app.get("/analytics")
def get_analytics():
    """Get current analytics state (basic aggregations)."""
    return get_analytics_state()


# ============================================================================
# ksqlDB Integration Endpoints
# ============================================================================


@app.get("/ksql/streams")
def list_ksql_streams():
    """List all ksqlDB streams."""
    from app.services.ksqldb_client import get_ksqldb_client

    try:
        client = get_ksqldb_client()
        streams = client.list_streams()
        return {"streams": streams, "count": len(streams)}
    except Exception as e:
        logger.error(f"Error listing streams: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to list streams", "message": str(e)},
        )


@app.get("/ksql/tables")
def list_ksql_tables():
    """List all ksqlDB tables."""
    from app.services.ksqldb_client import get_ksqldb_client

    try:
        client = get_ksqldb_client()
        tables = client.list_tables()
        return {"tables": tables, "count": len(tables)}
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to list tables", "message": str(e)},
        )


@app.get("/ksql/popular-buttons")
def get_popular_buttons(limit: int = 10):
    """
    Get most clicked buttons from ksqlDB.

    This queries the button_clicks_per_minute table and aggregates
    the results to show the most popular buttons.
    """
    from app.services.ksqldb_client import get_ksqldb_client

    try:
        client = get_ksqldb_client()
        buttons = client.get_popular_buttons(limit=limit)
        return {"popular_buttons": buttons, "source": "ksqlDB"}
    except Exception as e:
        logger.error(f"Error getting popular buttons: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to query ksqlDB", "message": str(e)},
        )


@app.get("/ksql/activity-summary")
def get_ksql_activity_summary():
    """
    Get activity summary from ksqlDB.

    Returns aggregated metrics from ksqlDB tables including:
    - Page views per minute
    - Average slider values
    - Other windowed aggregations
    """
    from app.services.ksqldb_client import get_ksqldb_client

    try:
        client = get_ksqldb_client()
        summary = client.get_activity_summary()
        return {"activity_summary": summary, "source": "ksqlDB"}
    except Exception as e:
        logger.error(f"Error getting activity summary: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to query ksqlDB", "message": str(e)},
        )


@app.get("/ksql/metrics")
def get_all_ksql_metrics():
    """
    Get all available ksqlDB metrics in one call.

    This is useful for the dashboard to get all data at once.
    """
    from app.services.ksqldb_client import get_ksqldb_client

    try:
        client = get_ksqldb_client()

        return {
            "timestamp": int(asyncio.get_event_loop().time() * 1000),
            "streams": client.list_streams(),
            "tables": client.list_tables(),
            "popular_buttons": client.get_popular_buttons(limit=10),
            "activity_summary": client.get_activity_summary(),
            "source": "ksqlDB",
        }
    except Exception as e:
        logger.error(f"Error getting ksqlDB metrics: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to query ksqlDB", "message": str(e)},
        )


# ============================================================================
# WebSocket Endpoint for Real-Time Updates
# ============================================================================


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time dashboard updates.

    Clients connect here to receive live updates of analytics data.
    Updates are broadcast every 2 seconds from the background task.
    """
    await manager.connect(websocket)

    try:
        # Send initial data
        initial_data = {
            "type": "connected",
            "message": "Connected to real-time updates",
        }
        await websocket.send_json(initial_data)

        # Keep connection alive and listen for messages
        while True:
            try:
                # Wait for client messages (e.g., commands)
                data = await websocket.receive_text()
                logger.debug(f"Received from client: {data}")
            except WebSocketDisconnect:
                break

    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        manager.disconnect(websocket)


# ============================================================================
# System Endpoints
# ============================================================================


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": "2.0.0",
        "features": ["kafka", "ksqldb", "websocket", "plotly"],
    }


@app.get("/metrics")
def get_system_metrics():
    """
    Get system metrics for monitoring.

    Useful for:
    - Debugging performance issues
    - Monitoring system health
    - Understanding Kafka behavior
    """
    producer = get_producer()
    return {
        "producer_metrics": producer.get_metrics(),
        "websocket_connections": len(manager.active_connections),
    }

"""
FastAPI application with improved lifecycle management.
"""

import threading
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse

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
from app.api.websocket import ConnectionManager

# Setup logging
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# WebSocket connection manager
manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    # Startup
    logger.info("Starting application...")

    # Start consumers in background threads
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

    yield  # Application runs

    # Shutdown
    logger.info("Shutting down application...")
    analytics_consumer.close()
    dashboard_consumer.close()
    producer.flush()
    producer.close()
    logger.info("Application shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Kafka Activity Tracker",
    description="Real-time activity tracking using Apache Kafka",
    version="1.0.0",
    lifespan=lifespan,
)

# Setup templates
templates = Jinja2Templates(directory="app/templates")


@app.get("/")
def home(request: Request):
    """Render main activity tracking page."""
    return templates.TemplateResponse("index.html", {"request": request})


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


@app.get("/analytics")
def get_analytics():
    """Get current analytics state."""
    return get_analytics_state()


@app.get("/dashboard")
def dashboard(request: Request):
    """Render analytics dashboard."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/metrics")
def get_metrics():
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
        # Could add consumer lag, partition info, etc.
    }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

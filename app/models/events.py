"""
Pydantic models for event validation and serialization.
These models ensure data consistency across the application.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
from enum import Enum


class EventType(str, Enum):
    """Enumeration of supported event types."""

    PAGE_LOAD = "page_load"
    BUTTON_CLICK = "button_click"
    SLIDER_INPUT = "slider_input"
    DROPDOWN_SELECTION = "dropdown_selection"
    TEXT_INPUT = "text_input"
    TOGGLE_SWITCH = "toggle_switch"


class BaseEvent(BaseModel):
    """Base event model with common fields."""

    event_type: EventType
    timestamp: int = Field(
        default_factory=lambda: int(datetime.now().timestamp() * 1000)
    )
    session_id: Optional[str] = None
    user_id: Optional[str] = None

    class Config:
        use_enum_values = True


class PageLoadEvent(BaseEvent):
    """Event triggered when a page loads."""

    event_type: EventType = EventType.PAGE_LOAD
    url: str
    referrer: Optional[str] = None
    user_agent: Optional[str] = None


class ButtonClickEvent(BaseEvent):
    """Event triggered when a button is clicked."""

    event_type: EventType = EventType.BUTTON_CLICK
    button_text: str
    button_id: Optional[str] = None
    x_coordinate: Optional[int] = None
    y_coordinate: Optional[int] = None


class SliderInputEvent(BaseEvent):
    """Event triggered when a slider value changes."""

    event_type: EventType = EventType.SLIDER_INPUT
    value: int
    min_value: int = 0
    max_value: int = 100

    @field_validator("value")
    @classmethod
    def validate_value_range(cls, v, info):
        """Ensure slider value is within range."""
        min_val = info.data.get("min_value", 0)
        max_val = info.data.get("max_value", 100)
        if not min_val <= v <= max_val:
            raise ValueError(f"Value {v} not in range [{min_val}, {max_val}]")
        return v


class DropdownSelectionEvent(BaseEvent):
    """Event triggered when a dropdown selection changes."""

    event_type: EventType = EventType.DROPDOWN_SELECTION
    selected_value: str
    dropdown_id: Optional[str] = None


class TextInputEvent(BaseEvent):
    """Event triggered when text input changes."""

    event_type: EventType = EventType.TEXT_INPUT
    input_value: str
    input_id: Optional[str] = None
    input_length: int = 0

    @field_validator("input_length", mode="before")
    @classmethod
    def calculate_length(cls, v, info):
        """Calculate input length if not provided."""
        if v == 0 and "input_value" in info.data:
            return len(info.data["input_value"])
        return v


class ToggleSwitchEvent(BaseEvent):
    """Event triggered when a toggle switch changes."""

    event_type: EventType = EventType.TOGGLE_SWITCH
    is_enabled: bool
    toggle_id: Optional[str] = None


class UserSessionEvent(BaseEvent):
    """Aggregated session data."""

    event_type: EventType = Field(default="user_session")
    session_start: int
    session_end: Optional[int] = None
    total_events: int = 0
    event_counts: Dict[str, int] = Field(default_factory=dict)


class AnalyticsResult(BaseModel):
    """Aggregated analytics result."""

    metric_name: str
    metric_value: float
    window_start: int
    window_end: int
    additional_data: Dict[str, Any] = Field(default_factory=dict)

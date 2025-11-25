import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

from app.api.kafka_producer import create_producer
from app.api.kafka_consumer import create_consumer, process_event
from app.api import activities as a

@asynccontextmanager
async def lifespan(app: FastAPI):
    def run_consumer():
        consumer = create_consumer()
        print("Kafka Consumer connected")
        for msg in consumer:
            process_event(msg.value)
    thread = threading.Thread(target=run_consumer, daemon=True)
    thread.start()
    yield
app = FastAPI(lifespan=lifespan)

templates = Jinja2Templates(directory="app/templates")

producer = create_producer()

@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/event")
async def receive_event(request: Request):
    data = await request.json()
    producer.send('events', value=data)
    return {"status": "Event received"}

@app.get("/activities")
def get_analytics():
    return {
        "page_loads": a.page_loads,
        "button_clicks": dict(a.button_clicks),
        "slider_inputs": a.slider_inputs,
        "dropdown_selections": dict(a.dropdown_selections),
        "text_input_updates": a.text_input_updates,
        "toggle_switch_counts": a.toggle_switch_counts,
        "events_per_type": dict(a.events_per_type),
        "events_per_second": dict(a.events_per_second),
    }

@app.get("/dashboard")
def activities_dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from api.kafka_producer import create_producer

app = FastAPI()

templates = Jinja2Templates(directory="app/templates")

producer = create_producer()

@app.get("/",  response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/event")
async def receive_event(request: Request):
    data = await request.json()
    producer.send('events', value=data)
    return {"status": "Event received"}
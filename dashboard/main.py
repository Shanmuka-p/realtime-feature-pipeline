import os
import json
import asyncio
import threading
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer
import httpx

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_FEATURE_STORE = os.getenv("TOPIC_FEATURE_STORE", "feature-store")

app = FastAPI()

# Store connected websocket clients
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except RuntimeError:
                pass # Client disconnected

manager = ConnectionManager()

# Kafka Consumer running in a separate thread
def consume_features():
    consumer = KafkaConsumer(
        TOPIC_FEATURE_STORE,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='dashboard-group'
    )
    
    # In a fully wired Flink setup, you might also consume a 'metrics' topic 
    # to get exact watermark lag and dropped late events. Here we pass the features.
    for message in consumer:
        feature_data = message.value
        # Calculate freshness based on the computed_at timestamp
        computed_at = datetime.fromisoformat(feature_data["computed_at"].replace('Z', '+00:00'))
        freshness_ms = (datetime.now(computed_at.tzinfo) - computed_at).total_seconds() * 1000
        feature_data["freshness_ms"] = round(freshness_ms, 2)
        
        # Broadcast to all connected clients
        asyncio.run(manager.broadcast(json.dumps(feature_data)))

FLINK_API_URL = "http://flink-jobmanager:8081"

async def fetch_flink_metrics():
    """Polls the Flink JobManager REST API for watermark and late event metrics."""
    async with httpx.AsyncClient() as client:
        while True:
            try:
                # 1. Get running job ID
                jobs_res = await client.get(f"{FLINK_API_URL}/jobs")
                jobs = jobs_res.json().get("jobs", [])
                running_job = next((j for j in jobs if j["status"] == "RUNNING"), None)
                
                if running_job:
                    job_id = running_job["id"]
                    
                    # 2. Get Job Vertices (Tasks)
                    job_detail = await client.get(f"{FLINK_API_URL}/jobs/{job_id}")
                    vertices = job_detail.json().get("vertices", [])
                    
                    if vertices:
                        # Grab the first vertex (usually the source/watermark generator)
                        v_id = vertices[0]["id"]
                        
                        # 3. Query specific metrics
                        metrics_res = await client.get(f"{FLINK_API_URL}/jobs/{job_id}/vertices/{v_id}/metrics?get=numLateRecordsDropped,currentLowWatermark")
                        metrics = metrics_res.json()
                        
                        late_dropped = next((m["value"] for m in metrics if m["id"] == "numLateRecordsDropped"), "0")
                        watermark = next((m["value"] for m in metrics if m["id"] == "currentLowWatermark"), None)
                        
                        lag_ms = "0"
                        if watermark and watermark != "NaN":
                            lag_ms = str(max(0, int(datetime.utcnow().timestamp() * 1000) - int(watermark)))
                        
                        # Broadcast operational metrics to UI
                        payload = json.dumps({
                            "type": "METRICS",
                            "late_dropped": late_dropped,
                            "watermark_lag_ms": lag_ms
                        })
                        await manager.broadcast(payload)
                        
            except Exception as e:
                print(f"Error fetching Flink metrics: {e}")
            
            await asyncio.sleep(2) # Poll every 2 seconds        

@app.on_event("startup")
async def startup_event():
    # Start Kafka consumer thread
    thread = threading.Thread(target=consume_features, daemon=True)
    thread.start()
    # Start Flink metrics polling
    asyncio.create_task(fetch_flink_metrics())

@app.get("/")
async def get():
    with open("index.html", "r") as f:
        html_content = f.read()
    return HTMLResponse(html_content)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection open
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
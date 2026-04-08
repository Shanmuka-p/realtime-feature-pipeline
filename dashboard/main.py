import os
import json
import asyncio
import threading
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer

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

@app.on_event("startup")
async def startup_event():
    # Start Kafka consumer thread
    thread = threading.Thread(target=consume_features, daemon=True)
    thread.start()

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
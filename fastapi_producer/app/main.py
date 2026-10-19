
import json
import os
from contextlib import asynccontextmanager
from uuid import uuid4

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC = "orders"


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    await producer.start()
    app.state.producer = producer

    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(title="Order Producer", lifespan=lifespan)


class OrderRequest(BaseModel):
    product: str
    quantity: int


@app.get("/health")
async def health():
    return {"service": "producer", "status": "ok"}


@app.post("/orders", status_code=202)
async def create_order(order: OrderRequest):
    event = {
        "event_id": str(uuid4()),
        "product": order.product,
        "quantity": order.quantity,
    }

    try:
        await app.state.producer.send_and_wait(TOPIC, event)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Could not publish order to Kafka",
        ) from exc

    return {
        "status": "accepted",
        "topic": TOPIC,
        "order": event,
    }

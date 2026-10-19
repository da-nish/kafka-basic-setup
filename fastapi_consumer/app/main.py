
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager, suppress

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC = "orders"


async def consume_messages(consumer: AIOKafkaConsumer):
    async for record in consumer:
        logger.info(
            "Received order: topic=%s partition=%s offset=%s value=%s",
            record.topic,
            record.partition,
            record.offset,
            record.value,
        )

        # Add your business logic here:
        # save the order to a database, send an email, etc.


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="order-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    await consumer.start()
    task = asyncio.create_task(consume_messages(consumer))

    app.state.consumer = consumer
    app.state.consumer_task = task

    logger.info("Consumer started; listening to topic '%s'", TOPIC)

    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await consumer.stop()


app = FastAPI(title="Order Consumer", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"service": "consumer", "status": "ok"}

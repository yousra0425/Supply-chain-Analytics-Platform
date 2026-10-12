
import json
from datetime import datetime, timezone
import os
from kafka import KafkaProducer


DLQ_TOPIC = "inventory.v2.dead-letter"
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)


def publish_dead_letter(message, error, attempts):
    """
    Store a failed Kafka message for later investigation.

    Returns only after Kafka acknowledges the write.
    """

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
            default=str,
        ).encode("utf-8"),
        acks="all",
        retries=3,
    )

    message_id = (
        f"{message.topic}:"
        f"{message.partition}:"
        f"{message.offset}"
    )

    payload = {
        "message_id": message_id,
        "original_topic": message.topic,
        "original_partition": message.partition,
        "original_offset": message.offset,
        "event": message.value,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "attempts": attempts,
        "failed_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    try:
        future = producer.send(
            DLQ_TOPIC,
            value=payload,
        )

        metadata = future.get(timeout=30)

        print(
            f"Dead-letter stored: {message_id} "
            f"(DLQ offset={metadata.offset})"
        )

        return metadata

    finally:
        producer.close(timeout=10)

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from src.common.settings import KAFKA_BOOTSTRAP, KAFKA_TOPIC_CLICKS


PAGES = ["/", "/docs", "/pricing", "/login", "/settings", "/search"]
COUNTRIES = ["SE", "NO", "FI", "DK", "DE", "NL", "US"]


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
    )

    print("Producer connected:", KAFKA_BOOTSTRAP, "topic:", KAFKA_TOPIC_CLICKS)

    try:
        while True:
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": random.randint(1, 2000),
                "page": random.choice(PAGES),
                "country": random.choice(COUNTRIES),
                "event_ts": iso_utc_now(),
            }
            producer.send(KAFKA_TOPIC_CLICKS, value=event)
            # small sleep to avoid overwhelming local machine
            time.sleep(0.05)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush(5)
        producer.close(5)


if __name__ == "__main__":
    main()

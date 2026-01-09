import json
import time
import uuid
from datetime import datetime, timezone
from random import choice, randint

from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "clicks"

p = Producer({"bootstrap.servers": BOOTSTRAP})

pages = ["/", "/pricing", "/signup", "/docs", "/blog", "/account"]
countries = ["SE", "NO", "DK", "FI", "DE", "NL"]

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed:", err)

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": randint(1, 200),
        "page": choice(pages),
        "country": choice(countries),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }

    key = str(event["user_id"]).encode("utf-8")
    value = json.dumps(event).encode("utf-8")

    p.produce(TOPIC, key=key, value=value, callback=delivery_report)
    p.poll(0)
    time.sleep(0.2)  # ~5 events/sec

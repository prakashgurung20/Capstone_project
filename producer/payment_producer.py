import json
import random
import time
import hashlib
import uuid
import os
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC_RAW        = "payments.raw"
TOPIC_DEADLETTER = "payments.deadletter"
MERCHANTS    = ["M98765", "M11223", "M44556"]
CURRENCIES   = ["USD", "EUR", "GBP", "NPR"]
MCC_CODES    = ["5411", "5812", "4814", "5912"]
CHANNELS     = ["POS", "ONLINE", "ATM", "MOBILE"]
LOCATIONS    = ["New York, USA", "London, UK", "Kathmandu, Nepal", "Tokyo, Japan"]
AUTH_RESULTS = ["APPROVED", "DECLINED", "APPROVED", "APPROVED"]

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            print(f"[Producer] Connecting to Kafka at {KAFKA_BOOTSTRAP} "
                  f"(attempt {attempt}/{retries})...")
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                api_version=(0, 10, 1),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                retry_backoff_ms=500,
            )
            print("[Producer] Connected successfully.")
            return p
        except NoBrokersAvailable:
            print(f"[Producer] Broker not ready — retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(
        f"[Producer] Could not connect to Kafka after {retries} attempts."
    )


def generate_payment() -> dict:
    fake_card = str(random.randint(1_000_000_000_000_000, 9_999_999_999_999_999))
    card_hash = hashlib.sha256(fake_card.encode()).hexdigest()[:16]

    amount = (
        round(random.uniform(10_001, 20_000), 2)
        if random.random() < 0.05
        else round(random.uniform(10, 1_000), 2)
    )

    return {
        "transaction_id": "TXN" + uuid.uuid4().hex[:12].upper(),
        "ts_event":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "card_hash":      card_hash,
        "merchant_id":    random.choice(MERCHANTS),
        "amount":         amount,
        "currency":       random.choice(CURRENCIES),
        "mcc":            random.choice(MCC_CODES),
        "channel":        random.choice(CHANNELS),
        "auth_result":    random.choice(AUTH_RESULTS),
        "location":       random.choice(LOCATIONS),
    }


if __name__ == "__main__":
    print("Starting producer\n")
    producer = create_producer()
    counts = 0

    try:
        while True:
            payment = generate_payment()
            print(f"[Producer] Sending → {payment}")

            future = producer.send(
                TOPIC_RAW,
                key=payment["card_hash"].encode(),
                value=payment,
            )
            record_metadata = future.get(timeout=10)
            counts += 1
            print(
                f"[Producer] ✓ topic={record_metadata.topic} "
                f"partition={record_metadata.partition} "
                f"offset={record_metadata.offset}"
            )

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n[Producer] Stopping...")

    except Exception as e:
        print(f"[Producer] Fatal error: {e}")
        try:
            producer.send(TOPIC_DEADLETTER, value={"error": str(e)})
        except Exception:
            pass

    finally:
        producer.flush()
        producer.close()
        print(f"[Producer] Total messages sent: {counts}")
import json
import random
import time
import hashlib
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC_RAW         = "payments.raw"
TOPIC_DEADLETTER  = "payments.deadletter"
MERCHANTS  = ["M98765", "M11223", "M44556", "M11111", "M22222"]
CURRENCIES = ["USD", "EUR", "GBP", "NPR"]
MCC_CODES  = ["5411", "5812", "4814", "5912"]
CHANNELS   = ["POS", "ONLINE", "ATM", "MOBILE"]
LOCATIONS  = ["New York, USA", "London, UK", "Kathmandu, Nepal", "Tokyo, Japan"]
AUTH_RESULTS =["APPROVED", "DECLINED", "APPROVED", "APPROVED"]

# HIGH_VALUE_THRESHOLD = 10000.00   


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
   
)
def generate_payment() -> dict:
    fake_card = str(random.randint(1_000_0000_0000_0000, 9_999_9999_9999_9999))
    card_hash = hashlib.sha256(fake_card.encode()).hexdigest()[:16]

    if random.random() < 0.05:
        amount = round(random.uniform(10_001, 20_000), 2)
    else:
        amount = round(random.uniform(10, 1_000), 2)

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


# def route_payment(payment: dict) -> tuple[str, str]:
 
#     if payment["auth_result"] == "DECLINED":
#         return TOPIC_DEADLETTER, "DECLINED transaction"

#     if payment["amount"] > HIGH_VALUE_THRESHOLD:
#         return TOPIC_DEADLETTER, f"High-value amount ({payment['amount']})"

#     return TOPIC_RAW, "valid"


if __name__ == "__main__":
    print("Starting producer \n")
    counts = 0

    try:
        while True:
            print("generating....")
            payment = generate_payment()
            print(f"data generated: {payment}")
            producer.send(TOPIC_RAW, value=payment)
            counts += 1

            time.sleep(0.5)
            # print(f"Data sent to {TOPIC_RAW} | ID: {payment['transaction_id']} | Total: {counts}")


            # print(f"data generated: {payment}")
            # topic, reason = payment
            # print(topic)
            # print("-----")
            # print(reason)
            # producer.send(topic, value=payment)
            # print("Data sent....clear")
            # producer.flush()

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.close()
        total = sum(counts.values())
        print(f"\nSummary — total: {total}  |  raw: {counts[TOPIC_RAW]}")
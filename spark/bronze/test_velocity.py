import json
import time
from kafka import KafkaProducer
from datetime import datetime, timezone

# --- CONFIG ---
KAFKA_SERVER = "localhost:9092"
TOPIC_RAW = "payments_raw"
TEST_CARD_HASH = "TEST_CARD_12345" # Using a fixed hash to trigger velocity

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_test_payment(index):
    payment = {
        "transaction_id": f"TEST-TXN-{index}",
        "ts_event": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "card_hash": TEST_CARD_HASH,
        "merchant_id": "M98765",
        "amount": 100.0,
        "currency": "NPR",
        "mcc": "5411",
        "channel": "POS",
        "auth_result": "APPROVED",
        "location": "Kathmandu, Nepal"
    }
    producer.send(TOPIC_RAW, value=payment)
    print(f"[{index}] Sent transaction to Kafka...")

if __name__ == "__main__":
    print(f"🚀 Starting Velocity Test for card: {TEST_CARD_HASH}")
    
    # Send 10 transactions very quickly (less than 1 minute)
    for i in range(1, 11):
        send_test_payment(i)
        time.sleep(1) # 1 second gap between each
        
    producer.flush()
    print("✅ Finished sending 10 transactions. Check your Spark logs!")
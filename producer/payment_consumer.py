import json
from kafka import KafkaConsumer

TOPICS = ["payments.deadletter"]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"Listening on: {TOPICS}\n")
print("-" * 60)

for msg in consumer:
    payment = msg.value
    label = "[RAW]        " if msg.topic == "payments.raw" else "[DEAD-LETTER]"
    print(f"{label}  {payment['transaction_id']}  |  "
          f"{payment['amount']} {payment['currency']}  |  "
          f"{payment['merchant_id']}  |  {payment['auth_result']}")
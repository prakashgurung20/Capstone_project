from kafka import KafkaProducer
import json

def main():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        # Send messages
        producer.send('orders', value={'order_id': 101, 'item': 'laptop', 'qty': 1})
        print("✅ Sent: order_id=101")

        producer.send('orders', value={'order_id': 102, 'item': 'mouse', 'qty': 2}, partition=0)
        print("✅ Sent: order_id=102 (partition 0)")

        # Ensure delivery
        producer.flush()
        print("✅ All messages flushed to Kafka")

    except Exception as e:
        print(f" Error: {e}")

    finally:
        producer.close()
        print("🔒 Producer closed")

if __name__ == "__main__":
    main()
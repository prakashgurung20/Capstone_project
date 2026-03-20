from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',                                          # topic name
    bootstrap_servers=['localhost:9092'],
    group_id='order-processing-group',                 # consumer group
    auto_offset_reset='earliest',                      # start from beginning if no offset
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in consumer:
    print(f"Partition: {message.partition}, Offset: {message.offset}")
    print(f"Value: {message.value}")
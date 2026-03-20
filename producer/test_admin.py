from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

new_topic = NewTopic(
    name='orders',
    num_partitions=3,       # split into 3 partitions for parallelism
    replication_factor=2    # each partition copied to 2 brokers
)

admin.create_topics(new_topics=[new_topic])
admin.close()
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, topic_names



admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"]
)

topics ={topic: NewTopic(name=topic, num_partitions=2, replication_factor=1) for topic in topic_names}

try:
    admin_client.create_topics(
        new_topics=[topic for _, topic in topics],
        validate_only=False
        )
except Exception as e:
    print(f"incorrect topic {str(e)}")

print(admin_client.list_topics())

admin_client.close()
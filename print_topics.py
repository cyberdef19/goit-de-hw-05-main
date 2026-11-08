from kafka.admin import KafkaAdminClient
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"]
)

print([topic for topic in admin_client.list_topics() if "oleksiy" in topic])

admin_client.close()
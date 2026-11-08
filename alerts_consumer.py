from kafka import KafkaConsumer
from configs import kafka_config
import json

topic_temp_name = "oleksiy_temperature_alerts"
topic_humidity_name = "oleksiy_humidity_alerts"

consumer_temp = KafkaConsumer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="group_temperature_alerts"
)

consumer_humidity = KafkaConsumer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],  
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="group_humidity_alerts"
)

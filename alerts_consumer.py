from kafka import KafkaConsumer, JsonSerializer
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
        value_deserializer=JsonSerializer(),
        key_deserializer=JsonSerializer(),
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
        value_deserializer=JsonSerializer(),
        key_deserializer=JsonSerializer(),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="group_humidity_alerts"
)

consumer_temp.subscribe([topic_temp_name])
consumer_humidity.subscribe([topic_humidity_name])

try:
    for message in consumer_temp:
        print(f"Temperature Alert: {message.value}")

    for message in consumer_humidity:
        print(f"Humidity Alert: {message.value}")
except KeyboardInterrupt:
    print("Consumer stopped.")
    
consumer_temp.close()
consumer_humidity.close()


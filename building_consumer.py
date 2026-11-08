from kafka import KafkaConsumer, KafkaProducer, JsonSerializer
from configs import kafka_config
import json

topic_name = "oleksiy_building_sensors"
consumer_building = KafkaConsumer(
      bootstrap_servers=kafka_config["bootstrap_servers"],
      security_protocol=kafka_config["security_protocol"],
      sasl_mechanism=kafka_config["sasl_mechanism"],
      sasl_plain_username=kafka_config["username"],
      sasl_plain_password=kafka_config["password"],
      value_deserializer=JsonSerializer(),
      key_deserializer=JsonSerializer(),
      auto_offset_reset='earliest',
      enable_auto_commit=True,
      group_id="group_building_sensors"
)

consumer_building.subscribe([topic_name])

print(f"Consumer {consumer_building} subscribed on topic {topic_name}")

try:
    for message in consumer_building:
        print(f"Message: {message.value}")
        if message.value["temperature"] > 40 or message.value["humidity"] > 80 or message.value["humidity"] < 20:
            producer = KafkaProducer(
                        bootstrap_servers=kafka_config["bootstrap_servers"],
                        security_protocol=kafka_config["security_protocol"],
                        sasl_mechanism=kafka_config["sasl_mechanism"],
                        sasl_plain_username=kafka_config["username"],
                        sasl_plain_password=kafka_config["password"],
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        key_serializer=lambda v: json.dumps(v).encode('utf-8')
                    )
            sensor_name = message.value["uuid_sensor"]
            date_event = message.value["date_event"]
           
            try:
                if message.value["temperature"] > 40:
                    temperature_alerts = "oleksiy_temperature_alerts"
                    temperature = message.value["temperature"]
                    data_temp_alert = {
                        "sensor_name": sensor_name,
                        "temperature": temperature,
                        "datetime": date_event,
                        "message": "Перевищення температурного показника 45 градусів"
                    }
                    producer.send(temperature_alerts, key=str(sensor_name), value=data_temp_alert)
                    producer.flush()
                    print(f"Temperature alert sent for sensor {sensor_name} with temperature {temperature}")
        
                if message.value["humidity"] > 80 or message.value["humidity"] < 20:
                    humidity_alerts = "oleksiy_humidity_alerts"
                    humidity = message.value["humidity"]
                    message_over_humidity = "Перевищення показника вологості 80"
                    message_beyond_humidity = "Показник нижче 20"
                    data_humidity_alert = {
                        "sensor_name": sensor_name,
                        "humidity": humidity,
                        "datetime": date_event,
                        "message": message_over_humidity if message.value["humidity"] > 80 else message_beyond_humidity
                    }
                    producer.send(humidity_alerts, key=str(sensor_name), value=data_humidity_alert)
                    producer.flush()
                    print(f"Humidity alert sent for sensor {sensor_name} with humidity {humidity}")
            except Exception as ex:
                print(f"incorrect producer: {str(ex)}")

            finally:
                producer.close()
                


except Exception as ex:
    print(f"incorrect request {str(ex)}")
finally:
    consumer_building.close()
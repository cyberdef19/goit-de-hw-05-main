from kafka import KafkaProducer
from configs import kafka_config, topic_names
import json
import uuid
import random
import time
import datetime

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

building_sensors_topic = "oleksiy_building_sensors"
sensor_name = uuid.uuid4()

for i in range(15):
    try:
          data = {
               "uuid_sensor": sensor_name,
               "temperature": random.randint(25, 46),
               "humidity": random.randint(15, 86),
               "date_event": datetime.datetime.now()
          }

          producer.send(building_sensors_topic, key=str(sensor_name), value=data)
          producer.flush()
          print(f"Надіслано повідомлення датчика {sensor_name}")

          time.sleep(5)
    except Exception as ex:
        print(f"incorrect data {str(ex)}")
    finally:
         producer.close()





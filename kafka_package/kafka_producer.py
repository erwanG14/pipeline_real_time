from kafka import KafkaProducer
from api_package import airplane_live_api
import json
import time
producer = KafkaProducer(bootstrap_servers = "kafka:9092" ,  value_serializer=lambda v: json.dumps(v).encode("utf-8"))
while True:
    data = airplane_live_api.airplane_request()
    for airplane in data:
        producer.send('airplane_topic',airplane)
    producer.flush()
    time.sleep(5)

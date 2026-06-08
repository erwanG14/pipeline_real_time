from kafka import KafkaProducer
from api_package import nasa_request
import json
data = nasa_request.nasa_request()
producer = KafkaProducer(bootstrap_servers = "kafka:9092" ,  value_serializer=lambda v: json.dumps(v).encode("utf-8"))
for asteroid in data:
    producer.send('test_topic',asteroid)
producer.flush()

import kafka
import json

comsumer = kafka.KafkaConsumer("test_topic",bootstrap_servers = "localhost:9092", value_deserializer=lambda v: json.loads(v.decode("utf-8")))

for msg in comsumer:
    print(msg)
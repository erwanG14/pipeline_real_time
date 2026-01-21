import kafka

comsumer = kafka.KafkaConsumer("test_topic",bootstrap_servers = "localhost:9092")

for msg in comsumer:
    print(msg)
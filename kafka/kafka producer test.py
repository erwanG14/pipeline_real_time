import kafka
import pipeline_real_time.api
data = pipeline_real_time.api.nasa_request()
producer = kafka.KafkaProducer(bootstrap_servers = "localhost:9092")
for asteroid in data:
    producer.send('test_topic',asteroid)
producer.flush()

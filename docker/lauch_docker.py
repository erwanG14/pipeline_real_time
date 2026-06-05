import docker
import time
from docker.types import Mount
client = docker.from_env()
kafka_container = client.containers.run("apache/kafka",name="kafka_image",detach=True,ports={"9092/tcp": 9092},environment={
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
        "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://host.docker.internal:9092",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1"
    })
time.sleep(2)

exec_result = kafka_container.exec_run(
    cmd=[
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "localhost:9092",
        "--create",
        "--if-not-exists",
        "--topic", "test_topic",
        "--partitions", "1",
        "--replication-factor", "1"
    ]
)

print(exec_result.output.decode())

time.sleep(2)
container = client.containers.run(
    name="spark_consumer_image",

    image="apache/spark:4.1.0",

    command=[
        "/opt/spark/bin/spark-submit",
        "--master", "local[*]",
        "--conf", "spark.jars.ivy=/tmp/ivy",
        "--packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
        "/app/spark_consumer.py"
    ],
    mounts=[
        Mount(
            target="/app",
            source=r"C:\perso\Coding\pipeline_real_time\kafka_package",
            type="bind",
            read_only=True
        ),
        Mount(
            target="/tmp/ivy",
            source=r"C:\spark-ivy",
            type="bind",
            read_only=False
        )
    ],
    working_dir="/app",
    tty=True,
    detach=True,
    remove=False
)
print("🔵 Spark consumer lancé, attente des logs...\n")

for line in container.logs(stream=True):
    print(line.decode("utf-8", errors="ignore"), end="")

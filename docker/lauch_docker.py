import docker
import time
client = docker.from_env()
container = client.containers.run("apache/kafka",name="test29",detach=True,ports={"9092/tcp": 9092},)

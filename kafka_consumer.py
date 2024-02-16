from kafka import KafkaProducer
from kafka import KafkaConsumer, TopicPartition
import time

topic = KafkaProducer(bootstrap_servers="localhost:9092")
for i in range(10):
    topic.send('demo', b'Sample Text')
    time.sleep(0.5)
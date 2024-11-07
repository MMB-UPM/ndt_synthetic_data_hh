from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
from pickle import loads, dumps

print(__name__)

bootstrap_servers = "milkyway.etsisi.upm.es:9094"
consumer = KafkaConsumer("inference_probs", bootstrap_servers=bootstrap_servers, group_id="my_group", value_deserializer=loads)

for message in consumer:
    print(f"offset = {message.offset}, key = {message.key}, value = {message.value}")
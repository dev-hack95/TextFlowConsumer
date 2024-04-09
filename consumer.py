import os
import sys
import json
import warnings
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer
from logger import logging

sys.path.append("./audio")
from audio import convert


# Config
load_dotenv("./env")
warnings.filterwarnings("ignore")

class KafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": self.bootstrap_servers,
        })

    def produce_messages(self, message):
        self.producer.produce(self.topic, value = json.dumps(message).encode('utf-8'))
        



class KafkaConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str) -> None:
        self.consume = True
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.consumer = Consumer({
        "bootstrap.servers": self.bootstrap_servers,
        "group.id": self.group_id,
        "auto.offset.reset": "earliest",
        })
 
        self.consumer.subscribe([self.topic])

    def consume_messages(self):
        while self.consume:
            message = self.consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
               print("Consumer error: {}".format(message.error()))
               continue

            data = json.loads(message.value())
            logging.info(data)
            video = data.get("video")
            email = data.get("email")
    
            self.consume = False
    
            t_instance = convert.Transform()
            t_instance.create_folder()
            t_instance.get_object(video)
            mp3_file = t_instance.convert(video)
            text = t_instance.transform(video)
            message = {
            "email": email,
            "video": video,
            "audio": mp3_file,
            "text": str(text)
            }
            producer.produce_messages(message)
            self.consume = True


if __name__ == "__main__":
    producer = KafkaProducer("192.168.29.7:9092", "Kafkatopic2")
    consumer = KafkaConsumer("192.168.29.7:9092", "group1", "Kafkatopic1")
    consumer.consume_messages()


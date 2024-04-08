import os
import sys
import json
import warnings
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from logger import logging

sys.path.append("./audio")
from audio import convert


# Config
load_dotenv("./env")
warnings.filterwarnings("ignore")
Base = declarative_base()



# KafkaConfig
topic1 = "Kafkatopic1"
topic2 = "Kafkatopic2"

producer = Producer(
    {
        "bootstrap.servers": "192.168.29.7:9092"
    }
)

consumer = Consumer(
    {
        "bootstrap.servers": "192.168.29.7:9092",
        "group.id": "group1",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe([topic1])


consume = True

while True:
    if consume:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        data = json.loads(msg.value())
        logging.info(data)
        video = data.get("video")
        email = data.get("email")

        consume = False

        t_instance = convert.Transform()
        t_instance.create_folder()
        t_instance.get_object(video)
        mp3_file = t_instance.convert(video)
        text = t_instance.transform(video)
        print(text)
        message = {
            "email": email,
            "video": video,
            "audio": mp3_file,
            "text": str(text)
        }
        producer.produce(topic2, value = json.dumps(message).encode('utf-8'))
        consume = True

    consumer.close()

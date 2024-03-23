import os
import sys
import json
import time
from dotenv import load_dotenv
from confluent_kafka import Consumer
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from logger import logging

sys.path.append("./audio")
from audio import convert

# SqlConfig
# load_dotenv("./env")
# db_user = os.environ.get("db_user")
# db_password = os.environ.get("db_password")
# db_host = os.environ.get("db_host")
# db_name = os.environ.get("db_name")

# engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
# SessionLocal = sessionmaker(autoflush=False, bind=engine)
# Base = declarative_base()
# db = SessionLocal()
# KafkaConfig
topic = "Kafkatopic1"

consumer = Consumer(
    {
        "bootstrap.servers": "192.168.29.7:9092",
        "group.id": "group1",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe([topic])


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
        logging.info("Process end")
        consume = True




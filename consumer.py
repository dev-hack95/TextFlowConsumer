import os
import sys
import json
import time
from dotenv import load_dotenv
from confluent_kafka import Consumer
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
Base = declarative_base()

class DBLocalSession:
    def __init__(self) -> None:
        self.db_user = os.environ.get("db_user")
        self.db_password = os.environ.get("db_password")
        self.db_host = os.environ.get("db_host")
        self.db_name = os.environ.get("db_name")

    def LocalSession(self) -> sessionmaker:
        engine = create_engine(f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}/{self.db_name}")
        SessionLocal = sessionmaker(autoflush=False, bind=engine)
        db = SessionLocal()
        return db


class SchemaModel(Base):
    __tablename__ = 'video_data'
    id = Column(Integer, autoincrement=True)
    email = Column(String)
    video = Column(String)
    audio = Column(String)
    text = Column(String) 

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
        session = DBLocalSession()
        data = session.LocalSession()
        try:
            new_record = SchemaModel(email=str(email), video=str(video), audio=str(mp3_file), text=str(text))
            data.add(new_record)
            data.commit()
            logging.info("Data successfully added to database")
        except Exception as err:
            data.rollback()
            logging.info(f"Failed to insert data into database: {str(err)}")
        finally:
            data.close()
        logging.info("Process end")
        consume = True




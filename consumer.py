import os
import sys
import json
sys.path.append("./audio")
from audio import convert
import motor.motor_asyncio
from bson import ObjectId
from logger import logging
from typing_extensions import Annotated
from exception import CustomException
from typing import Optional, List
from fastapi import FastAPI, Body, HTTPException
from confluent_kafka import Consumer, ConsumerGroupState
from pydantic import BaseModel, Field, EmailStr
from pydantic.functional_validators import BeforeValidator



# Config 
topic = "Kafkatopic1"

# server = FastAPI()
# client = motor.motor_asyncio.AsyncIOMotorClient(os.environ["MONGODB_URL"])
# db = client.textflow
# textflow_collection = db.get_collection("textflow")

# PyObjectId = Annotated[str, BeforeValidator(str)]

# class TextflowModel(BaseModel):
#     id: Optional[PyObjectId] = Field(alias="_id", default=None)
#     email: EmailStr = Field(...)
#     video: str = Field(...)
#     audio: str = Field(...)
#     text: str = Field(...)


consumer = Consumer({
    'bootstrap.servers': '192.168.29.7:9092',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest',
})

consumer.subscribe([topic])


# while True:
#     msg = consumer.poll(1.0)
    
    

#     output = msg.value().decode("utf-8")
#     print(output)

#     print('Received message: {}'.format(msg.value().decode('utf-8')))

while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    data = json.loads(msg.value())
    print(data)
    video = data["video"]

    t_instance = convert.Transform()
    t_instance.create_folder()
    t_instance.get_object(video)
    mp3_file = t_instance.convert(video)
    text = t_instance.transform(video)
    print(text)

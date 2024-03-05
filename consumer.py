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
from pydantic import BaseModel, Field, EmailStr, ConfigDict
from pydantic.functional_validators import BeforeValidator



# Config 
topic = "Kafkatopic1"

client = motor.motor_asyncio.AsyncIOMotorClient(os.environ.get("mongo"))
db = client.textflow
textflow_collection = db.get_collection("textflow")

PyObjectId = Annotated[str, BeforeValidator(str)]

class TextflowModel(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    email: EmailStr = Field(...)
    video: str = Field(...)
    audio: str = Field(...)
    text: str = Field(...)
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_schema_extra={
            "example" : {
                "email": "test@gmail.com",
                "video": "video/input.mp4",
                "audio": "audio/output.mp3",
                "text": "Hello world python programme",
            }
        },
    )


consumer = Consumer({
    'bootstrap.servers': '192.168.29.7:9092',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest',
})

consumer.subscribe([topic])


while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    data = json.loads(msg.value())
    video = data["video"]
    email = data["email"]

    t_instance = convert.Transform()
    t_instance.create_folder()
    t_instance.get_object(video)
    mp3_file = t_instance.convert(video)
    text = t_instance.transform(video)
    textflow_model = TextflowModel(email=email, video=video, audio=mp3_file, text=text)
    textflow_collection.insert_one(textflow_model.dict())

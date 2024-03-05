import os
import json
import whisper
from typing import Dict
from minio import Minio
from moviepy.editor import VideoFileClip


class Transform:
    def __init__(self) -> None:
        self.temp = "./session/"
        self.bucketname = "textflow"
        self.client = Minio(
            endpoint="192.168.29.7:9000",
            access_key="b8vYToqP0K6INKz10tLb",
            secret_key="iRkNVkXW5O1BogbaLSrTHqNPMauVKnpBAGXS9I2J",
            secure=False,
        )


    def create_folder(self):
        os.makedirs(self.temp, exist_ok=True)

    def get_object_name(self, data) -> str:
        file_name = self.temp +  data.split("/")[-1]
        mp3_file = data.split(".")[0]
        return file_name, mp3_file
    

    def get_object(self, data):
        response = self.client.get_object(self.bucketname, data)
        output = response.data
        file_path = data
        with open(file_path, 'wb') as file:
            file.write(output)
        


    def convert(self, data):
        file_name, mp3_file = self.get_object_name(data)
        video_clip = VideoFileClip(data)
        
        if video_clip.audio is None:
            print("No audio track found in the video.")
            video_clip.close()
            return None
    
        audio_clip = video_clip.audio
        audio_clip.write_audiofile(f"{mp3_file}.mp3")
        video_clip.close()
        audio_clip.close()
        output = f"{mp3_file}.mp3"
        return output
        
    def transform(self, data):
        _, mp3_file = self.get_object_name(data)
        model = whisper.load_model("base")
        result = model.transcribe(f"{mp3_file}.mp3")
        return result['text']
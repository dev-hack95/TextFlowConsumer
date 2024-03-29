FROM python:3.10.6
WORKDIR /app
COPY . /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
ENTRYPOINT python consumer.py
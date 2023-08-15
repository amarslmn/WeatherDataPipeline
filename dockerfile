FROM python:3.8.3

WORKDIR /opt/src

COPY weather_producer_main.py .
COPY weather_consumer_main.py .
COPY requirements.txt .

RUN pip install -r requirements.txt




FROM python:latest

COPY . /app

RUN pip install -r /app/requirements.txt

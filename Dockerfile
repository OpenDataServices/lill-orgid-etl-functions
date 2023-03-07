FROM python:3.9-bullseye

# Setup

WORKDIR /app
COPY . .

# Python

RUN pip install -r requirements.txt

# Run

CMD python docker.py


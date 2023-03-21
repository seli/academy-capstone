FROM python:3.9-slim-buster

USER root

RUN apt-get update && \
    apt-get install -y procps openjdk-11-jdk python3-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY read-transform-load.py .

ENTRYPOINT [ "python3" ]
CMD [ "read-transform-load.py" ]
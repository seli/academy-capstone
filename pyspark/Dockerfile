FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt /app

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY read-transform-load.py .

ENTRYPOINT [ "python3" ]
CMD [ "read-transform-load.py" ]
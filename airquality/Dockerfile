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

ADD https://repo1.maven.org/maven2/net/snowflake/snowflake-ingest-sdk/0.10.8/snowflake-ingest-sdk-0.10.8.jar /usr/local/lib/python3.9/site-packages/pyspark/jars
ADD https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl-java/1.0.7.Final/wildfly-openssl-java-1.0.7.Final.jar /usr/local/lib/python3.9/site-packages/pyspark/jars
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /usr/local/lib/python3.9/site-packages/pyspark/jars
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar /usr/local/lib/python3.9/site-packages/pyspark/jars
ADD https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.11.1-spark_3.3/spark-snowflake_2.12-2.11.1-spark_3.3.jar /usr/local/lib/python3.9/site-packages/pyspark/jars
ADD https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.24/snowflake-jdbc-3.13.24.jar /usr/local/lib/python3.9/site-packages/pyspark/jars

RUN chmod +r /usr/local/lib/python3.9/site-packages/pyspark/jars/*

COPY stage-airquality-data.py .

ENTRYPOINT [ "python3" ]
CMD [ "stage-airquality-data.py" ]
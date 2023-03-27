import requests
import boto3
import json
import datetime

from pathlib import Path

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp, col


# Use S3 AWS Bucket: s3a://dataminded-academy-capstone-resources/raw/open_aq/
BUCKET = "s3a://dataminded-academy-capstone-resources/"
KEY = "Serge@pxl/ingest"

config = {
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
}
conf = SparkConf().setAll(config.items())

def ingest_data(path: Path):
    current_year = datetime.date.today().year
    last_year = current_year - 1
    url = "https://api.openaq.org/v2/measurements?date_from=" + str(last_year) + "-01-01T00%3A00%3A00%2B00%3A00&date_to=" + str(current_year) + "-01-01T00%3A00%3A00%2B00%3A00&limit=1000&page=1&offset=0&country_id=BE"
    # print(url)
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    # print(response.text)
    json_obj = json.loads(response.text)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    rdd = spark.sparkContext.parallelize([json_obj])
    df = spark.createDataFrame(rdd);
    return df.coalesce(1).write.mode("overwrite").json(
        str(path)
    )

def read_data(path: Path):
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark.read.json(
        str(path)
    )

def flatten_data(frame: DataFrame) -> DataFrame:
    flattened_frame = (
        frame
            .select("*", "coordinates.*", "date.*")
            .drop("coordinates", "date")
    )
    return flattened_frame;

def transform_data(frame: DataFrame) -> DataFrame:
    transformed_frame = (
        frame
            .withColumn(
                "local",
                to_timestamp(
                    col("local"),
                    "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
                )
            )
            .withColumn(
                "utc",
                to_timestamp(
                    col("utc"),
                    "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
                )
            )
    )
    return transformed_frame

def get_snowflake_credentials() -> dict:
    client = boto3.client('secretsmanager', region_name='eu-west-1')
    secret_name = 'snowflake/capstone/login'
    response = client.get_secret_value(SecretId=secret_name)
    secret_value = response['SecretString']
    return json.loads(secret_value)


def load_data(frame: DataFrame, snowflake_secret: dict):
    options = {
        "sfURL": snowflake_secret['URL'],
        "sfWarehouse": snowflake_secret['WAREHOUSE'],
        "sfDatabase": snowflake_secret['DATABASE'],
        "sfSchema": "SERGE@PXL",
        "dbtable": "SERGE_WEATHER_TABLE",
        "sfUser": snowflake_secret['USER_NAME'],
        "sfPassword": snowflake_secret['PASSWORD'],
        "sfRole": snowflake_secret['ROLE']
    }
    (
    frame.write
        .format('snowflake')
        .options(**options) 
        .mode('overwrite') 
        .save()
    )
     

if __name__ == "__main__":
    # Ingest
    ingest_frame = ingest_data(BUCKET + KEY)
    # ingest_frame.printSchema()
    # ingest_frame.show(truncate=False)
    # Extract
 #   read_frame = read_data(BUCKET + KEY)
 #   read_frame.printSchema()
 #   read_frame.show(truncate=False)
    # Flatten
 #   flattened_frame = flatten_data(read_frame)
 #   flattened_frame.printSchema()
 #   flattened_frame.show(truncate=False)
    # Transform
 #   transformed_frame = transform_data(flattened_frame)
 #   transformed_frame.printSchema()
 #   transformed_frame.show(truncate=False)
    # Load
 #   secret_value = get_snowflake_credentials()
 #   print(secret_value)
 #   load_data(transformed_frame, secret_value)

# TODO
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import boto3
import time
import json

# # Set S3 endpoint to point to LocalStack
s3_endpoint_url = "http://localstack:4566"

# Kafka broker address and topic
kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "plain-topic"

bucket_name = "bronze-bucket"
output_s3_bucket = f's3a://{bucket_name}/plain'

# Log Group and Log Stream
log_group_name = 'pyspark-logs'
log_stream_name = 'bronze-job-stream'

def log_logging_events(message, logs_client):
    log_event = {
        'timestamp': int(round(time.time() * 1000)),
        'message': message
    }
    logs_client.put_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        logEvents=[log_event]
    )


def main():

    # Create a CloudWatch Logs client
    logs_client = boto3.client(
        'logs',
        endpoint_url=s3_endpoint_url,  # Adjust as per your LocalStack host
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )

    print('Spark CloudWatch Logs Client Instantiated')

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Bronze Layer Job") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    # print("Spark session initialized - jars")
    # print(spark.sparkContext._jsc.sc().listJars())
    # print("Spark driver - jars loaded in order")
    # print(spark.sparkContext.getConf().get("spark.driver.extraClassPath"))

    log_logging_events("Spark session initialized - jars", logs_client)
    log_logging_events(f'{spark.sparkContext._jsc.sc().listJars()}', logs_client)
    log_logging_events("Spark driver - jars loaded in order", logs_client)
    log_logging_events(f'{spark.sparkContext.getConf().get("spark.driver.extraClassPath")}', logs_client)

    # create_s3_bucket(bucket_name, s3_endpoint_url)

    # print('Spark Iceberg S3 bucket created')
    
    glueContext = GlueContext(spark)
    logger = glueContext.get_logger()

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Define the schema for the JSON data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("amount", IntegerType(), True)
    ])
    
    # Extract the value and parse the JSON
    parsed_df = df.selectExpr("CAST(value AS STRING) as value") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    def process_batch(data_frame, batch_id):
        if data_frame.count() > 0:
            # Convert back to DynamicFrame
            dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "batch_data")

            # Iterate through rows and log to CloudWatch
            for row in data_frame.collect():
                log_logging_events(json.dumps(row.asDict()), logs_client)
            
            # Write to S3 as Parquet
            glueContext.write_dynamic_frame.from_options(
                frame = dynamic_frame,
                connection_type = "s3",
                connection_options = {"path": output_s3_bucket},
                format = "parquet"
            )

    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .format("parquet") \
        .option("path",output_s3_bucket) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/bronze_layer/plain") \
        .trigger(processingTime="500 milliseconds") \
        .outputMode("append") \
        .start()

    query.awaitTermination()
    
    # spark.stop()

if __name__ == "__main__":
    main()
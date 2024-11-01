# TODO
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, ArrayType, DoubleType

import boto3
import time
import json

# # Set S3 endpoint to point to LocalStack
s3_endpoint_url = "http://localstack:4566"

# Kafka broker address and topic
kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "purchase-order"

bucket_name = "bronze-bucket"
output_s3_bucket = f's3a://{bucket_name}/purchase-order'

# Create Log Group and Log Stream
log_group_name = 'pyspark-po-logs'
log_stream_name = 'bronze-job-po-stream'


# Create the S3 bucket in LocalStack
# def create_s3_bucket(bucket_name, endpoint_url=None):
#     s3_client = boto3.client('s3', endpoint_url=endpoint_url)
#     try:
#         s3_client.create_bucket(Bucket=bucket_name)
#         print(f'Bucket {bucket_name} created successfully.')

#         response = s3_client.list_buckets()
#         print("Available buckets:", response['Buckets'])
        
#     except s3_client.exceptions.BucketAlreadyOwnedByYou:
#         print(f"Bucket {bucket_name} already exists.")
#     except Exception as e:
#         print(f"Error creating bucket {bucket_name}: {e}")

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

# Define schema for purchase order events
purchase_order_schema = StructType([
    StructField("contact_info", StructType([
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType())
    ])),
    StructField("po_number", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("sku", StringType()),
        StructField("description", StringType()),
        StructField("weight_quantity", IntegerType()),
        StructField("price", DoubleType())
    ]))),
    StructField("subtotal", DoubleType()),
    StructField("taxes", DoubleType()),
    StructField("total", DoubleType()),
    StructField("payment_due_date", DateType())
])

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
        .config("spark.jars", "/home/glue_user/spark/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar,/home/glue_user/spark/jars/kafka-clients-3.3.1.jar,"
                "/home/glue_user/spark/jars/commons-pool2-2.11.1.jar,/home/glue_user/spark/jars/iceberg-spark-runtime-3.3_2.12-0.14.0.jar") \
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

    # Parse JSON from Kafka value
    df = df.selectExpr("CAST(value AS STRING)") \
           .select(from_json(col("value"), purchase_order_schema).alias("data"))
    
    # Flatten the structure and select desired columns
    parsed_df = df.select(
        "data.contact_info.name",
        "data.contact_info.email",
        "data.contact_info.phone",
        "data.po_number",
        "data.items",
        "data.subtotal",
        "data.taxes",
        "data.total",
        "data.payment_due_date"
    )

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

    # Write to S3 in Parquet format (Bronze Layer)
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/bronze_layer/purchase_order") \
        .trigger(processingTime="500 milliseconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
# TODO
import boto3
import os
import time
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

bucket_name = "bronze-bucket"
output_s3_bucket = f's3a://{bucket_name}/purchase-order'

s3_endpoint_url = "http://localstack:4566"
namespace_catalog = "hadoop_catalog"
catalog_name = "local_catalog_purchase_order"
table_name = "silver_table"
full_table_name = f"`{namespace_catalog}`.`{catalog_name}`.`{table_name}`"

# Create Log Group and Log Stream
log_group_name = 'pyspark-po-logs'
log_stream_name = 'silver-job-po-stream'


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

def create_database_if_not_exists(spark, catalog_name):
    # Create the database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}")

def create_table_if_not_exists(spark, namespace_catalog, catalog_name, table_name):
    # Create the table if it doesn't exist 
    # NB: location must specified to tell Iceberg where the Hive metastore is located to store the data
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {namespace_catalog}.{catalog_name}.{table_name} (
            name STRING,
            email STRING,
            phone STRING,
            po_number STRING,
            sku STRING,
            description STRING,
            weight_quantity INT,
            price FLOAT,
            subtotal FLOAT,
            taxes FLOAT,
            total FLOAT,
            payment_due_date STRING
        )
        USING iceberg
        LOCATION 's3a://iceberg/warehouse/{catalog_name}/{table_name}'
    """)

     # Show the created table to verify
    tables_df = spark.sql(f"SHOW TABLES IN {namespace_catalog}.{catalog_name}")
    tables_df.show()

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
        .appName("Silver Layer Job") \
        .master("local[*]") \
        .config("spark.jars", "/home/glue_user/spark/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar,/home/glue_user/spark/jars/kafka-clients-3.3.1.jar,"
                "/home/glue_user/spark/jars/commons-pool2-2.11.1.jar,/home/glue_user/spark/jars/iceberg-spark-runtime-3.3_2.12-0.14.0.jar") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_catalog.warehouse", "s3a://iceberg/warehouse") \
        .config("spark.sql.catalog.hadoop_catalog.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.sql.catalog.hadoop_catalog.hadoop.fs.s3a.access.key", "test") \
        .config("spark.sql.catalog.hadoop_catalog.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.sql.catalog.hadoop_catalog.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.catalog.hadoop_catalog.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Create the S3 bucket in LocalStack
    # create_s3_bucket("iceberg", s3_endpoint_url)

    # print('Spark Iceberg S3 bucket created')
    
    # Create the database if it doesn't exist
    create_database_if_not_exists(spark, catalog_name)

    # print("Spark Iceberg database created")
    log_logging_events('Spark Iceberg database created', logs_client)

    # Create the table if it doesn't exist
    create_table_if_not_exists(spark, namespace_catalog, catalog_name, table_name)

    # print("Spark Iceberg table created")
    log_logging_events('Spark Iceberg table created', logs_client)


    # Read from S3 (Bronze)
    bronze_path = output_s3_bucket
    df = spark.read.format("parquet").load(bronze_path)
    # print(df.printSchema())
    # print(df.show())

    log_logging_events(f'{df.printSchema()}', logs_client)
    log_logging_events(f'{df.show()}', logs_client)


    # Data Cleaning and Transformations:
    # 1. Explode items array
    df = df.withColumn("item", explode("items")).select(
        "name", "email", "phone", "po_number", "item.sku", "item.description",
        "item.weight_quantity", "item.price", "subtotal", "taxes", "total", "payment_due_date"
    )

    # 2. Filter out invalid data (add your specific data quality checks here)
    df = df.filter(col("weight_quantity") > 5000) 

    # Write to S3 in Parquet format (Silver Layer)
    df.writeTo(full_table_name).append()

    # Show the Silver Iceberg table records
    message = spark.table(full_table_name).show()
    log_logging_events(f'{message}', logs_client)

    spark.stop()

if __name__ == "__main__":
    main()
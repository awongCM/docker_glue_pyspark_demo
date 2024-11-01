# TODO

import boto3
import time
import json
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError
from pyspark.sql.functions import col, sum, avg
from pyspark.sql.types import StringType, FloatType
from decimal import Decimal

# # Set S3 endpoint to point to LocalStack
s3_endpoint_url = "http://localstack:4566"
namespace_catalog = "hadoop_catalog"
catalog_name = "local_catalog_purchase_order"
table_name = "silver_table"
full_table_name = f"`{namespace_catalog}`.`{catalog_name}`.`{table_name}`"

# Define the schema of the DataFrame
# dataframe_schema = [
#     {'AttributeName': 'email', 'AttributeType': 'S'}
# ]

# Define the primary key schema for the DynamoDB table
# Here, 'id' is used as the partition key
# key_schema = [
#     {'AttributeName': 'email', 'KeyType': 'HASH'},  # Partition key
# ]

# Create the DynamoDB table
# def create_dynamodb_table(table_name, schema, key_schema):
#     dynamodb = boto3.client('dynamodb', endpoint_url=s3_endpoint_url, region_name='us-east-1')

#     try:
#         # Create the table
#         table = dynamodb.create_table(
#             TableName=table_name,
#             KeySchema=key_schema,
#             AttributeDefinitions=schema,
#             ProvisionedThroughput={
#                 'ReadCapacityUnits': 5,
#                 'WriteCapacityUnits': 5
#             }
#         )

#         # Wait for the table to be created
#         waiter = dynamodb.get_waiter('table_exists')
#         waiter.wait(TableName=table_name)
#         print(f"Table {table_name} created successfully.")

#     except ClientError as e:
#         error_code = e.response['Error']['Code']
#         if error_code == 'ResourceInUseException':
#             print(f"Table {table_name} already exists.")
#         else:
#             print(f"Failed to create table {table_name}. Error: {e}")
#         return None

#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         return None

# Create Log Group and Log Stream
log_group_name = 'pyspark-po-logs'
log_stream_name = 'gold-job-po-stream'

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
      .appName("Gold Layer Job") \
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
    
    # print("Spark session initialized - jars")
    # print(spark.sparkContext._jsc.sc().listJars())
    # print("Spark driver - jars loaded in order")
    # print(spark.sparkContext.getConf().get("spark.driver.extraClassPath"))

    log_logging_events("Spark session initialized - jars", logs_client)
    log_logging_events(f'{spark.sparkContext._jsc.sc().listJars()}', logs_client)
    log_logging_events("Spark driver - jars loaded in order", logs_client)
    log_logging_events(f'{spark.sparkContext.getConf().get("spark.driver.extraClassPath")}', logs_client)

    # Read from Iceberg (Silver)
    df = spark.read.format("iceberg").load(full_table_name)

    # Define the table name
    dynamo_table_name = "gold_table_purchase_order"

    # create_dynamodb_table(dynamo_table_name, dataframe_schema, key_schema)

    # Aggregations for reporting
    customer_order_totals = df.groupBy("email").agg(
        sum("total").alias("total_purchase_amount"),
        avg("total").alias("average_purchase_amount")
    )
    
    print(customer_order_totals.printSchema())
    print(customer_order_totals.show())

    log_logging_events(f'{customer_order_totals.printSchema()}', logs_client)
    log_logging_events(f'{customer_order_totals.show()}', logs_client)

    # Write to DynamoDB (Gold Layer)
    dynamodb = boto3.client("dynamodb", endpoint_url=s3_endpoint_url,region_name="us-east-1")

    data = customer_order_totals.collect()

    for row in data:
        item={
          "email": {"S": str(row["email"])},
          "total_purchase_amount": {"N": str(Decimal(row["total_purchase_amount"]))},
          "average_purchase_amount": {"N": str(Decimal(row["average_purchase_amount"]))}
        }
        dynamodb.put_item(TableName=dynamo_table_name,Item=item)
        # print(f"Inserted {len(data)} items into {dynamo_table_name}.")
        log_logging_events(f"Inserted {json.dumps(item)} items into {dynamo_table_name}.", logs_client)
    
    spark.stop()

if __name__ == "__main__":
    main()
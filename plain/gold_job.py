import boto3
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError
import time
import json

# # Set S3 endpoint to point to LocalStack
s3_endpoint_url = "http://localstack:4566"
namespace_catalog = "local_catalog"
catalog_name = "local_catalog_plain"
table_name = "silver_table"
full_table_name = f"`{namespace_catalog}`.`{catalog_name}`.`{table_name}`"

# Define the schema of the DataFrame
# dataframe_schema = [
#     {"AttributeName": "id", "AttributeType": "N"}
# ]

# Define the primary key schema for the DynamoDB table
# Here, 'id' is used as the partition key
# key_schema = [
#     {"AttributeName": "id", "KeyType": "HASH"},  # Partition key
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
log_group_name = 'pyspark-logs'
log_stream_name = 'gold-job-stream'

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
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local_catalog.type", "hadoop") \
        .config("spark.sql.catalog.local_catalog.warehouse", "s3a://iceberg/warehouse") \
        .config("spark.sql.catalog.local_catalog.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.sql.catalog.local_catalog.hadoop.fs.s3a.access.key", "test") \
        .config("spark.sql.catalog.local_catalog.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.sql.catalog.local_catalog.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.catalog.local_catalog.hadoop.fs.s3a.path.style.access", "true") \
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
    dynamo_table_name = "gold_table_plain"

    # create_dynamodb_table(dynamo_table_name, dataframe_schema, key_schema)

    dynamodb = boto3.client("dynamodb", endpoint_url=s3_endpoint_url,region_name="us-east-1")

    log_logging_events('Spark CloudWatch Logs created', logs_client)
    # print('Spark CloudWatch Logs created')

    data = df.collect()

    for row in data:
        item = {
            'id': {'N': str(row['id'])},
            'name': {'S': row['name']},
            'amount': {'N': str(row['amount'])}
        }
        dynamodb.put_item(TableName=dynamo_table_name, Item=item)

        log_logging_events(f"Inserted {json.dumps(item)} items into {dynamo_table_name}.", logs_client)

        # print(f"Inserted {len(data)} items into {dynamo_table_name}.")

    spark.stop()

if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import boto3

# # Set S3 endpoint to point to LocalStack
s3_endpoint_url = "http://localstack:4566"
catalog_name = "local_catalog"
table_name = "local_table"

# Kafka broker address and topic
kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "test-topic"

# Create the S3 bucket in LocalStack
def create_s3_bucket(bucket_name, endpoint_url=None):
    s3_client = boto3.client('s3', endpoint_url=endpoint_url)
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f'Bucket {bucket_name} created successfully.')

        response = s3_client.list_buckets()
        print("Available buckets:", response['Buckets'])
        
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"Error creating bucket {bucket_name}: {e}")


def create_database_if_not_exists(spark, catalog_name):
    # Create the database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}")

def create_table_if_not_exists(spark, catalog_name, table_name):
    # Create the table if it doesn't exist 
    # NB: location must specified to tell Iceberg where the Hive metastore is located to store the data
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS hadoop_catalog.{catalog_name}.{table_name} (
            id INT,
            name STRING,
            amount INT
        )
        USING iceberg
        PARTITIONED BY (id)
        LOCATION 's3a://iceberg/warehouse/{catalog_name}/{table_name}' 
    """)

    # Show the created table to verify
    tables_df = spark.sql(f"SHOW TABLES IN hadoop_catalog.{catalog_name}")
    tables_df.show()


def main():

    # Create the S3 bucket in LocalStack
    create_s3_bucket("iceberg", s3_endpoint_url)

    print('Spark Iceberg S3 bucket created')


    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("pysparkgluekafka") \
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
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

        
    print("Spark session initialized - jars")
    print(spark.sparkContext._jsc.sc().listJars())
    print("Spark driver - jars loaded in order...")
    print(spark.sparkContext.getConf().get("spark.driver.extraClassPath"))

    # Create the database if it doesn't exist
    create_database_if_not_exists(spark, catalog_name)

    print("Spark Iceberg database created")

    # Create the table if it doesn't exist
    create_table_if_not_exists(spark, catalog_name, table_name)

    print("Spark Iceberg table created")

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("amount", IntegerType(), True)
    ])

    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("startingOffset", "earliest") \
        .option("subscribe", kafka_topic) \
        .load()

    # Parse the JSON and write it to the Bronze table
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


    # Write to Bronze table (Iceberg)
    query = parsed_df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpointlocation") \
        .trigger(processingTime="10 seconds") \
        .toTable(f'hadoop_catalog.{catalog_name}.{table_name}')

    query.awaitTermination()
    

if __name__ == "__main__":
    main()

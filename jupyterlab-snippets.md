### Jupyterlab Notebook
URL to access: http://localhost:8888/


```
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
        .appName("Silver Layer Job") \
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

print("Spark session initialized successfully")


try:
    df  = spark.read.format("iceberg").load("local_catalog.local_catalog_plain.silver_table")
    print("Iceberg Table loaded successfully")
except Exception as e:
    print(f"Error loading Iceberg table: {e}")


try:
    df.show(truncate=False)
    print(f"Number of rows in the table: {df.count()}")
except Exception as e:
    print(f"Error showing data: {e}")
```

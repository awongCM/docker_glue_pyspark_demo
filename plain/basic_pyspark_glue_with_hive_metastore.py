import sys
from pyspark.sql import SparkSession

# Initialize Spark Session with Glue and Hive support
spark = (SparkSession.builder
         .appName("Glue PySpark Test")
         .config("hive.metastore.warehouse.dir", "/opt/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())

# Log that the job has started
print("Starting the PySpark Glue Job")

# Drop the table if it exists
spark.sql("DROP TABLE IF EXISTS test_table")

# Create a simple table using the Hive metastore
spark.sql("""
    CREATE TABLE test_table (
        id INT,
        name STRING
    ) STORED AS PARQUET
""")

# Insert some sample data
spark.sql("""
    INSERT INTO test_table VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie')
""")

# Query the table to confirm data is inserted
result_df = spark.sql("SELECT * FROM test_table")

# Show the results
result_df.show()

# Stop the Spark session
spark.stop()

print("Finished the PySpark Glue Job")

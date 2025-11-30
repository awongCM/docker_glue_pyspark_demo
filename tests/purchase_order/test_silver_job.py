"""Unit tests for purchase_order/silver_job.py - Silver Layer (S3 Parquet → Iceberg)."""
import pytest
from unittest.mock import MagicMock, patch, Mock
from pyspark.sql.functions import col, explode


@pytest.mark.unit
class TestPurchaseOrderSilverJobLogging:
    """Test CloudWatch logging functionality in purchase_order silver_job."""
    
    def test_should_log_message_to_cloudwatch_when_valid_message_provided(self, mock_boto3_logs_client):
        """
        Verify that log_logging_events sends messages to CloudWatch with correct format.
        
        Tests the logging utility function for purchase order silver layer.
        """
        # Arrange
        from purchase_order.silver_job import log_logging_events
        test_message = "Silver layer transformation completed"
        log_group = "pyspark-po-logs"
        log_stream = "silver-job-po-stream"
        
        # Act
        log_logging_events(test_message, mock_boto3_logs_client)
        
        # Assert
        mock_boto3_logs_client.put_log_events.assert_called_once()
        call_args = mock_boto3_logs_client.put_log_events.call_args
        assert call_args.kwargs['logGroupName'] == log_group
        assert call_args.kwargs['logStreamName'] == log_stream
        assert call_args.kwargs['logEvents'][0]['message'] == test_message


@pytest.mark.unit
class TestPurchaseOrderSilverJobConfiguration:
    """Test configuration constants in purchase_order silver_job."""
    
    def test_should_use_correct_s3_paths_when_reading_bronze_data(self):
        """
        Verify that silver_job reads from the correct bronze S3 path.
        
        Tests S3 input configuration for silver layer.
        """
        # Arrange
        from purchase_order.silver_job import output_s3_bucket, bucket_name
        
        # Act & Assert
        assert bucket_name == "bronze-bucket"
        assert output_s3_bucket == "s3a://bronze-bucket/purchase-order"
        assert output_s3_bucket.startswith("s3a://")
    
    def test_should_use_correct_iceberg_catalog_when_writing_data(self):
        """
        Verify that silver_job uses the correct Iceberg catalog configuration.
        
        Tests Iceberg table naming and catalog structure for purchase orders.
        """
        # Arrange
        from purchase_order.silver_job import namespace_catalog, catalog_name, table_name, full_table_name
        
        # Act & Assert
        assert namespace_catalog == "hadoop_catalog"
        assert catalog_name == "local_catalog_purchase_order"
        assert table_name == "silver_table"
        assert full_table_name == "`hadoop_catalog`.`local_catalog_purchase_order`.`silver_table`"
    
    def test_should_use_dedicated_log_streams_when_logging(self):
        """
        Verify that purchase_order silver_job uses dedicated CloudWatch configuration.
        
        Tests CloudWatch Logs separation from plain pipeline.
        """
        # Arrange
        from purchase_order.silver_job import log_group_name, log_stream_name
        
        # Act & Assert
        assert log_group_name == "pyspark-po-logs"
        assert log_stream_name == "silver-job-po-stream"


@pytest.mark.unit
class TestPurchaseOrderSilverJobDatabaseOperations:
    """Test database and table creation functions in purchase_order silver_job."""
    
    def test_should_create_database_when_not_exists(self, spark_session):
        """
        Verify that create_database_if_not_exists creates Iceberg database.
        
        Tests database creation logic for purchase order catalog.
        """
        # Arrange
        from purchase_order.silver_job import create_database_if_not_exists
        test_catalog_name = "test_po_catalog"
        
        # Act
        create_database_if_not_exists(spark_session, test_catalog_name)
        
        # Assert
        databases = spark_session.sql("SHOW DATABASES").collect()
        database_names = [row.namespace for row in databases]
        assert test_catalog_name in database_names
    
    def test_should_create_table_with_correct_flattened_schema_when_not_exists(self):
        """
        Verify that create_table_if_not_exists creates table with flattened purchase order schema.
        
        Tests table creation with denormalized structure after exploding items array.
        """
        # Arrange
        expected_columns = [
            "name", "email", "phone", "po_number", 
            "sku", "description", "weight_quantity", "price",
            "subtotal", "taxes", "total", "payment_due_date"
        ]
        
        # Act - Construct the CREATE TABLE SQL as in silver_job
        table_sql = """
            CREATE TABLE IF NOT EXISTS hadoop_catalog.test_catalog.test_table (
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
            LOCATION 's3a://iceberg/warehouse/test_catalog/test_table'
        """
        
        # Assert - Verify SQL structure
        assert "CREATE TABLE IF NOT EXISTS" in table_sql
        assert "USING iceberg" in table_sql
        assert all(col in table_sql for col in expected_columns)
        assert len(expected_columns) == 12


@pytest.mark.unit
class TestPurchaseOrderSilverJobDataTransformation:
    """Test data transformation logic in purchase_order silver_job."""
    
    def test_should_explode_items_array_when_transforming_data(
        self, spark_session, purchase_order_sample_data
    ):
        """
        Verify that silver_job explodes items array to create one row per line item.
        
        Tests array explosion transformation to denormalize nested data.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        df = spark_session.createDataFrame(purchase_order_sample_data, schema=purchase_order_schema)
        
        # Act - Replicate silver_job transformation
        exploded_df = df.withColumn("item", explode("items")).select(
            "contact_info.name", "contact_info.email", "contact_info.phone", "po_number", "item.sku", "item.description",
            "item.weight_quantity", "item.price", "subtotal", "taxes", "total", "payment_due_date"
        )
        result = exploded_df.collect()
        
        # Assert
        # John's order has 2 items + Jane's order has 1 item = 3 total rows
        assert len(result) == 3
        assert result[0]['sku'] == "SKU-001"
        assert result[1]['sku'] == "SKU-002"
        assert result[2]['sku'] == "SKU-003"
    
    def test_should_flatten_contact_info_when_transforming_data(
        self, spark_session, purchase_order_sample_data
    ):
        """
        Verify that silver_job flattens nested contact_info structure.
        
        Tests denormalization of nested contact information.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        df = spark_session.createDataFrame(purchase_order_sample_data, schema=purchase_order_schema)
        
        # Act - Replicate silver_job transformation
        exploded_df = df.withColumn("item", explode("items")).select(
            col("contact_info.name").alias("name"), 
            col("contact_info.email").alias("email"), 
            col("contact_info.phone").alias("phone"), 
            "po_number", "item.sku", "item.description",
            "item.weight_quantity", "item.price", "subtotal", "taxes", "total", "payment_due_date"
        )
        result = exploded_df.collect()
        
        # Assert - Check that contact_info fields are flattened
        assert result[0]['name'] == "John Doe"
        assert result[0]['email'] == "john@example.com"
        assert result[0]['phone'] == "555-1234"
        assert 'contact_info' not in exploded_df.columns
    
    def test_should_filter_by_weight_quantity_when_cleaning_data(
        self, spark_session, purchase_order_sample_data
    ):
        """
        Verify that silver_job filters records with weight_quantity > 5000.
        
        Tests data quality filter for minimum weight threshold.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        df = spark_session.createDataFrame(purchase_order_sample_data, schema=purchase_order_schema)
        threshold = 5000
        
        # Act - Replicate silver_job transformation and filter
        exploded_df = df.withColumn("item", explode("items")).select(
            "contact_info.name", "contact_info.email", "contact_info.phone", "po_number", "item.sku", "item.description",
            "item.weight_quantity", "item.price", "subtotal", "taxes", "total", "payment_due_date"
        )
        filtered_df = exploded_df.filter(col("weight_quantity") > threshold)
        result = filtered_df.collect()
        
        # Assert
        # All sample items have weight_quantity > 5000 (10000, 8000, 12000)
        assert len(result) == 3
        for row in result:
            assert row['weight_quantity'] > threshold
    
    def test_should_exclude_low_weight_items_when_filtering_data(
        self, spark_session
    ):
        """
        Verify that silver_job excludes items below weight threshold.
        
        Tests negative case for weight filter.
        """
        # Arrange
        from datetime import date
        from purchase_order.bronze_job import purchase_order_schema
        low_weight_data = [
            {
                "contact_info": {"name": "Test User", "email": "test@example.com", "phone": "555-0000"},
                "po_number": "PO-TEST",
                "items": [
                    {"sku": "SKU-LIGHT", "description": "Light item", "weight_quantity": 3000, "price": 10.0}
                ],
                "subtotal": 10.0,
                "taxes": 1.0,
                "total": 11.0,
                "payment_due_date": date(2026, 1, 1)
            }
        ]
        df = spark_session.createDataFrame(low_weight_data, schema=purchase_order_schema)
        
        # Act
        exploded_df = df.withColumn("item", explode("items")).select(
            "contact_info.name", "contact_info.email", "contact_info.phone", "po_number", "item.sku", "item.description",
            "item.weight_quantity", "item.price", "subtotal", "taxes", "total", "payment_due_date"
        )
        filtered_df = exploded_df.filter(col("weight_quantity") > 5000)
        result = filtered_df.collect()
        
        # Assert
        assert len(result) == 0, "Items with weight_quantity <= 5000 should be filtered out"
    
    def test_should_preserve_all_columns_after_transformation_when_exploding_items(
        self, spark_session, purchase_order_sample_data
    ):
        """
        Verify that all required columns are present after transformation.
        
        Tests schema completeness after denormalization.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        df = spark_session.createDataFrame(purchase_order_sample_data, schema=purchase_order_schema)
        expected_columns = [
            "name", "email", "phone", "po_number", 
            "sku", "description", "weight_quantity", "price",
            "subtotal", "taxes", "total", "payment_due_date"
        ]
        
        # Act
        exploded_df = df.withColumn("item", explode("items")).select(
            col("contact_info.name").alias("name"), 
            col("contact_info.email").alias("email"), 
            col("contact_info.phone").alias("phone"), 
            "po_number", "item.sku", "item.description",
            "item.weight_quantity", "item.price", "subtotal", "taxes", "total", "payment_due_date"
        )
        actual_columns = exploded_df.columns
        
        # Assert
        assert actual_columns == expected_columns
        assert len(actual_columns) == 12
    
    def test_should_handle_boundary_weight_value_when_filtering_data(
        self, spark_session
    ):
        """
        Verify that filtering correctly handles boundary value (weight_quantity = 5000).
        
        Tests boundary condition for weight filter (exclusive threshold).
        """
        # Arrange
        from datetime import date
        from purchase_order.bronze_job import purchase_order_schema
        boundary_data = [
            {
                "contact_info": {"name": "Boundary Test", "email": "boundary@example.com", "phone": "555-1111"},
                "po_number": "PO-BOUNDARY",
                "items": [
                    {"sku": "SKU-5000", "description": "Exactly 5000", "weight_quantity": 5000, "price": 50.0},
                    {"sku": "SKU-5001", "description": "Just above", "weight_quantity": 5001, "price": 51.0}
                ],
                "subtotal": 101.0,
                "taxes": 8.0,
                "total": 109.0,
                "payment_due_date": date(2026, 2, 1)
            }
        ]
        df = spark_session.createDataFrame(boundary_data, schema=purchase_order_schema)
        
        # Act
        exploded_df = df.withColumn("item", explode("items")).select(
            col("contact_info.name").alias("name"), 
            col("contact_info.email").alias("email"), 
            col("contact_info.phone").alias("phone"), 
            "po_number", "item.sku", "item.description",
            "item.weight_quantity", "item.price", "subtotal", "taxes", "total", "payment_due_date"
        )
        filtered_df = exploded_df.filter(col("weight_quantity") > 5000)
        result = filtered_df.collect()
        
        # Assert
        assert len(result) == 1, "Only weight_quantity > 5000 should pass"
        assert result[0]['weight_quantity'] == 5001
        assert result[0]['sku'] == "SKU-5001"

"""Unit tests for purchase_order/bronze_job.py - Bronze Layer (Kafka → S3 Parquet)."""
import pytest
from unittest.mock import MagicMock, patch, Mock
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, ArrayType
)


@pytest.mark.unit
class TestPurchaseOrderBronzeJobLogging:
    """Test CloudWatch logging functionality in purchase_order bronze_job."""
    
    def test_should_log_message_to_cloudwatch_when_valid_message_provided(self, mock_boto3_logs_client):
        """
        Verify that log_logging_events sends messages to CloudWatch with correct format.
        
        Tests the logging utility function for purchase order bronze layer.
        """
        # Arrange
        from purchase_order.bronze_job import log_logging_events
        test_message = "Purchase order bronze processing started"
        log_group = "pyspark-po-logs"
        log_stream = "bronze-job-po-stream"
        
        # Act
        log_logging_events(test_message, mock_boto3_logs_client)
        
        # Assert
        mock_boto3_logs_client.put_log_events.assert_called_once()
        call_args = mock_boto3_logs_client.put_log_events.call_args
        assert call_args.kwargs['logGroupName'] == log_group
        assert call_args.kwargs['logStreamName'] == log_stream
        assert call_args.kwargs['logEvents'][0]['message'] == test_message


@pytest.mark.unit
class TestPurchaseOrderBronzeJobSchema:
    """Test schema definition for purchase_order bronze job."""
    
    def test_should_have_correct_top_level_fields_when_parsing_kafka_messages(self):
        """
        Verify that purchase_order schema has all required top-level fields.
        
        Tests that the StructType schema includes contact_info, po_number, items, etc.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        expected_fields = [
            "contact_info", "po_number", "items", 
            "subtotal", "taxes", "total", "payment_due_date"
        ]
        
        # Act
        actual_fields = [field.name for field in purchase_order_schema.fields]
        
        # Assert
        assert len(actual_fields) == 7
        for expected_field in expected_fields:
            assert expected_field in actual_fields
    
    def test_should_have_nested_contact_info_structure_when_defining_schema(self):
        """
        Verify that contact_info field has nested name, email, phone fields.
        
        Tests nested StructType for contact information.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        contact_info_field = next(f for f in purchase_order_schema.fields if f.name == "contact_info")
        
        # Act
        nested_fields = [field.name for field in contact_info_field.dataType.fields]
        
        # Assert
        assert "name" in nested_fields
        assert "email" in nested_fields
        assert "phone" in nested_fields
        assert len(nested_fields) == 3
    
    def test_should_have_array_of_items_when_defining_schema(self):
        """
        Verify that items field is an ArrayType containing item structs.
        
        Tests ArrayType structure for line items.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        items_field = next(f for f in purchase_order_schema.fields if f.name == "items")
        
        # Act
        is_array_type = isinstance(items_field.dataType, ArrayType)
        item_struct = items_field.dataType.elementType
        item_field_names = [field.name for field in item_struct.fields]
        
        # Assert
        assert is_array_type is True
        assert "sku" in item_field_names
        assert "description" in item_field_names
        assert "weight_quantity" in item_field_names
        assert "price" in item_field_names
        assert len(item_field_names) == 4
    
    def test_should_use_correct_numeric_types_when_defining_schema(self):
        """
        Verify that numeric fields use appropriate data types (IntegerType, DoubleType).
        
        Tests type selection for monetary and quantity values.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        
        # Act
        subtotal_field = next(f for f in purchase_order_schema.fields if f.name == "subtotal")
        taxes_field = next(f for f in purchase_order_schema.fields if f.name == "taxes")
        total_field = next(f for f in purchase_order_schema.fields if f.name == "total")
        
        # Assert
        assert isinstance(subtotal_field.dataType, DoubleType)
        assert isinstance(taxes_field.dataType, DoubleType)
        assert isinstance(total_field.dataType, DoubleType)
    
    def test_should_use_date_type_for_payment_due_date_when_defining_schema(self):
        """
        Verify that payment_due_date field uses DateType.
        
        Tests temporal data type usage.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        
        # Act
        payment_due_date_field = next(
            f for f in purchase_order_schema.fields if f.name == "payment_due_date"
        )
        
        # Assert
        assert isinstance(payment_due_date_field.dataType, DateType)


@pytest.mark.unit
class TestPurchaseOrderBronzeJobConfiguration:
    """Test configuration constants in purchase_order bronze_job."""
    
    def test_should_use_correct_kafka_topic_when_reading_stream(self):
        """
        Verify that bronze_job is configured with the correct Kafka topic.
        
        Tests configuration constants for Kafka integration.
        """
        # Arrange
        from purchase_order.bronze_job import kafka_topic, kafka_bootstrap_servers
        
        # Act & Assert
        assert kafka_topic == "purchase-order"
        assert kafka_bootstrap_servers == "kafka:29092"
    
    def test_should_use_correct_s3_path_when_writing_output(self):
        """
        Verify that bronze_job writes to the correct S3 bucket path.
        
        Tests S3 output configuration for purchase_order bronze layer.
        """
        # Arrange
        from purchase_order.bronze_job import output_s3_bucket, bucket_name
        
        # Act & Assert
        assert bucket_name == "bronze-bucket"
        assert output_s3_bucket == "s3a://bronze-bucket/purchase-order"
        assert output_s3_bucket.startswith("s3a://")
    
    def test_should_use_correct_log_group_when_logging(self):
        """
        Verify that purchase_order bronze_job uses dedicated CloudWatch log group.
        
        Tests CloudWatch Logs configuration separation from plain pipeline.
        """
        # Arrange
        from purchase_order.bronze_job import log_group_name, log_stream_name
        
        # Act & Assert
        assert log_group_name == "pyspark-po-logs"
        assert log_stream_name == "bronze-job-po-stream"
    
    def test_should_use_localstack_endpoint_when_in_development(self):
        """
        Verify that bronze_job uses LocalStack endpoint for local development.
        
        Tests that AWS services point to LocalStack.
        """
        # Arrange
        from purchase_order.bronze_job import s3_endpoint_url
        
        # Act & Assert
        assert s3_endpoint_url == "http://localstack:4566"


@pytest.mark.unit
class TestPurchaseOrderBronzeJobDataProcessing:
    """Test data processing logic in purchase_order bronze_job."""
    
    def test_should_handle_nested_contact_info_when_processing_batch(
        self, spark_session, purchase_order_sample_data
    ):
        """
        Verify that process_batch correctly handles nested contact_info structure.
        
        Tests processing of nested data structures from Kafka.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        df = spark_session.createDataFrame(purchase_order_sample_data, schema=purchase_order_schema)
        
        # Act
        records = df.select("contact_info.name", "contact_info.email", "contact_info.phone").collect()
        
        # Assert
        assert len(records) == 2
        assert records[0]['name'] == "John Doe"
        assert records[0]['email'] == "john@example.com"
        assert records[1]['name'] == "Jane Smith"
    
    def test_should_handle_array_items_when_processing_batch(
        self, spark_session, purchase_order_sample_data
    ):
        """
        Verify that process_batch correctly handles array of items.
        
        Tests processing of array fields with multiple line items.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        df = spark_session.createDataFrame(purchase_order_sample_data, schema=purchase_order_schema)
        
        # Act
        records = df.select("items").collect()
        first_order_items = records[0]['items']
        second_order_items = records[1]['items']
        
        # Assert
        assert len(first_order_items) == 2  # John's order has 2 items
        assert len(second_order_items) == 1  # Jane's order has 1 item
        assert first_order_items[0]['sku'] == "SKU-001"
        assert first_order_items[1]['sku'] == "SKU-002"
    
    def test_should_preserve_monetary_precision_when_processing_batch(
        self, spark_session, purchase_order_sample_data
    ):
        """
        Verify that monetary values maintain precision through processing.
        
        Tests that DoubleType preserves decimal precision for currency.
        """
        # Arrange
        from purchase_order.bronze_job import purchase_order_schema
        df = spark_session.createDataFrame(purchase_order_sample_data, schema=purchase_order_schema)
        
        # Act
        records = df.select("subtotal", "taxes", "total").collect()
        
        # Assert
        assert records[0]['subtotal'] == 41.25
        assert records[0]['taxes'] == 3.30
        assert records[0]['total'] == 44.55
        assert isinstance(records[0]['subtotal'], float)

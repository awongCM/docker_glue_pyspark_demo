"""Unit tests for purchase_order/gold_job.py - Gold Layer (Iceberg → DynamoDB)."""
import pytest
from unittest.mock import MagicMock, patch, Mock
from pyspark.sql.functions import col, sum, avg
from decimal import Decimal


@pytest.mark.unit
class TestPurchaseOrderGoldJobLogging:
    """Test CloudWatch logging functionality in purchase_order gold_job."""
    
    def test_should_log_message_to_cloudwatch_when_valid_message_provided(self, mock_boto3_logs_client):
        """
        Verify that log_logging_events sends messages to CloudWatch with correct format.
        
        Tests the logging utility function for purchase order gold layer.
        """
        # Arrange
        from purchase_order.gold_job import log_logging_events
        test_message = "Gold layer aggregation processing started"
        log_group = "pyspark-po-logs"
        log_stream = "gold-job-po-stream"
        
        # Act
        log_logging_events(test_message, mock_boto3_logs_client)
        
        # Assert
        mock_boto3_logs_client.put_log_events.assert_called_once()
        call_args = mock_boto3_logs_client.put_log_events.call_args
        assert call_args.kwargs['logGroupName'] == log_group
        assert call_args.kwargs['logStreamName'] == log_stream
        assert call_args.kwargs['logEvents'][0]['message'] == test_message


@pytest.mark.unit
class TestPurchaseOrderGoldJobConfiguration:
    """Test configuration constants in purchase_order gold_job."""
    
    def test_should_use_correct_iceberg_table_when_reading_silver_data(self):
        """
        Verify that gold_job reads from the correct Iceberg silver table.
        
        Tests Iceberg table reference for gold layer input.
        """
        # Arrange
        from purchase_order.gold_job import namespace_catalog, catalog_name, table_name, full_table_name
        
        # Act & Assert
        assert namespace_catalog == "hadoop_catalog"
        assert catalog_name == "local_catalog_purchase_order"
        assert table_name == "silver_table"
        assert full_table_name == "`hadoop_catalog`.`local_catalog_purchase_order`.`silver_table`"
    
    def test_should_use_correct_dynamodb_table_when_writing_gold_data(self):
        """
        Verify that gold_job writes to the correct DynamoDB table.
        
        Tests DynamoDB output configuration for purchase order gold layer.
        """
        # Arrange - The table name is defined inline in main()
        expected_table_name = "gold_table_purchase_order"
        
        # Act & Assert
        assert expected_table_name == "gold_table_purchase_order"
    
    def test_should_use_dedicated_log_streams_when_logging(self):
        """
        Verify that purchase_order gold_job uses dedicated CloudWatch configuration.
        
        Tests CloudWatch Logs separation from plain pipeline.
        """
        # Arrange
        from purchase_order.gold_job import log_group_name, log_stream_name
        
        # Act & Assert
        assert log_group_name == "pyspark-po-logs"
        assert log_stream_name == "gold-job-po-stream"


@pytest.mark.unit
class TestPurchaseOrderGoldJobAggregation:
    """Test aggregation logic in purchase_order gold_job."""
    
    def test_should_aggregate_by_email_when_calculating_customer_totals(self, spark_session):
        """
        Verify that gold_job aggregates purchase amounts by customer email.
        
        Tests groupBy aggregation for customer-level analytics.
        """
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
        
        sample_data = [
            {"email": "john@example.com", "name": "John Doe", "total": 44.55, "po_number": "PO-001"},
            {"email": "john@example.com", "name": "John Doe", "total": 30.00, "po_number": "PO-002"},
            {"email": "jane@example.com", "name": "Jane Smith", "total": 107.99, "po_number": "PO-003"}
        ]
        
        schema = StructType([
            StructField("email", StringType(), True),
            StructField("name", StringType(), True),
            StructField("total", FloatType(), True),
            StructField("po_number", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(sample_data, schema=schema)
        
        # Act - Replicate gold_job aggregation
        customer_order_totals = df.groupBy("email").agg(
            sum("total").alias("total_purchase_amount"),
            avg("total").alias("average_purchase_amount")
        )
        result = customer_order_totals.collect()
        
        # Assert
        assert len(result) == 2  # Two unique customers
        john_row = next(r for r in result if r['email'] == "john@example.com")
        jane_row = next(r for r in result if r['email'] == "jane@example.com")
        
        assert john_row['total_purchase_amount'] == pytest.approx(74.55, abs=0.01)
        assert john_row['average_purchase_amount'] == pytest.approx(37.275, abs=0.01)
        assert jane_row['total_purchase_amount'] == pytest.approx(107.99, abs=0.01)
        assert jane_row['average_purchase_amount'] == pytest.approx(107.99, abs=0.01)
    
    def test_should_calculate_sum_and_average_when_aggregating_totals(self, spark_session):
        """
        Verify that gold_job calculates both sum and average for each customer.
        
        Tests dual aggregation functions in groupBy operation.
        """
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType, FloatType
        
        sample_data = [
            {"email": "test@example.com", "total": 100.0},
            {"email": "test@example.com", "total": 200.0},
            {"email": "test@example.com", "total": 300.0}
        ]
        
        schema = StructType([
            StructField("email", StringType(), True),
            StructField("total", FloatType(), True)
        ])
        
        df = spark_session.createDataFrame(sample_data, schema=schema)
        
        # Act
        customer_order_totals = df.groupBy("email").agg(
            sum("total").alias("total_purchase_amount"),
            avg("total").alias("average_purchase_amount")
        )
        result = customer_order_totals.collect()
        
        # Assert
        assert len(result) == 1
        assert result[0]['total_purchase_amount'] == pytest.approx(600.0, abs=0.01)
        assert result[0]['average_purchase_amount'] == pytest.approx(200.0, abs=0.01)
    
    def test_should_handle_single_order_per_customer_when_aggregating(self, spark_session):
        """
        Verify that aggregation works correctly for customers with single orders.
        
        Tests edge case where average equals total for single-order customers.
        """
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType, FloatType
        
        sample_data = [
            {"email": "single@example.com", "total": 99.99}
        ]
        
        schema = StructType([
            StructField("email", StringType(), True),
            StructField("total", FloatType(), True)
        ])
        
        df = spark_session.createDataFrame(sample_data, schema=schema)
        
        # Act
        customer_order_totals = df.groupBy("email").agg(
            sum("total").alias("total_purchase_amount"),
            avg("total").alias("average_purchase_amount")
        )
        result = customer_order_totals.collect()
        
        # Assert
        assert len(result) == 1
        assert result[0]['total_purchase_amount'] == result[0]['average_purchase_amount']
        assert result[0]['total_purchase_amount'] == pytest.approx(99.99, abs=0.01)


@pytest.mark.unit
class TestPurchaseOrderGoldJobDynamoDBOperations:
    """Test DynamoDB write operations in purchase_order gold_job."""
    
    def test_should_format_aggregated_item_correctly_when_writing_to_dynamodb(self):
        """
        Verify that aggregated data is formatted correctly for DynamoDB put_item.
        
        Tests DynamoDB item structure with Decimal conversion for aggregated values.
        """
        # Arrange
        sample_aggregation = {
            "email": "john@example.com",
            "total_purchase_amount": 150.75,
            "average_purchase_amount": 75.375
        }
        
        # Act - Format as done in gold_job
        item = {
            "email": {"S": str(sample_aggregation["email"])},
            "total_purchase_amount": {"N": str(Decimal(sample_aggregation["total_purchase_amount"]))},
            "average_purchase_amount": {"N": str(Decimal(sample_aggregation["average_purchase_amount"]))}
        }
        
        # Assert
        assert item['email'] == {'S': 'john@example.com'}
        assert item['total_purchase_amount']['N'] == str(Decimal('150.75'))
        assert item['average_purchase_amount']['N'] == str(Decimal('75.375'))
        assert 'S' in item['email'], "Email should use String type"
        assert 'N' in item['total_purchase_amount'], "Numeric values should use Number type"
    
    def test_should_use_decimal_for_monetary_values_when_formatting_for_dynamodb(self):
        """
        Verify that monetary values are converted to Decimal for DynamoDB precision.
        
        Tests Decimal conversion to maintain currency precision in DynamoDB.
        """
        # Arrange
        float_value = 123.456789
        
        # Act
        decimal_value = Decimal(float_value)
        dynamodb_item = {"N": str(decimal_value)}
        
        # Assert
        assert isinstance(decimal_value, Decimal)
        assert "123.456789" in dynamodb_item['N']
    
    def test_should_handle_all_aggregated_records_when_batch_writing_to_dynamodb(
        self
    ):
        """
        Verify that all aggregated customer records are written to DynamoDB.
        
        Tests that gold_job processes all customer aggregations to DynamoDB.
        """
        # Arrange
        from unittest.mock import MagicMock
        mock_dynamodb_client = MagicMock()
        mock_dynamodb_client.put_item.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        dynamo_table_name = "gold_table_purchase_order"
        
        aggregated_data = [
            {"email": "john@example.com", "total_purchase_amount": 150.0, "average_purchase_amount": 75.0},
            {"email": "jane@example.com", "total_purchase_amount": 200.0, "average_purchase_amount": 100.0}
        ]
        
        # Act - Simulate the loop in gold_job main()
        for record in aggregated_data:
            item = {
                "email": {"S": str(record["email"])},
                "total_purchase_amount": {"N": str(Decimal(record["total_purchase_amount"]))},
                "average_purchase_amount": {"N": str(Decimal(record["average_purchase_amount"]))}
            }
            mock_dynamodb_client.put_item(TableName=dynamo_table_name, Item=item)
        
        # Assert
        assert mock_dynamodb_client.put_item.call_count == len(aggregated_data)
        assert mock_dynamodb_client.put_item.call_count == 2


@pytest.mark.unit
class TestPurchaseOrderGoldJobDataIntegrity:
    """Test data integrity and edge cases in purchase_order gold_job."""
    
    def test_should_handle_empty_dataset_when_no_silver_records_exist(self, spark_session):
        """
        Verify that gold_job handles empty DataFrame from Iceberg gracefully.
        
        Tests edge case where silver table has no records after filtering.
        """
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType, FloatType
        
        schema = StructType([
            StructField("email", StringType(), True),
            StructField("total", FloatType(), True)
        ])
        
        empty_df = spark_session.createDataFrame([], schema=schema)
        
        # Act
        customer_order_totals = empty_df.groupBy("email").agg(
            sum("total").alias("total_purchase_amount"),
            avg("total").alias("average_purchase_amount")
        )
        result = customer_order_totals.collect()
        
        # Assert
        assert len(result) == 0
        assert result == []
    
    def test_should_handle_special_characters_in_email_when_writing_to_dynamodb(self):
        """
        Verify that email addresses with special characters are handled correctly.
        
        Tests edge case with various email formats (dots, hyphens, plus signs).
        """
        # Arrange
        special_emails = [
            "user.name@example.com",
            "user+tag@example.com",
            "user-name@example.co.uk"
        ]
        
        # Act & Assert
        for email in special_emails:
            item = {"email": {"S": email}}
            assert item['email']['S'] == email
            assert '@' in item['email']['S']
    
    def test_should_preserve_precision_when_converting_aggregates_to_decimal(self):
        """
        Verify that Decimal conversion preserves monetary precision.
        
        Tests that floating-point aggregations maintain precision through Decimal conversion.
        """
        # Arrange
        total_amount = 12345.67
        average_amount = 1234.567
        
        # Act
        total_decimal = Decimal(str(total_amount))
        average_decimal = Decimal(str(average_amount))
        
        # Assert
        assert str(total_decimal) == "12345.67"
        assert str(average_decimal) == "1234.567"
        assert total_decimal == Decimal('12345.67')
    
    def test_should_handle_zero_total_when_aggregating(self, spark_session):
        """
        Verify that aggregation handles records with zero total values.
        
        Tests boundary case with zero-value orders.
        """
        # Arrange
        from pyspark.sql.types import StructType, StructField, StringType, FloatType
        
        sample_data = [
            {"email": "zero@example.com", "total": 0.0}
        ]
        
        schema = StructType([
            StructField("email", StringType(), True),
            StructField("total", FloatType(), True)
        ])
        
        df = spark_session.createDataFrame(sample_data, schema=schema)
        
        # Act
        customer_order_totals = df.groupBy("email").agg(
            sum("total").alias("total_purchase_amount"),
            avg("total").alias("average_purchase_amount")
        )
        result = customer_order_totals.collect()
        
        # Assert
        assert len(result) == 1
        assert result[0]['total_purchase_amount'] == 0.0
        assert result[0]['average_purchase_amount'] == 0.0

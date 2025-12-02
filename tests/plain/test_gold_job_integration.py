"""
Integration tests for plain/gold_job.py main function.
These tests execute the actual main() function with mocked AWS services.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch


@pytest.mark.integration
class TestPlainGoldJobIntegration:
    """Integration tests for Gold layer main function execution."""
    
    @patch('plain.gold_job.boto3.client')
    @patch('plain.gold_job.SparkSession')
    def test_should_execute_main_function_when_called(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() function executes without errors with mocked dependencies.
        
        Tests complete execution flow including Spark initialization, reading from Iceberg,
        and writing to DynamoDB.
        """
        # Arrange - Mock CloudWatch logs client and DynamoDB client
        mock_logs_client = MagicMock()
        mock_dynamodb_client = MagicMock()
        
        def boto3_client_factory(service_name, **kwargs):
            if service_name == 'logs':
                return mock_logs_client
            elif service_name == 'dynamodb':
                return mock_dynamodb_client
            return MagicMock()
        
        mock_boto3_client.side_effect = boto3_client_factory
        
        # Mock Spark session and components
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        # Mock Spark context and configuration
        mock_spark.sparkContext._jsc.sc().listJars.return_value = ["iceberg.jar"]
        mock_spark.sparkContext.getConf().get.return_value = "/path/to/jars"
        
        # Mock DataFrame read operations with sample data
        mock_df = MagicMock()
        mock_row1 = {'id': 1, 'name': 'Alice', 'amount': 50}
        mock_row2 = {'id': 2, 'name': 'Bob', 'amount': 75}
        mock_df.collect.return_value = [
            MagicMock(**mock_row1, __getitem__=lambda self, key: mock_row1[key]),
            MagicMock(**mock_row2, __getitem__=lambda self, key: mock_row2[key])
        ]
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        # Act - Import and execute main
        from plain.gold_job import main
        main()
        
        # Assert - Verify key components were called
        assert mock_boto3_client.call_count == 2  # logs and dynamodb clients
        assert mock_logs_client.put_log_events.call_count >= 4  # Multiple log events
        mock_spark.read.format.assert_called_with("iceberg")
        assert mock_dynamodb_client.put_item.call_count == 2  # Two rows inserted
        mock_spark.stop.assert_called_once()
    
    @patch('plain.gold_job.boto3.client')
    @patch('plain.gold_job.SparkSession')
    def test_should_write_all_records_to_dynamodb_when_processing_data(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() writes all DataFrame records to DynamoDB.
        
        Tests the iteration over DataFrame rows and DynamoDB put_item calls.
        """
        # Arrange
        mock_logs_client = MagicMock()
        mock_dynamodb_client = MagicMock()
        
        def boto3_client_factory(service_name, **kwargs):
            if service_name == 'logs':
                return mock_logs_client
            elif service_name == 'dynamodb':
                return mock_dynamodb_client
            return MagicMock()
        
        mock_boto3_client.side_effect = boto3_client_factory
        
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = []
        mock_spark.sparkContext.getConf().get.return_value = ""
        
        # Mock DataFrame with 3 records
        mock_df = MagicMock()
        mock_rows = [
            {'id': 1, 'name': 'Test1', 'amount': 10},
            {'id': 2, 'name': 'Test2', 'amount': 20},
            {'id': 3, 'name': 'Test3', 'amount': 30}
        ]
        mock_df.collect.return_value = [
            MagicMock(**row, __getitem__=lambda self, key, r=row: r[key])
            for row in mock_rows
        ]
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        # Act
        from plain.gold_job import main
        main()
        
        # Assert - Verify DynamoDB writes for all records
        assert mock_dynamodb_client.put_item.call_count == 3
        
        # Verify structure of put_item calls
        put_item_calls = mock_dynamodb_client.put_item.call_args_list
        for call in put_item_calls:
            assert 'TableName' in call[1]
            assert call[1]['TableName'] == 'gold_table_plain'
            assert 'Item' in call[1]
            item = call[1]['Item']
            assert 'id' in item
            assert 'name' in item
            assert 'amount' in item
    
    @patch('plain.gold_job.boto3.client')
    @patch('plain.gold_job.SparkSession')
    def test_should_log_each_insert_when_writing_to_dynamodb(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() logs each DynamoDB insert operation.
        
        Tests that CloudWatch logging occurs for each data row inserted.
        """
        # Arrange
        mock_logs_client = MagicMock()
        mock_dynamodb_client = MagicMock()
        
        def boto3_client_factory(service_name, **kwargs):
            if service_name == 'logs':
                return mock_logs_client
            elif service_name == 'dynamodb':
                return mock_dynamodb_client
            return MagicMock()
        
        mock_boto3_client.side_effect = boto3_client_factory
        
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = []
        mock_spark.sparkContext.getConf().get.return_value = ""
        
        mock_df = MagicMock()
        mock_row = {'id': 1, 'name': 'LogTest', 'amount': 99}
        mock_df.collect.return_value = [
            MagicMock(**mock_row, __getitem__=lambda self, key: mock_row[key])
        ]
        
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        # Act
        from plain.gold_job import main
        main()
        
        # Assert - Verify logging occurred for the insert
        log_calls = [str(call) for call in mock_logs_client.put_log_events.call_args_list]
        assert any("Inserted" in call and "gold_table_plain" in call for call in log_calls)

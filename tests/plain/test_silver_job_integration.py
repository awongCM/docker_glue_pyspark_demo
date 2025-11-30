"""
Integration tests for plain/silver_job.py main function.
These tests execute the actual main() function with mocked AWS services.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch


@pytest.mark.integration
class TestPlainSilverJobIntegration:
    """Integration tests for Silver layer main function execution."""
    
    @patch('plain.silver_job.boto3.client')
    @patch('plain.silver_job.SparkSession')
    def test_should_execute_main_function_when_called(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() function executes without errors with mocked dependencies.
        
        Tests complete execution flow including Spark initialization, database/table creation,
        data reading from S3, transformation, and writing to Iceberg.
        """
        # Arrange - Mock CloudWatch logs client
        mock_logs_client = MagicMock()
        mock_boto3_client.return_value = mock_logs_client
        
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
        mock_spark.sparkContext.getConf().get.return_value = "/path/to/iceberg/jars"
        
        # Mock SQL operations
        mock_spark.sql.return_value = MagicMock()
        
        # Mock DataFrame read operations
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.printSchema.return_value = None
        mock_df.show.return_value = None
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        # Mock writeTo for Iceberg
        mock_write_to = MagicMock()
        mock_write_to.append.return_value = None
        mock_df.writeTo.return_value = mock_write_to
        
        # Mock table read
        mock_table_df = MagicMock()
        mock_table_df.show.return_value = None
        mock_spark.table.return_value = mock_table_df
        
        # Act - Import and execute main
        from plain.silver_job import main
        main()
        
        # Assert - Verify key components were called
        mock_boto3_client.assert_called_once()
        assert mock_logs_client.put_log_events.call_count >= 5  # Multiple log events
        assert mock_spark.sql.call_count >= 2  # CREATE DATABASE and CREATE TABLE
        mock_spark.read.format.assert_called_with("parquet")
        mock_df.filter.assert_called_once()
        mock_df.writeTo.assert_called_once()
        mock_spark.stop.assert_called_once()
    
    @patch('plain.silver_job.boto3.client')
    @patch('plain.silver_job.SparkSession')
    def test_should_create_database_and_table_when_main_executes(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() creates Iceberg database and table.
        
        Tests that create_database_if_not_exists and create_table_if_not_exists are called.
        """
        # Arrange
        mock_logs_client = MagicMock()
        mock_boto3_client.return_value = mock_logs_client
        
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = []
        mock_spark.sparkContext.getConf().get.return_value = ""
        
        mock_spark.sql.return_value = MagicMock()
        
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.printSchema.return_value = None
        mock_df.show.return_value = None
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        mock_write_to = MagicMock()
        mock_df.writeTo.return_value = mock_write_to
        
        mock_table_df = MagicMock()
        mock_spark.table.return_value = mock_table_df
        
        # Act
        from plain.silver_job import main
        main()
        
        # Assert - Check SQL operations for database and table creation
        sql_calls = [str(call) for call in mock_spark.sql.call_args_list]
        assert any("CREATE DATABASE IF NOT EXISTS" in call for call in sql_calls)
        assert any("CREATE TABLE IF NOT EXISTS" in call for call in sql_calls)
    
    @patch('plain.silver_job.boto3.client')
    @patch('plain.silver_job.SparkSession')
    def test_should_filter_and_write_data_when_processing_bronze_data(
        self, mock_spark_session_builder, mock_boto3_client
    ):
        """
        Verify that main() filters data and writes to Iceberg table.
        
        Tests the data transformation (filter amount < 100) and Iceberg write.
        """
        # Arrange
        mock_logs_client = MagicMock()
        mock_boto3_client.return_value = mock_logs_client
        
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_builder.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        mock_spark.sparkContext._jsc.sc().listJars.return_value = []
        mock_spark.sparkContext.getConf().get.return_value = ""
        mock_spark.sql.return_value = MagicMock()
        
        mock_df = MagicMock()
        mock_filtered_df = MagicMock()
        mock_df.filter.return_value = mock_filtered_df
        mock_df.printSchema.return_value = None
        mock_df.show.return_value = None
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_spark.read = mock_reader
        
        mock_write_to = MagicMock()
        mock_write_to.append.return_value = None
        mock_filtered_df.writeTo.return_value = mock_write_to
        
        mock_table_df = MagicMock()
        mock_spark.table.return_value = mock_table_df
        
        # Act
        from plain.silver_job import main
        main()
        
        # Assert
        mock_df.filter.assert_called_once_with("amount < 100")
        mock_filtered_df.writeTo.assert_called_once()
        mock_write_to.append.assert_called_once()

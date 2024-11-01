
resource "aws_s3_bucket" "bronze-bucket" {
  bucket = "bronze-bucket"
  acl    = "private"
}

resource "aws_s3_bucket" "iceberg" {
  bucket = "iceberg"
  acl    = "private"
}

resource "aws_dynamodb_table" "dynamo_db_table_gold_table_plain" {
  name         = "gold_table_plain"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "N"
  }
}

resource "aws_dynamodb_table" "dynamo_db_table_gold_table_purchase_order" {
  name         = "gold_table_purchase_order"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "email"

  attribute {
    name = "email"
    type = "S"
  }

}

resource "aws_cloudwatch_log_group" "pyspark-logs" {
  name = "pyspark-logs"
}

resource "aws_cloudwatch_log_stream" "bronze-job-stream" {
  name           = "bronze-job-stream"
  log_group_name = aws_cloudwatch_log_group.pyspark-logs.name
}
resource "aws_cloudwatch_log_stream" "silver-job-stream" {
  name           = "silver-job-stream"
  log_group_name = aws_cloudwatch_log_group.pyspark-logs.name
}
resource "aws_cloudwatch_log_stream" "gold-job-stream" {
  name           = "gold-job-stream"
  log_group_name = aws_cloudwatch_log_group.pyspark-logs.name
}


resource "aws_cloudwatch_log_group" "pyspark-po-logs" {
  name = "pyspark-po-logs"
}
resource "aws_cloudwatch_log_stream" "bronze-job-po-stream" {
  name           = "bronze-job-po-stream"
  log_group_name = aws_cloudwatch_log_group.pyspark-po-logs.name
}
resource "aws_cloudwatch_log_stream" "silver-job-po-stream" {
  name           = "silver-job-po-stream"
  log_group_name = aws_cloudwatch_log_group.pyspark-po-logs.name
}
resource "aws_cloudwatch_log_stream" "gold-job-po-stream" {
  name           = "gold-job-po-stream"
  log_group_name = aws_cloudwatch_log_group.pyspark-po-logs.name
}

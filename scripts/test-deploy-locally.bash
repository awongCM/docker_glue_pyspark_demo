#!/bin/bash
set -e

# Local Deployment Testing Script
# Tests deployment process against LocalStack instead of real AWS

echo "🚀 Starting local deployment test..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
export ARTIFACTS_BUCKET=local-artifacts-bucket
export GLUE_SCRIPTS_BUCKET=local-glue-scripts-bucket

# Step 1: Start LocalStack
echo -e "${YELLOW}📦 Step 1: Starting LocalStack...${NC}"
docker-compose up -d localstack

# Wait for LocalStack to be ready
echo "Waiting for LocalStack..."
timeout=60
counter=0
until curl -s http://localhost:4566/_localstack/health | grep -q '"s3"' || [ $counter -eq $timeout ]; do
    echo -n "."
    sleep 2
    ((counter+=2))
done
echo ""
if [ $counter -eq $timeout ]; then
    echo -e "${RED}❌ LocalStack failed to start${NC}"
    exit 1
fi
echo -e "${GREEN}✅ LocalStack is ready${NC}"

# Step 2: Setup AWS resources
echo -e "${YELLOW}📦 Step 2: Creating S3 buckets...${NC}"
aws --endpoint-url=$AWS_ENDPOINT_URL s3 mb s3://${ARTIFACTS_BUCKET} 2>/dev/null || echo "Bucket already exists"
aws --endpoint-url=$AWS_ENDPOINT_URL s3 mb s3://${GLUE_SCRIPTS_BUCKET} 2>/dev/null || echo "Bucket already exists"
aws --endpoint-url=$AWS_ENDPOINT_URL s3 mb s3://bronze-bucket 2>/dev/null || echo "Bucket already exists"
aws --endpoint-url=$AWS_ENDPOINT_URL s3 mb s3://iceberg 2>/dev/null || echo "Bucket already exists"

echo -e "${YELLOW}📦 Step 3: Creating DynamoDB tables...${NC}"
aws --endpoint-url=$AWS_ENDPOINT_URL dynamodb create-table \
    --table-name gold_table_plain \
    --attribute-definitions AttributeName=id,AttributeType=N \
    --key-schema AttributeName=id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    2>/dev/null || echo "Table already exists"

aws --endpoint-url=$AWS_ENDPOINT_URL dynamodb create-table \
    --table-name gold_table_po \
    --attribute-definitions AttributeName=id,AttributeType=N \
    --key-schema AttributeName=id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    2>/dev/null || echo "Table already exists"

echo -e "${YELLOW}📦 Step 4: Creating CloudWatch log groups...${NC}"
aws --endpoint-url=$AWS_ENDPOINT_URL logs create-log-group \
    --log-group-name /aws/glue/jobs/pyspark-logs 2>/dev/null || echo "Log group already exists"

# Step 3: Build Poetry artifacts
echo -e "${YELLOW}🏗️  Step 5: Building Poetry artifacts...${NC}"
if command -v poetry &> /dev/null; then
    poetry build
    echo -e "${GREEN}✅ Poetry build complete${NC}"
    ls -lh dist/
else
    echo -e "${YELLOW}⚠️  Poetry not installed, skipping build${NC}"
fi

# Step 4: Upload artifacts to LocalStack S3
if [ -d "dist" ] && [ "$(ls -A dist)" ]; then
    echo -e "${YELLOW}📤 Step 6: Uploading artifacts to LocalStack S3...${NC}"
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    
    for file in dist/*; do
        if [ -f "$file" ]; then
            echo "Uploading $(basename $file)..."
            aws --endpoint-url=$AWS_ENDPOINT_URL s3 cp "$file" \
                "s3://${ARTIFACTS_BUCKET}/artifacts/${TIMESTAMP}/$(basename $file)"
        fi
    done
    
    echo "latest" > /tmp/latest.txt
    aws --endpoint-url=$AWS_ENDPOINT_URL s3 cp /tmp/latest.txt \
        "s3://${ARTIFACTS_BUCKET}/artifacts/latest.txt"
    
    echo -e "${GREEN}✅ Artifacts uploaded${NC}"
    aws --endpoint-url=$AWS_ENDPOINT_URL s3 ls s3://${ARTIFACTS_BUCKET}/artifacts/${TIMESTAMP}/
fi

# Step 5: Upload Glue job scripts
echo -e "${YELLOW}📤 Step 7: Uploading Glue job scripts...${NC}"
JOBS=(
    "plain/bronze_job.py:plain-bronze-job"
    "plain/silver_job.py:plain-silver-job"
    "plain/gold_job.py:plain-gold-job"
    "purchase_order/bronze_job.py:po-bronze-job"
    "purchase_order/silver_job.py:po-silver-job"
    "purchase_order/gold_job.py:po-gold-job"
)

for job_info in "${JOBS[@]}"; do
    IFS=':' read -r script_path job_name <<< "$job_info"
    if [ -f "$script_path" ]; then
        echo "Uploading $script_path as ${job_name}.py..."
        aws --endpoint-url=$AWS_ENDPOINT_URL s3 cp "$script_path" \
            "s3://${GLUE_SCRIPTS_BUCKET}/scripts/${job_name}.py"
    else
        echo -e "${YELLOW}⚠️  File not found: $script_path${NC}"
    fi
done

echo -e "${GREEN}✅ Glue scripts uploaded${NC}"
aws --endpoint-url=$AWS_ENDPOINT_URL s3 ls s3://${GLUE_SCRIPTS_BUCKET}/scripts/

# Step 6: Verify deployment
echo -e "${YELLOW}🔍 Step 8: Verifying deployment...${NC}"

echo "S3 Buckets:"
aws --endpoint-url=$AWS_ENDPOINT_URL s3 ls

echo ""
echo "DynamoDB Tables:"
aws --endpoint-url=$AWS_ENDPOINT_URL dynamodb list-tables

echo ""
echo "CloudWatch Log Groups:"
aws --endpoint-url=$AWS_ENDPOINT_URL logs describe-log-groups --query 'logGroups[*].logGroupName' --output table

echo ""
echo -e "${GREEN}✅ All steps completed successfully!${NC}"
echo ""
echo "================================================"
echo "🎉 Local Deployment Test Summary"
echo "================================================"
echo "LocalStack Endpoint: $AWS_ENDPOINT_URL"
echo "Artifacts Bucket: s3://${ARTIFACTS_BUCKET}"
echo "Scripts Bucket: s3://${GLUE_SCRIPTS_BUCKET}"
echo "DynamoDB Tables: gold_table_plain, gold_table_po"
echo "================================================"
echo ""
echo "To view resources:"
echo "  aws --endpoint-url=$AWS_ENDPOINT_URL s3 ls s3://${ARTIFACTS_BUCKET}/ --recursive"
echo "  aws --endpoint-url=$AWS_ENDPOINT_URL dynamodb scan --table-name gold_table_plain"
echo ""
echo "To stop LocalStack:"
echo "  docker-compose down"
echo ""

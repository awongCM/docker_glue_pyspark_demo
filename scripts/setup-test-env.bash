#!/usr/bin/env bash
#
# Quick setup script to prepare Docker environment for testing
#

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_header() {
    echo -e "${BLUE}==>${NC} ${GREEN}$1${NC}"
}

print_info() {
    echo -e "${YELLOW}$1${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

print_header "Docker Test Environment Setup"
echo ""

# Step 1: Restart containers
print_info "Step 1: Restarting containers to mount test volumes..."
docker-compose down
./scripts/start-containers.bash

echo ""
print_info "Waiting for containers to be ready..."
sleep 5

# Step 2: Verify container is running
print_info "Step 2: Verifying glue-pyspark-poc container..."
if docker ps --format '{{.Names}}' | grep -q "^glue-pyspark-poc$"; then
    print_header "✓ Container is running"
else
    print_error "Container is not running. Please check docker-compose logs."
    exit 1
fi

# Step 3: Verify volumes are mounted
print_info "Step 3: Verifying volume mounts..."
if docker exec glue-pyspark-poc test -d /app/tests; then
    print_header "✓ Test directory mounted"
else
    print_error "Test directory not mounted. Check docker-compose.yaml volumes."
    exit 1
fi

# Step 4: Install dependencies
print_info "Step 4: Installing test dependencies inside container..."
docker exec -it glue-pyspark-poc bash -c "cd /app && poetry install"

echo ""
print_header "✓ Setup Complete!"
echo ""
print_info "You can now run tests:"
echo ""
echo "  ./scripts/run-tests-docker.bash all        # Run all tests"
echo "  ./scripts/run-tests-docker.bash plain      # Plain pipeline"
echo "  ./scripts/run-tests-docker.bash coverage   # With coverage report"
echo "  ./scripts/run-tests-docker.bash shell      # Interactive shell"
echo ""
print_info "For more options: ./scripts/run-tests-docker.bash help"
echo ""

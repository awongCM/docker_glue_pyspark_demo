#!/usr/bin/env bash
#
# Run tests inside the glue-pyspark-poc Docker container
# This script executes tests within the container environment where awsglue is available
#

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

CONTAINER_NAME="glue-pyspark-poc"

print_header() {
    echo -e "${BLUE}==>${NC} ${GREEN}$1${NC}"
}

print_info() {
    echo -e "${YELLOW}$1${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

# Check if container is running
check_container() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        print_error "Container '${CONTAINER_NAME}' is not running"
        print_info "Start the container first:"
        print_info "  ./scripts/start-containers.bash"
        exit 1
    fi
}

# Display usage
usage() {
    cat << EOF
Usage: $0 [OPTION]

Run tests inside the glue-pyspark-poc Docker container

OPTIONS:
    all             Run all tests with coverage
    plain           Run plain pipeline tests only
    po              Run purchase_order pipeline tests only
    bronze          Run all bronze layer tests
    silver          Run all silver layer tests
    gold            Run all gold layer tests
    unit            Run only unit tests
    fast            Run tests without coverage (faster)
    coverage        Generate HTML coverage report
    verbose         Run tests with verbose output
    clean           Remove test artifacts and cache
    shell           Open bash shell in container
    help            Show this help message

EXAMPLES:
    $0 all          # Run all tests with coverage
    $0 plain        # Test only plain pipeline
    $0 bronze       # Test all bronze layer jobs
    $0 shell        # Open shell in container for manual testing

EOF
    exit 0
}

# Execute command in container
run_in_container() {
    docker exec -it ${CONTAINER_NAME} bash -c "$1"
}

# Run all tests with coverage
run_all() {
    check_container
    print_header "Running all tests with coverage inside container..."
    run_in_container "cd /app && poetry run pytest -v"
}

# Run plain pipeline tests
run_plain() {
    check_container
    print_header "Running plain pipeline tests inside container..."
    run_in_container "cd /app && poetry run pytest tests/plain/ -v"
}

# Run purchase_order pipeline tests
run_po() {
    check_container
    print_header "Running purchase_order pipeline tests inside container..."
    run_in_container "cd /app && poetry run pytest tests/purchase_order/ -v"
}

# Run all bronze layer tests
run_bronze() {
    check_container
    print_header "Running all bronze layer tests inside container..."
    run_in_container "cd /app && poetry run pytest tests/plain/test_bronze_job.py tests/purchase_order/test_bronze_job.py -v"
}

# Run all silver layer tests
run_silver() {
    check_container
    print_header "Running all silver layer tests inside container..."
    run_in_container "cd /app && poetry run pytest tests/plain/test_silver_job.py tests/purchase_order/test_silver_job.py -v"
}

# Run all gold layer tests
run_gold() {
    check_container
    print_header "Running all gold layer tests inside container..."
    run_in_container "cd /app && poetry run pytest tests/plain/test_gold_job.py tests/purchase_order/test_gold_job.py -v"
}

# Run only unit tests
run_unit() {
    check_container
    print_header "Running unit tests inside container..."
    run_in_container "cd /app && poetry run pytest -m unit -v"
}

# Run tests fast (no coverage)
run_fast() {
    check_container
    print_header "Running tests without coverage (fast mode) inside container..."
    run_in_container "cd /app && poetry run pytest --no-cov -v"
}

# Generate and open coverage report
run_coverage() {
    check_container
    print_header "Generating HTML coverage report inside container..."
    run_in_container "cd /app && poetry run pytest --cov=plain --cov=purchase_order --cov-report=html --cov-report=term"
    
    print_info "Coverage report generated at htmlcov/index.html"
    print_info "To view, run: open htmlcov/index.html"
}

# Clean test artifacts
run_clean() {
    check_container
    print_header "Cleaning test artifacts inside container..."
    run_in_container "cd /app && rm -rf .pytest_cache htmlcov .coverage **/__pycache__ **/*.pyc"
    print_info "✓ Cleaned test artifacts"
}

# Run tests with verbose output
run_verbose() {
    check_container
    print_header "Running tests with verbose output inside container..."
    run_in_container "cd /app && poetry run pytest -vv -s"
}

# Open shell in container
run_shell() {
    check_container
    print_header "Opening bash shell in ${CONTAINER_NAME}..."
    print_info "You can now run tests manually, e.g.:"
    print_info "  cd /app"
    print_info "  poetry run pytest tests/plain/"
    print_info "  exit"
    docker exec -it ${CONTAINER_NAME} bash
}

# Main script logic
case "${1:-}" in
    all)
        run_all
        ;;
    plain)
        run_plain
        ;;
    po)
        run_po
        ;;
    bronze)
        run_bronze
        ;;
    silver)
        run_silver
        ;;
    gold)
        run_gold
        ;;
    unit)
        run_unit
        ;;
    fast)
        run_fast
        ;;
    coverage)
        run_coverage
        ;;
    verbose)
        run_verbose
        ;;
    clean)
        run_clean
        ;;
    shell)
        run_shell
        ;;
    help|--help|-h)
        usage
        ;;
    "")
        print_info "No option provided. Running all tests..."
        run_all
        ;;
    *)
        echo "Unknown option: $1"
        echo "Run '$0 help' for usage information"
        exit 1
        ;;
esac

print_header "Done! ✓"

#!/usr/bin/env bash
#
# ⚠️  DEPRECATED: This script runs tests on the host machine.
# ⚠️  Use run-tests-docker.bash instead to run tests inside the container.
# ⚠️  Tests require 'awsglue' module which is only available in the Docker container.
#
# Quick test runner script for docker_glue_pyspark_demo
# Provides convenient shortcuts for common test scenarios
#

set -e

echo "⚠️  WARNING: You are running tests on the host machine."
echo "⚠️  Tests require 'awsglue' module and will likely fail."
echo "⚠️  Use './scripts/run-tests-docker.bash' instead to run tests inside the container."
echo ""
read -p "Continue anyway? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Exiting. Run './scripts/run-tests-docker.bash all' to run tests properly."
    exit 1
fi
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}==>${NC} ${GREEN}$1${NC}"
}

print_info() {
    echo -e "${YELLOW}$1${NC}"
}

# Display usage
usage() {
    cat << EOF
Usage: $0 [OPTION]

Test runner for docker_glue_pyspark_demo project

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
    watch           Run tests in watch mode (requires pytest-watch)
    clean           Remove test artifacts and cache
    help            Show this help message

EXAMPLES:
    $0 all          # Run all tests with coverage
    $0 plain        # Test only plain pipeline
    $0 bronze       # Test all bronze layer jobs
    $0 fast         # Quick run without coverage
    $0 coverage     # Generate and open coverage report

EOF
    exit 0
}

# Run all tests with coverage
run_all() {
    print_header "Running all tests with coverage..."
    poetry run pytest -v
}

# Run plain pipeline tests
run_plain() {
    print_header "Running plain pipeline tests..."
    poetry run pytest tests/plain/ -v
}

# Run purchase_order pipeline tests
run_po() {
    print_header "Running purchase_order pipeline tests..."
    poetry run pytest tests/purchase_order/ -v
}

# Run all bronze layer tests
run_bronze() {
    print_header "Running all bronze layer tests..."
    poetry run pytest tests/plain/test_bronze_job.py tests/purchase_order/test_bronze_job.py -v
}

# Run all silver layer tests
run_silver() {
    print_header "Running all silver layer tests..."
    poetry run pytest tests/plain/test_silver_job.py tests/purchase_order/test_silver_job.py -v
}

# Run all gold layer tests
run_gold() {
    print_header "Running all gold layer tests..."
    poetry run pytest tests/plain/test_gold_job.py tests/purchase_order/test_gold_job.py -v
}

# Run only unit tests
run_unit() {
    print_header "Running unit tests..."
    poetry run pytest -m unit -v
}

# Run tests fast (no coverage)
run_fast() {
    print_header "Running tests without coverage (fast mode)..."
    poetry run pytest --no-cov -v
}

# Generate and open coverage report
run_coverage() {
    print_header "Generating HTML coverage report..."
    poetry run pytest --cov=plain --cov=purchase_order --cov-report=html --cov-report=term
    
    if [ -f "htmlcov/index.html" ]; then
        print_info "Opening coverage report in browser..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            open htmlcov/index.html
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            xdg-open htmlcov/index.html 2>/dev/null || echo "Please open htmlcov/index.html manually"
        else
            echo "Please open htmlcov/index.html manually"
        fi
    fi
}

# Run tests in watch mode
run_watch() {
    print_header "Running tests in watch mode..."
    print_info "Install pytest-watch: poetry add --group dev pytest-watch"
    poetry run ptw -- -v
}

# Clean test artifacts
run_clean() {
    print_header "Cleaning test artifacts..."
    rm -rf .pytest_cache
    rm -rf htmlcov
    rm -rf .coverage
    rm -rf **/__pycache__
    rm -rf **/*.pyc
    print_info "✓ Cleaned test artifacts"
}

# Run tests with verbose output
run_verbose() {
    print_header "Running tests with verbose output..."
    poetry run pytest -vv -s
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
    watch)
        run_watch
        ;;
    verbose)
        run_verbose
        ;;
    clean)
        run_clean
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

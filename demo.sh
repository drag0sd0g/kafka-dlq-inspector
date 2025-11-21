#!/bin/bash

################################################################################
# Kafka DLQ Inspector - Demo Script
# 
# This script demonstrates all the functionality of the Kafka DLQ Inspector
# application including:
# - Starting all dependencies (Kafka and Schema Registry in KRaft mode)
# - Building and running the application
# - Creating test DLQ topics with sample data
# - Demonstrating CLI commands
# - Demonstrating REST API endpoints
#
# Prerequisites:
# - Docker and Docker Compose installed
# - Java 21+ installed
# - curl and jq installed
################################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_PORT=8080
KAFKA_PORT=9092
APP_JAR="build/libs/kafka-dlq-inspector-0.1.0-all.jar"
APP_PID_FILE="/tmp/kafka-dlq-inspector.pid"
TEST_TOPIC_PREFIX="test-service"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
}

# Check prerequisites
check_prerequisites() {
    log_section "Checking Prerequisites"
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    log_success "Docker is installed"
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    log_success "Docker Compose is installed"
    
    if ! command -v java &> /dev/null; then
        log_error "Java is not installed. Please install Java 21+ first."
        exit 1
    fi
    log_success "Java is installed: $(java -version 2>&1 | head -n 1)"
    
    if ! command -v curl &> /dev/null; then
        log_error "curl is not installed. Please install curl first."
        exit 1
    fi
    log_success "curl is installed"
    
    if ! command -v jq &> /dev/null; then
        log_warning "jq is not installed. API responses will be harder to read."
        log_info "Install jq for better output: sudo apt-get install jq (Linux) or brew install jq (Mac)"
    else
        log_success "jq is installed"
    fi
}

# Build the application
build_application() {
    log_section "Building Application"
    
    if [ -f "$APP_JAR" ]; then
        log_info "Application JAR already exists at $APP_JAR"
        read -p "Do you want to rebuild? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "Skipping build"
            return
        fi
    fi
    
    log_info "Building application with Gradle..."
    ./gradlew clean build -x test
    
    if [ -f "$APP_JAR" ]; then
        log_success "Application built successfully"
    else
        log_error "Failed to build application"
        exit 1
    fi
}

# Start Docker services
start_docker_services() {
    log_section "Starting Docker Services"
    
    log_info "Starting Kafka and Schema Registry..."
    docker compose -f docker/docker-compose.yml up -d kafka schema-registry
    
    log_info "Waiting for Kafka to be ready..."
    sleep 10
    
    local retries=30
    while [ $retries -gt 0 ]; do
        if docker compose -f docker/docker-compose.yml exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; then
            log_success "Kafka is ready"
            break
        fi
        log_info "Waiting for Kafka... ($retries attempts remaining)"
        sleep 2
        retries=$((retries-1))
    done
    
    if [ $retries -eq 0 ]; then
        log_error "Kafka failed to start"
        exit 1
    fi
    
    log_success "All Docker services are running"
}

# Create test DLQ topics and populate with sample data
create_test_data() {
    log_section "Creating Test Data"
    
    log_info "Creating DLQ topics with sample messages..."
    
    # Create topics
    docker compose -f docker/docker-compose.yml exec -T kafka kafka-topics \
        --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --topic ${TEST_TOPIC_PREFIX}-orders.dlq \
        --partitions 2 \
        --replication-factor 1
    
    docker compose -f docker/docker-compose.yml exec -T kafka kafka-topics \
        --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --topic ${TEST_TOPIC_PREFIX}-payments.dlq \
        --partitions 1 \
        --replication-factor 1
    
    docker compose -f docker/docker-compose.yml exec -T kafka kafka-topics \
        --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --topic ${TEST_TOPIC_PREFIX}-orders.retry \
        --partitions 1 \
        --replication-factor 1
    
    log_success "Topics created"
    
    # Produce sample messages to DLQ topics
    log_info "Producing sample messages to DLQ topics..."
    
    # Orders DLQ messages with various exception types
    for i in {1..10}; do
        local exception_type=""
        case $((i % 3)) in
            0) exception_type="NullPointerException" ;;
            1) exception_type="JsonParseException" ;;
            2) exception_type="IllegalArgumentException" ;;
        esac
        
        echo "{\"orderId\": \"order-$i\", \"customerId\": \"cust-$((i % 5))\", \"amount\": $((100 + i * 10)), \"exception\": \"$exception_type\", \"timestamp\": $(date +%s)000}" | \
        docker compose -f docker/docker-compose.yml exec -T kafka kafka-console-producer \
            --bootstrap-server localhost:9092 \
            --topic ${TEST_TOPIC_PREFIX}-orders.dlq \
            --property "parse.key=false" 2>/dev/null
    done
    
    # Payments DLQ messages
    for i in {1..5}; do
        echo "{\"paymentId\": \"payment-$i\", \"orderId\": \"order-$i\", \"status\": \"failed\", \"error\": \"PaymentGatewayException\", \"timestamp\": $(date +%s)000}" | \
        docker compose -f docker/docker-compose.yml exec -T kafka kafka-console-producer \
            --bootstrap-server localhost:9092 \
            --topic ${TEST_TOPIC_PREFIX}-payments.dlq \
            --property "parse.key=false" 2>/dev/null
    done
    
    log_success "Sample messages produced to DLQ topics"
    
    # List created topics
    log_info "Created topics:"
    docker compose -f docker/docker-compose.yml exec -T kafka kafka-topics \
        --list \
        --bootstrap-server localhost:9092 | grep -E "dlq|retry"
}

# Start the application
start_application() {
    log_section "Starting Application"
    
    if [ -f "$APP_PID_FILE" ]; then
        local pid=$(cat "$APP_PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_warning "Application is already running (PID: $pid)"
            return
        fi
    fi
    
    log_info "Starting Kafka DLQ Inspector..."
    java -jar "$APP_JAR" > /tmp/kafka-dlq-inspector.log 2>&1 &
    echo $! > "$APP_PID_FILE"
    
    log_info "Waiting for application to start..."
    local retries=30
    while [ $retries -gt 0 ]; do
        if curl -s "http://localhost:$APP_PORT/actuator/health" > /dev/null 2>&1; then
            log_success "Application is running on port $APP_PORT"
            return
        fi
        log_info "Waiting for application... ($retries attempts remaining)"
        sleep 2
        retries=$((retries-1))
    done
    
    log_error "Application failed to start. Check logs at /tmp/kafka-dlq-inspector.log"
    exit 1
}

# Demonstrate CLI commands
demo_cli_commands() {
    log_section "Demonstrating CLI Commands"
    
    log_info "1. List all DLQ topics"
    echo "Command: java -Dcli.enabled=true -jar $APP_JAR dlq list-topics"
    java -Dcli.enabled=true -jar "$APP_JAR" dlq list-topics
    echo ""
    
    log_info "2. Show messages from a specific topic"
    echo "Command: java -Dcli.enabled=true -jar $APP_JAR dlq show --topic ${TEST_TOPIC_PREFIX}-orders.dlq --limit 5"
    java -Dcli.enabled=true -jar "$APP_JAR" dlq show --topic "${TEST_TOPIC_PREFIX}-orders.dlq" --limit 5
    echo ""
    
    log_info "3. Show aggregated statistics"
    echo "Command: java -Dcli.enabled=true -jar $APP_JAR dlq aggregate"
    java -Dcli.enabled=true -jar "$APP_JAR" dlq aggregate
    echo ""
    
    log_info "4. Replay messages (dry-run mode)"
    echo "Command: java -Dcli.enabled=true -jar $APP_JAR dlq replay --source ${TEST_TOPIC_PREFIX}-orders.dlq --destination ${TEST_TOPIC_PREFIX}-orders.retry --dry-run true"
    java -Dcli.enabled=true -jar "$APP_JAR" dlq replay --source "${TEST_TOPIC_PREFIX}-orders.dlq" --destination "${TEST_TOPIC_PREFIX}-orders.retry" --dry-run true
    echo ""
    
    log_info "5. Export messages to JSON file"
    echo "Command: java -Dcli.enabled=true -jar $APP_JAR dlq export --topic ${TEST_TOPIC_PREFIX}-orders.dlq --file /tmp/orders-dlq.json"
    java -Dcli.enabled=true -jar "$APP_JAR" dlq export --topic "${TEST_TOPIC_PREFIX}-orders.dlq" --file /tmp/orders-dlq.json
    echo ""
    
    log_success "CLI commands demonstration completed"
}

# Demonstrate REST API endpoints
demo_rest_api() {
    log_section "Demonstrating REST API Endpoints"
    
    local base_url="http://localhost:$APP_PORT"
    
    log_info "1. Health Check"
    echo "GET $base_url/actuator/health"
    curl -s "$base_url/actuator/health" | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_info "2. List all DLQ topics"
    echo "GET $base_url/api/topics"
    curl -s "$base_url/api/topics" | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_info "3. List messages from a topic (paginated)"
    echo "GET $base_url/api/topics/${TEST_TOPIC_PREFIX}-orders.dlq/messages?page=0&size=5"
    curl -s "$base_url/api/topics/${TEST_TOPIC_PREFIX}-orders.dlq/messages?page=0&size=5" | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_info "4. Get a specific message"
    echo "GET $base_url/api/topics/${TEST_TOPIC_PREFIX}-orders.dlq/messages/0/0"
    curl -s "$base_url/api/topics/${TEST_TOPIC_PREFIX}-orders.dlq/messages/0/0" | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_info "5. Compute aggregations"
    echo "POST $base_url/api/aggregations"
    curl -s -X POST "$base_url/api/aggregations" \
        -H "Content-Type: application/json" \
        -d '{"topics": ["'"${TEST_TOPIC_PREFIX}"'-orders.dlq"]}' | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_info "6. Export messages to JSON"
    echo "POST $base_url/api/export/json"
    curl -s -X POST "$base_url/api/export/json" \
        -H "Content-Type: application/json" \
        -d '{"topics": ["'"${TEST_TOPIC_PREFIX}"'-orders.dlq"]}' \
        -o /tmp/api-export.json
    log_success "Exported to /tmp/api-export.json ($(wc -c < /tmp/api-export.json) bytes)"
    echo ""
    
    log_info "7. Export messages to CSV"
    echo "POST $base_url/api/export/csv"
    curl -s -X POST "$base_url/api/export/csv" \
        -H "Content-Type: application/json" \
        -d '{"topics": ["'"${TEST_TOPIC_PREFIX}"'-payments.dlq"]}' \
        -o /tmp/api-export.csv
    log_success "Exported to /tmp/api-export.csv ($(wc -c < /tmp/api-export.csv) bytes)"
    echo ""
    
    log_info "8. Replay messages (dry-run)"
    echo "POST $base_url/api/replay"
    curl -s -X POST "$base_url/api/replay" \
        -H "Content-Type: application/json" \
        -d '{
            "cluster": "local",
            "sourceTopic": "'"${TEST_TOPIC_PREFIX}"'-orders.dlq",
            "destinationTopic": "'"${TEST_TOPIC_PREFIX}"'-orders.retry",
            "dryRun": true,
            "filters": {"topics": ["'"${TEST_TOPIC_PREFIX}"'-orders.dlq"]}
        }' | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_info "9. Prometheus metrics"
    echo "GET $base_url/actuator/prometheus"
    curl -s "$base_url/actuator/prometheus" | grep -E "^dlq_" | head -5
    echo "... (showing first 5 DLQ metrics)"
    echo ""
    
    log_info "10. OpenAPI documentation"
    log_success "API documentation available at: $base_url/swagger-ui.html"
    log_success "OpenAPI spec available at: $base_url/v3/api-docs"
    echo ""
    
    log_success "REST API demonstration completed"
}

# Advanced scenarios
demo_advanced_scenarios() {
    log_section "Advanced Usage Scenarios"
    
    local base_url="http://localhost:$APP_PORT"
    
    log_info "Scenario 1: Filter messages by partition"
    echo "GET $base_url/api/topics/${TEST_TOPIC_PREFIX}-orders.dlq/messages?partition=0&size=3"
    curl -s "$base_url/api/topics/${TEST_TOPIC_PREFIX}-orders.dlq/messages?partition=0&size=3" | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_info "Scenario 2: Filter messages by offset range"
    echo "GET $base_url/api/topics/${TEST_TOPIC_PREFIX}-orders.dlq/messages?offsetFrom=0&offsetTo=2"
    curl -s "$base_url/api/topics/${TEST_TOPIC_PREFIX}-orders.dlq/messages?offsetFrom=0&offsetTo=2" | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_info "Scenario 3: Aggregations with filters"
    echo "POST $base_url/api/aggregations (with partition filter)"
    curl -s -X POST "$base_url/api/aggregations" \
        -H "Content-Type: application/json" \
        -d '{"topics": ["'"${TEST_TOPIC_PREFIX}"'-orders.dlq"], "partitions": [0]}' | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_info "Scenario 4: Search messages with regex (if supported)"
    echo "POST $base_url/api/aggregations (with payload regex)"
    curl -s -X POST "$base_url/api/aggregations" \
        -H "Content-Type: application/json" \
        -d '{"topics": ["'"${TEST_TOPIC_PREFIX}"'-orders.dlq"], "payloadRegex": ".*order-[0-5].*"}' | if command -v jq &> /dev/null; then jq .; else cat; fi
    echo -e "\n"
    
    log_success "Advanced scenarios completed"
}

# Summary
show_summary() {
    log_section "Demo Summary"
    
    echo "✅ Services Status:"
    echo "   - Kafka: Running on localhost:9092 (KRaft mode)"
    echo "   - Schema Registry: Running on localhost:8081"
    echo "   - Application: Running on localhost:$APP_PORT"
    echo ""
    echo "✅ Test Data Created:"
    echo "   - ${TEST_TOPIC_PREFIX}-orders.dlq (10 messages, 2 partitions)"
    echo "   - ${TEST_TOPIC_PREFIX}-payments.dlq (5 messages, 1 partition)"
    echo "   - ${TEST_TOPIC_PREFIX}-orders.retry (empty, for replay testing)"
    echo ""
    echo "✅ Demonstrated Features:"
    echo "   - Topic discovery"
    echo "   - Message inspection and pagination"
    echo "   - Aggregations and statistics"
    echo "   - Message replay (dry-run)"
    echo "   - Export to JSON and CSV"
    echo "   - CLI commands"
    echo "   - REST API endpoints"
    echo "   - Prometheus metrics"
    echo ""
    echo "📚 Additional Resources:"
    echo "   - API Documentation: http://localhost:$APP_PORT/swagger-ui.html"
    echo "   - Health Endpoint: http://localhost:$APP_PORT/actuator/health"
    echo "   - Metrics: http://localhost:$APP_PORT/actuator/prometheus"
    echo "   - Application Logs: /tmp/kafka-dlq-inspector.log"
    echo ""
    echo "🛠️  To stop all services, run:"
    echo "   ./demo.sh stop"
    echo ""
}

# Stop all services
stop_services() {
    log_section "Stopping Services"
    
    if [ -f "$APP_PID_FILE" ]; then
        local pid=$(cat "$APP_PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Stopping application (PID: $pid)..."
            kill "$pid"
            rm -f "$APP_PID_FILE"
            log_success "Application stopped"
        fi
    fi
    
    log_info "Stopping Docker services..."
    docker compose -f docker/docker-compose.yml down
    log_success "Docker services stopped"
    
    log_info "Cleaning up temporary files..."
    rm -f /tmp/orders-dlq.json /tmp/api-export.json /tmp/api-export.csv
    log_success "Cleanup completed"
}

# Main execution
main() {
    case "${1:-demo}" in
        stop)
            stop_services
            ;;
        demo)
            check_prerequisites
            build_application
            start_docker_services
            create_test_data
            start_application
            
            # Wait a bit for application to fully initialize
            sleep 5
            
            demo_cli_commands
            demo_rest_api
            demo_advanced_scenarios
            show_summary
            ;;
        *)
            echo "Usage: $0 [demo|stop]"
            echo "  demo - Run the full demonstration (default)"
            echo "  stop - Stop all services and cleanup"
            exit 1
            ;;
    esac
}

main "$@"

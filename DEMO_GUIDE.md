# Demo Script Guide

This guide explains how to use the `demo.sh` script to demonstrate all features of the Kafka DLQ Inspector.

## Quick Start

Run the full demo:
```bash
./demo.sh
```

Stop all services:
```bash
./demo.sh stop
```

## What the Demo Does

The demo script performs the following steps automatically:

### 1. Prerequisites Check
- Verifies Docker is installed
- Verifies Java 21+ is installed
- Verifies curl is installed
- Checks for jq (optional, for better JSON output)

### 2. Build Application
- Builds the application JAR using Gradle
- Skips rebuild if JAR already exists (with option to rebuild)

### 3. Start Docker Services
- Starts Zookeeper on port 2181
- Starts Kafka on port 9092
- Starts Schema Registry on port 8081
- Waits for all services to be ready

### 4. Create Test Data
- Creates DLQ topics:
  - `test-service-orders.dlq` (2 partitions, 10 messages)
  - `test-service-payments.dlq` (1 partition, 5 messages)
  - `test-service-orders.retry` (1 partition, empty for replay testing)
- Populates topics with sample JSON messages
- Messages include various exception types for testing

### 5. Start Application
- Starts the Kafka DLQ Inspector application
- Waits for application to be healthy
- Logs output to `/tmp/kafka-dlq-inspector.log`

### 6. Demonstrate CLI Commands
Shows examples of all CLI commands:
- `list-topics` - List all DLQ topics
- `show` - Display messages from a topic
- `aggregate` - Show aggregated statistics
- `replay` - Replay messages (dry-run mode)
- `export` - Export messages to JSON file

### 7. Demonstrate REST API
Shows examples of all REST API endpoints:
- GET `/actuator/health` - Health check
- GET `/api/topics` - List topics
- GET `/api/topics/{topic}/messages` - List messages with pagination
- GET `/api/topics/{topic}/messages/{partition}/{offset}` - Get specific message
- POST `/api/aggregations` - Compute aggregations
- POST `/api/export/json` - Export to JSON
- POST `/api/export/csv` - Export to CSV
- POST `/api/replay` - Replay messages
- GET `/actuator/prometheus` - Prometheus metrics
- References to OpenAPI documentation

### 8. Advanced Scenarios
Demonstrates advanced filtering capabilities:
- Filter by partition
- Filter by offset range
- Aggregations with filters
- Search with regex patterns

### 9. Summary
Displays a summary of:
- Running services
- Created test data
- Demonstrated features
- Useful URLs and commands

## Customization

You can customize the script by modifying these variables at the top of the script:

```bash
APP_PORT=8080              # Application port
KAFKA_PORT=9092            # Kafka broker port
APP_JAR="..."              # Path to application JAR
TEST_TOPIC_PREFIX="test-service"  # Prefix for test topics
```

## Troubleshooting

### Docker not available
If you get "Docker is not installed" error:
- Install Docker Desktop (Mac/Windows)
- Or install Docker Engine (Linux)

### Port conflicts
If ports 8080, 9092, 2181, or 8081 are already in use:
- Stop conflicting services
- Or modify port numbers in the script

### Application fails to start
Check the application logs:
```bash
tail -f /tmp/kafka-dlq-inspector.log
```

### Services don't stop cleanly
Manually stop Docker services:
```bash
docker compose -f docker/docker-compose.yml down -v
```

Kill the application process:
```bash
kill $(cat /tmp/kafka-dlq-inspector.pid)
rm /tmp/kafka-dlq-inspector.pid
```

## Manual Testing

If you prefer to run steps manually:

1. Start Docker services:
```bash
docker compose -f docker/docker-compose.yml up -d zookeeper kafka schema-registry
```

2. Build application:
```bash
./gradlew clean build shadowJar
```

3. Start application:
```bash
java -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar
```

4. Create test topics and data using Kafka CLI tools or the demo script's `create_test_data()` function

5. Test CLI commands:
```bash
java -Dcli.enabled=true -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq list-topics
```

6. Test REST API:
```bash
curl http://localhost:8080/api/topics | jq .
```

## Test Scenarios

### Scenario 1: Topic Discovery
**Goal**: Verify DLQ topics are discovered

**Steps**:
1. Run `./demo.sh` to create test topics
2. Use CLI: `java -Dcli.enabled=true -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq list-topics`
3. Or use API: `curl http://localhost:8080/api/topics`

**Expected**: Should see `test-service-orders.dlq` and `test-service-payments.dlq`

### Scenario 2: Message Inspection
**Goal**: View messages from a DLQ topic

**Steps**:
1. Use CLI: `java -Dcli.enabled=true -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq show --topic test-service-orders.dlq --limit 5`
2. Or use API: `curl http://localhost:8080/api/topics/test-service-orders.dlq/messages?size=5`

**Expected**: Should see 5 messages with order data

### Scenario 3: Aggregations
**Goal**: Get statistics about DLQ messages

**Steps**:
1. Use CLI: `java -Dcli.enabled=true -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq aggregate`
2. Or use API: `curl -X POST http://localhost:8080/api/aggregations -H "Content-Type: application/json" -d '{}'`

**Expected**: Should see message counts, partition counts, and exception type statistics

### Scenario 4: Message Replay (Dry-Run)
**Goal**: Test message replay functionality

**Steps**:
1. Use CLI: `java -Dcli.enabled=true -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq replay --source test-service-orders.dlq --destination test-service-orders.retry --dry-run true`
2. Or use API: `curl -X POST http://localhost:8080/api/replay -H "Content-Type: application/json" -d '{"cluster":"local","sourceTopic":"test-service-orders.dlq","destinationTopic":"test-service-orders.retry","dryRun":true}'`

**Expected**: Should see list of messages that would be replayed (without actually replaying them)

### Scenario 5: Export to JSON
**Goal**: Export DLQ messages for offline analysis

**Steps**:
1. Use CLI: `java -Dcli.enabled=true -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq export --topic test-service-orders.dlq --file /tmp/orders.json`
2. Or use API: `curl -X POST http://localhost:8080/api/export/json -H "Content-Type: application/json" -d '{"topics":["test-service-orders.dlq"]}' -o /tmp/orders.json`
3. View exported file: `cat /tmp/orders.json | jq .`

**Expected**: Should see JSON file with all messages from the topic

### Scenario 6: Filtering by Partition
**Goal**: Get messages from specific partition

**Steps**:
1. Use API: `curl "http://localhost:8080/api/topics/test-service-orders.dlq/messages?partition=0&size=3"`

**Expected**: Should see only messages from partition 0

### Scenario 7: Filtering by Offset Range
**Goal**: Get messages within offset range

**Steps**:
1. Use API: `curl "http://localhost:8080/api/topics/test-service-orders.dlq/messages?offsetFrom=0&offsetTo=2"`

**Expected**: Should see messages with offsets 0, 1, and 2

### Scenario 8: Monitoring with Prometheus
**Goal**: View application metrics

**Steps**:
1. View metrics: `curl http://localhost:8080/actuator/prometheus`
2. Filter DLQ metrics: `curl -s http://localhost:8080/actuator/prometheus | grep dlq_`

**Expected**: Should see metrics like `dlq_messages_read_total`, `dlq_replay_attempts_total`, etc.

## Integration with CI/CD

The demo script can be used in CI/CD pipelines for integration testing:

```yaml
# Example GitHub Actions workflow
- name: Run Integration Tests
  run: |
    ./demo.sh
    # Add assertions here
    ./demo.sh stop
```

## Performance Notes

- The demo creates small datasets (15 messages total)
- For performance testing, increase message counts in `create_test_data()`
- Consider using separate test topics for load testing
- Monitor memory usage with large message volumes

## Security Notes

- Demo script runs with default security settings
- For production use, configure authentication and authorization
- Secure Kafka cluster access
- Use HTTPS for API endpoints
- Protect sensitive configuration (webhook URLs, etc.)

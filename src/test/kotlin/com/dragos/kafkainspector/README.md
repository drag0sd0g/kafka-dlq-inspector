# Integration Test Suite

This directory contains comprehensive integration tests for the Kafka DLQ Inspector application.

## Overview

The integration test suite validates the entire application stack with real Kafka infrastructure using Testcontainers. All tests run against actual Kafka brokers to ensure authentic end-to-end behavior.

## Test Structure

### 1. EndToEndIntegrationTest (6 tests)
Complete workflow validation covering the entire DLQ inspection lifecycle:
- **Topic Discovery**: Validates automatic discovery of DLQ topics matching configured patterns
- **Message Reading & Filtering**: Tests message retrieval with various filters (topic, partition, payload regex, time-based, header-based)
- **Aggregations**: Validates aggregation by topic, partition, and exception type
- **Export Functionality**: Tests JSON and CSV export with real message data
- **Replay Functionality**: Validates both dry-run and actual message replay to destination topics
- **Exception Metadata Handling**: Tests proper extraction and handling of DLQ exception information

### 2. RestApiIntegrationTest (10 tests)
REST API endpoint validation with MockMvc and real Kafka:
- `GET /api/topics` - Topic discovery endpoint
- `GET /api/topics/{topic}/messages` - Paginated message retrieval with filtering
- `GET /api/topics/{topic}/messages/{partition}/{offset}` - Specific message retrieval
- `POST /api/export/json` - JSON export endpoint
- `POST /api/export/csv` - CSV export endpoint  
- `POST /api/replay` - Message replay with dry-run support
- `POST /api/aggregations` - Aggregation statistics endpoint
- Concurrent request handling validation

### 3. CliIntegrationTest (9 tests)
CLI command functionality validation:
- Service availability verification
- Topic discovery via CLI services
- Message listing and search
- Export operations (JSON/CSV)
- Message replay operations
- Advanced filtering (partition, payload regex)
- Large volume handling (100+ messages)
- Exception metadata handling

### 4. AlertingServiceIntegrationTest (8 tests)
Alerting and monitoring scenarios:
- Threshold condition evaluation
- High error rate detection per topic
- Exception pattern tracking and aggregation
- Message lag and delay monitoring
- Time-window based alerting
- Metrics aggregation for monitoring dashboards
- Burst detection scenarios
- Rate calculation and anomaly detection

## Test Infrastructure

### Testcontainers
All integration tests use [Testcontainers](https://www.testcontainers.org/) to spin up real Kafka brokers:
- **Image**: `confluentinc/cp-kafka:7.5.1`
- **Isolation**: Each test class gets its own Kafka container
- **Cleanup**: Automatic container cleanup after tests complete

### Spring Boot Test Configuration
Tests use `@SpringBootTest` with dynamic property configuration:
```kotlin
@DynamicPropertySource
fun kafkaProps(registry: DynamicPropertyRegistry) {
    registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
    registry.add("spring.kafka.consumer.group-id") { "test-group" }
    registry.add("kafka.dlq.topic-pattern") { ".*\\.dlq" }
}
```

## Running the Tests

### Prerequisites
- Docker must be running (required for Testcontainers)
- JDK 21
- Gradle 8.x

### Run All Tests
```bash
./gradlew test
```

### Run Only Integration Tests
```bash
./gradlew test --tests "*IntegrationTest"
```

### Run Specific Test Class
```bash
./gradlew test --tests "EndToEndIntegrationTest"
./gradlew test --tests "RestApiIntegrationTest"
./gradlew test --tests "CliIntegrationTest"
./gradlew test --tests "AlertingServiceIntegrationTest"
```

### Run with Debug Logging
```bash
./gradlew test --info
```

## Test Data Patterns

### DLQ Message Headers
Tests produce messages with standard DLQ headers:
- `__TypeId__` - Original message type
- `__ExceptionClass__` - Exception class name
- `__ExceptionMessage__` - Exception message
- `__OriginalTopic__` - Source topic before DLQ
- `__StackTrace__` - Full exception stack trace (optional)

### Test Topics
Tests create topics following naming conventions:
- Pattern: `{domain}.dlq` (e.g., `orders.dlq`, `payments.dlq`)
- Partitions: 1-3 depending on test scenario
- Replication: 1 (single broker in test environment)

## CI/CD Integration

Tests are designed to run in GitHub Actions CI:
```yaml
- name: Run Integration Tests
  run: ./gradlew test --no-daemon
  
- name: Archive Test Results
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: test-results
    path: build/test-results/
```

## Test Coverage

The integration test suite covers:
- ✅ Topic discovery and pattern matching
- ✅ Message reading and consumption
- ✅ Search and filtering (8+ filter types)
- ✅ Aggregations (by topic, partition, exception, time)
- ✅ Export functionality (JSON/CSV)
- ✅ Replay operations (dry-run and live)
- ✅ REST API endpoints (all 7 endpoints)
- ✅ CLI operations (all major commands)
- ✅ Alerting and monitoring scenarios
- ✅ Exception metadata handling
- ✅ Concurrent request handling
- ✅ Large volume processing (100+ messages)

## Troubleshooting

### Testcontainers Issues
If tests fail with container errors:
1. Verify Docker is running: `docker ps`
2. Check Docker socket permissions
3. For Colima users: Ensure `DOCKER_HOST` is correctly set
4. Try cleaning containers: `docker container prune -f`

### Port Conflicts
If Kafka broker fails to start:
1. Check for processes using Kafka ports (9092, 9093)
2. Kill conflicting processes or let Testcontainers use random ports

### Memory Issues
For large test suites:
```bash
./gradlew test -Xmx2g -XX:MaxMetaspaceSize=512m
```

## Contributing

When adding new integration tests:
1. Follow existing test patterns and naming conventions
2. Use Testcontainers for real Kafka infrastructure
3. Clean up test data (automatic with @Testcontainers)
4. Add descriptive test names explaining what is validated
5. Document any special test scenarios in this README

## Performance Notes

- Integration tests are slower than unit tests (Kafka startup overhead)
- Average test class execution: 30-60 seconds
- Full suite execution: 2-5 minutes
- Parallelization: Tests run sequentially by default (container isolation)

## Further Reading

- [Testcontainers Kafka Module](https://www.testcontainers.org/modules/kafka/)
- [Spring Boot Testing](https://docs.spring.io/spring-boot/docs/current/reference/html/features.html#features.testing)
- [JUnit 5 User Guide](https://junit.org/junit5/docs/current/user-guide/)

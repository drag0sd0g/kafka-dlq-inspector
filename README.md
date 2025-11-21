# Kafka DLQ Inspector

A comprehensive Kotlin-based Spring Boot application for discovering, inspecting, aggregating, exporting, replaying, and monitoring Kafka dead letter queue (DLQ) topics.

## Overview

Kafka DLQ Inspector provides a robust solution for managing dead letter queues in Apache Kafka environments. The application exposes REST API endpoints, CLI commands, and Prometheus metrics to help teams monitor and recover from message processing failures.

## Features

- **Topic Discovery**: Automatically discover DLQ topics matching configurable regex patterns
- **Message Inspection**: Stream and decode DLQ messages with support for JSON and Apache Avro formats
- **Advanced Filtering**: Filter messages by partition, offset range, timestamp, headers, and exception types
- **Pagination**: Retrieve messages in configurable page sizes for efficient browsing
- **Aggregations**: Compute statistics by topic, partition, exception type, and time windows
- **Message Replay**: Replay messages to original or alternate topics with dry-run mode and rate limiting
- **Export**: Export messages to JSON or CSV formats for offline analysis
- **Alerting**: Configurable notifications via Slack and generic webhooks when thresholds are exceeded
- **Metrics**: Comprehensive Prometheus metrics and Spring Boot Actuator endpoints
- **CLI**: Optional command-line interface for scripting and automation
- **OpenAPI**: Interactive API documentation at /swagger-ui.html

## Requirements

- Java 21 or later
- Apache Kafka 2.8+ (tested with 3.x)
- Docker (for running Kafka locally and integration tests)
- Gradle 8+ (included via wrapper)

## Running Locally

### Prerequisites

Ensure Docker is installed and running on your system

### Build the Application

Build the fat JAR using Gradle:

```bash
./gradlew clean build shadowJar
```

The artifact will be created at `build/libs/kafka-dlq-inspector-0.1.0-all.jar`

### Start Supporting Services

Start Kafka, Zookeeper, and Schema Registry using Docker Compose:

```bash
docker compose -f docker/docker-compose.yml up -d zookeeper kafka schema-registry
```

This will start:
- Kafka broker on port 9092
- Zookeeper on port 2181
- Schema Registry on port 8081

### Run the Application

Start the application:

```bash
java -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar
```

The API will be available at `http://localhost:8080`

### Verify Installation

Check the health endpoint:

```bash
curl http://localhost:8080/actuator/health
```

Access the OpenAPI documentation:

```bash
open http://localhost:8080/swagger-ui.html
```

## Configuration

The application is configured via `src/main/resources/application.yml`. Key configuration properties:

### Kafka Connection

```yaml
spring:
  kafka:
    bootstrap-servers:
      - localhost:9092
    properties:
      schema.registry.url: http://localhost:8081
```

### DLQ Discovery

```yaml
kafka:
  dlq:
    topic-pattern: ".*\\.dlq"  # Regex pattern to match DLQ topics
    poll-size: 500              # Maximum records per poll
    poll-timeout-ms: 1000       # Consumer poll timeout
    max-records: 1000           # Maximum records to read per operation
```

### API and Search Limits

```yaml
kafka:
  export:
    max-records: 1000           # Maximum records per export operation
  search:
    default-limit: 200          # Default search result limit
  aggregation:
    limit-per-topic: 500        # Maximum messages per topic for aggregation
  replay:
    max-messages: 1000          # Maximum messages per replay operation
  alerting:
    threshold: 1000             # Default alert threshold

api:
  pagination:
    default-page-size: 50       # Default page size for paginated endpoints

cli:
  default-limit: 500            # Default limit for CLI commands
```

### Error Handling

```yaml
kafka:
  kafka-config:
    error-handler-backoff-ms: 5000  # Backoff interval for error handler
```

### Notifications

```yaml
notifications:
  slack:
    webhook-url: ""             # Slack webhook URL for alerts
  webhook:
    url: ""                     # Generic webhook URL for alerts
```

## API Endpoints

The OpenAPI spec is located at:
```
src/main/resources/openapi.yaml
```

### Topics

- `GET /api/topics` - List all discovered DLQ topics

### Messages

- `GET /api/topics/{topic}/messages` - List messages from a topic with pagination
  - Query parameters: `partition`, `offsetFrom`, `offsetTo`, `page`, `size`
- `GET /api/topics/{topic}/messages/{partition}/{offset}` - Get a specific message

### Aggregations

- `POST /api/aggregations` - Compute aggregations with filters

### Replay

- `POST /api/replay` - Replay messages to a destination topic
  - Request body: `ReplayRequest` with source topic, destination topic, filters, dry-run flag, and rate limit

### Export

- `POST /api/export/json` - Export messages to JSON format
- `POST /api/export/csv` - Export messages to CSV format

### Actuator

- `GET /actuator/health` - Application health status
- `GET /actuator/info` - Application information
- `GET /actuator/prometheus` - Prometheus metrics

### OpenAPI Documentation

- `GET /swagger-ui.html` - Interactive API documentation
- `GET /v3/api-docs` - OpenAPI specification (JSON)

## CLI Usage

Enable the CLI by setting the `cli.enabled` property:

```bash
java -Dcli.enabled=true -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq [command]
```

Available commands:

- `list-topics` - List all DLQ topics
- `show --topic <topic> --limit <n>` - Show messages from a topic
- `aggregate` - Show aggregated statistics
- `replay --source <topic> --destination <topic> --dry-run <true|false>` - Replay messages
- `export --topic <topic> --file <path>` - Export messages to JSON

## Testing

### Unit and Integration Tests

Run all tests (requires Docker):

```bash
./gradlew test
```

The integration tests use Testcontainers to spin up Kafka instances automatically.

### Code Coverage

Generate code coverage reports:

```bash
./gradlew jacocoTestReport
```

Reports are available at `build/reports/jacoco/test/html/index.html`

### Code Style

Check code style compliance:

```bash
./gradlew ktlintCheck
```

Automatically format code:

```bash
./gradlew ktlintFormat
```

## Monitoring

### Prometheus Metrics

The application exposes custom metrics:

- `dlq.messages.read` - Counter of messages read from DLQ topics
- `dlq.replay.attempts` - Counter of replay attempts
- `dlq.alerts.triggered` - Counter of alerts triggered

Configure Prometheus to scrape the metrics endpoint:

```yaml
scrape_configs:
  - job_name: 'kafka-dlq-inspector'
    static_configs:
    - targets: ['localhost:8080']
    metrics_path: '/actuator/prometheus'
```
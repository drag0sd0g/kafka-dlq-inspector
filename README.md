# Kafka DLQ Inspector

Kafka DLQ Inspector is a Kotlin + Spring Boot tool for discovering, inspecting, aggregating, exporting, replaying, and alerting on Kafka dead letter queue topics. It exposes REST endpoints, CLI commands, and metrics for Prometheus.

## Features
- Discover DLQ topics via regex configuration
- Stream and decode DLQ messages (JSON/Avro) with filtering and pagination
- Aggregations by topic, partition, exception type, and time windows
- Replay messages to original or alternate topics with dry-run and rate limiting
- Export messages to JSON or CSV
- Notifications via Slack and webhooks when thresholds are crossed
- Micrometer/Prometheus metrics and Spring Boot Actuator endpoints
- Optional CLI powered by Picocli (`cli.enabled=true`)

## Running locally
1. Start supporting services:
   ```bash
   docker compose -f docker/docker-compose.yml up -d
   ```
2. Build and run the application:
   ```bash
   ./gradlew shadowJar
   java -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar
   ```
3. Access the API at `http://localhost:8080`.

## REST API
- `GET /api/topics`
- `GET /api/topics/{topic}/messages`
- `GET /api/topics/{topic}/messages/{partition}/{offset}`
- `POST /api/replay`
- `POST /api/export/json`
- `POST /api/export/csv`
- `POST /api/aggregations`

## CLI Usage
Enable the CLI runner and execute commands:
```bash
java -Dcli.enabled=true -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq list-topics
```

## Testing
Run unit and integration tests (Kafka Testcontainers):
```bash
./gradlew test
```

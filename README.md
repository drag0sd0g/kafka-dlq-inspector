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

Ensure Docker is installed and running on your system:

#### macOS
```bash
# Install Docker Desktop from https://www.docker.com/products/docker-desktop
# Or via Homebrew
brew install --cask docker
```

#### Linux
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io docker-compose

# Fedora/RHEL/CentOS
sudo dnf install docker docker-compose
```

#### Windows
```bash
# Install Docker Desktop from https://www.docker.com/products/docker-desktop
# Requires WSL 2 backend
```

### Start Supporting Services

Start Kafka, Zookeeper, and Schema Registry using Docker Compose:

```bash
docker compose -f docker/docker-compose.yml up -d
```

This will start:
- Kafka broker on port 9092
- Zookeeper on port 2181
- Schema Registry on port 8081

### Build the Application

Build the fat JAR using Gradle:

```bash
./gradlew shadowJar
```

The artifact will be created at `build/libs/kafka-dlq-inspector-0.1.0-all.jar`

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

## Building for Production

### Create Production JAR

```bash
./gradlew clean shadowJar
```

### Docker Image

Create a Dockerfile in the project root:

```dockerfile
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY build/libs/kafka-dlq-inspector-0.1.0-all.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Build and run:

```bash
./gradlew shadowJar
docker build -t kafka-dlq-inspector:0.1.0 .
docker run -p 8080:8080 \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  kafka-dlq-inspector:0.1.0
```

## Deployment

### Environment Variables

Override configuration using environment variables:

```bash
export SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka-broker-1:9092,kafka-broker-2:9092
export SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_URL=http://schema-registry:8081
export KAFKA_DLQ_TOPIC_PATTERN=.*\\.dlq
export NOTIFICATIONS_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Kubernetes

Example deployment configuration:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-dlq-inspector
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-dlq-inspector
  template:
    metadata:
      labels:
        app: kafka-dlq-inspector
    spec:
      containers:
      - name: app
        image: kafka-dlq-inspector:0.1.0
        ports:
        - containerPort: 8080
        env:
        - name: SPRING_KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-service:9092"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
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

## Troubleshooting

### Docker Connection Issues

If you encounter Testcontainers failures:

1. Ensure Docker is running: `docker ps`
2. Unset any lingering environment variables: `unset DOCKER_HOST`
3. Check for Colima or other Docker alternatives in your shell configuration
4. Restart your IDE/terminal to refresh the environment

### Kafka Connection Timeout

If the application cannot connect to Kafka:

1. Verify Kafka is running: `docker ps | grep kafka`
2. Check the bootstrap servers configuration
3. Ensure network connectivity between the application and Kafka brokers
4. Review logs for connection errors: `docker logs <kafka-container-id>`

### Schema Registry Errors

If Avro deserialization fails:

1. Verify Schema Registry is running and accessible
2. Check the schema registry URL configuration
3. Ensure schemas are registered for your topics
4. Review compatibility settings in Schema Registry

## OpenAPI Specification and Stub Generation

This project uses an external OpenAPI specification file (`src/main/resources/openapi.yaml`) to define the REST API. The specification is used for:

1. **API Documentation**: Served via Swagger UI at `/swagger-ui.html`
2. **API Contract**: Defines the interface contracts for all REST endpoints
3. **Code Generation**: Generates Kotlin Spring server stubs automatically during build

### OpenAPI Specification File

The OpenAPI spec is located at:
```
src/main/resources/openapi.yaml
```

This YAML file contains:
- Complete API endpoint definitions
- Request/response schemas
- Data models
- API documentation and descriptions

### Stub Generation

The project uses the [OpenAPI Generator](https://openapi-generator.tech/) Gradle plugin to automatically generate server stubs during the build process.

#### Generated Code Location

Generated interfaces and models are placed in:
```
build/generated/openapi/src/main/kotlin/
```

The generated code includes:
- API interfaces in `com.dragos.kafkainspector.api.generated` package
- Data models in `com.dragos.kafkainspector.model.generated` package

#### Build Configuration

The OpenAPI Generator is configured in `build.gradle.kts`:

```kotlin
openApiGenerate {
    generatorName.set("kotlin-spring")
    inputSpec.set("$rootDir/src/main/resources/openapi.yaml")
    outputDir.set("${layout.buildDirectory.get()}/generated/openapi")
    apiPackage.set("com.dragos.kafkainspector.api.generated")
    modelPackage.set("com.dragos.kafkainspector.model.generated")
    configOptions.set(
        mapOf(
            "interfaceOnly" to "true",
            "skipDefaultInterface" to "true",
            "useTags" to "true",
            "useSpringBoot3" to "true",
            "serializationLibrary" to "jackson",
        ),
    )
}
```

#### Build Process

The stub generation is automatically triggered during the build:

```bash
./gradlew clean build
```

The `openApiGenerate` task runs before `compileKotlin`, ensuring stubs are available for compilation.

#### Generated Code Exclusions

Generated code is:
- Excluded from ktlint checks
- Not committed to version control (located in `build/` directory)
- Regenerated on each clean build

### Modifying the API

To modify the API:

1. **Edit the OpenAPI spec**: Update `src/main/resources/openapi.yaml`
2. **Rebuild the project**: Run `./gradlew clean build`
3. **Verify**: Generated stubs will be updated automatically

### Viewing the API Documentation

Access the interactive API documentation:

1. **Start the application**: `./gradlew bootRun` or `java -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar`
2. **Open Swagger UI**: Navigate to `http://localhost:8080/swagger-ui.html`
3. **View OpenAPI JSON**: Access `http://localhost:8080/v3/api-docs`

### Benefits of External OpenAPI Spec

1. **Single Source of Truth**: API contract is defined in one place
2. **Contract-First Development**: Design the API before implementing
3. **Automatic Documentation**: API docs are always up-to-date
4. **Code Generation**: Reduces boilerplate and ensures consistency
5. **Client Generation**: Can generate clients in multiple languages
6. **Version Control**: Easy to track API changes over time

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes and add tests
4. Ensure all tests pass: `./gradlew test`
5. Check code style: `./gradlew ktlintCheck`
6. Commit your changes: `git commit -am 'Add new feature'`
7. Push to the branch: `git push origin feature/my-feature`
8. Create a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

For issues, questions, or contributions, please use the GitHub issue tracker.

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

## Development Environment Setup

### Docker Configuration
This project uses Testcontainers for integration tests, which requires Docker to be running.

**Important for developers migrating from Colima or other Docker alternatives:**

If you've recently switched from Colima to Docker Desktop (or vice versa), you may encounter Testcontainers failures with errors like:
```
Caused by: com.github.dockerjava.api.exception.NotFoundException: Status 404: {"message":"No such container: ..."}
```

To resolve this:
1. **Unset any lingering `DOCKER_HOST` environment variable:**
   ```bash
   unset DOCKER_HOST
   ```

2. **Check your shell configuration files** (`~/.bashrc`, `~/.zshrc`, etc.) and remove any Colima-specific Docker configuration:
   ```bash
   # Remove lines like these:
   # export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
   ```

3. **Verify Docker is accessible:**
   ```bash
   docker ps  # Should connect to Docker Desktop
   ```

4. **Restart your terminal/IDE** to ensure the environment is clean.

Testcontainers should automatically detect the Docker socket at the standard location (`/var/run/docker.sock` on Unix or Docker Desktop's named pipe on Windows).

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

**Note:** Integration tests require Docker. See "Docker Configuration" section above if you encounter Testcontainers issues.

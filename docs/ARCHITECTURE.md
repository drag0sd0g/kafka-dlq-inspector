# Architecture Overview

Kafka DLQ Inspector follows a layered architecture:

- **Model**: Canonical data classes (`DlqMessage`, `DlqTopicInfo`, `SearchFilters`, `ReplayRequest`, aggregations).
- **Kafka Infrastructure**: Admin-based topic discovery, consumer configuration for safe seeking, schema decoding (JSON/Avro), producer for replay.
- **Services**: Reader/search, aggregation, replay, export, alerting.
- **API**: REST controllers exposing topic discovery, message browsing, replay, export, and aggregations.
- **CLI**: Picocli commands mirroring the API for automation.
- **Observability**: Micrometer + Prometheus via Actuator, counters for reads/replays/alerts.

The project uses Kotlin with Spring Boot 3, Kafka clients, and Micrometer. Docker Compose provisions Kafka, Schema Registry, and the application for local development.

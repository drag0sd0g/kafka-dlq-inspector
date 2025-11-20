package com.dragos.kafkainspector.config

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.info.License
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenApiConfig {
    @Bean
    fun customOpenAPI(): OpenAPI =
        OpenAPI().info(
            Info()
                .title("Kafka DLQ Inspector API")
                .version("0.1.0")
                .description(
                    "REST API for discovering, inspecting, aggregating, exporting, and replaying Kafka dead letter queue topics. " +
                        "Features: Discover DLQ topics via regex configuration, stream and decode DLQ messages (JSON/Avro) " +
                        "with filtering and pagination, aggregations by topic, partition, exception type, and time windows, " +
                        "replay messages to original or alternate topics with dry-run and rate limiting, export messages to JSON or CSV.",
                ).license(License().name("Apache 2.0").url("https://www.apache.org/licenses/LICENSE-2.0")),
        )
}

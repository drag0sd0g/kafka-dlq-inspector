package com.dragos.kafkainspector.config

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.parser.OpenAPIV3Parser
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource

@Configuration
class OpenApiConfig {
    @Bean
    fun customOpenAPI(): OpenAPI {
        // Load OpenAPI spec from external YAML file
        val resource = ClassPathResource("openapi.yaml")
        val parser = OpenAPIV3Parser()
        return parser.read(resource.file.absolutePath) ?: throw IllegalStateException("Failed to load OpenAPI spec from openapi.yaml")
    }
}

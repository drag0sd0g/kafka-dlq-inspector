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
        val yamlContent = resource.inputStream.bufferedReader().use { it.readText() }
        val parser = OpenAPIV3Parser()
        val parseResult = parser.readContents(yamlContent)
        return parseResult.openAPI ?: throw IllegalStateException("Failed to load OpenAPI spec from openapi.yaml")
    }
}

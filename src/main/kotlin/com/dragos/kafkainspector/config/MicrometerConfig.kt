package com.dragos.kafkainspector.config

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration

@Configuration
class MicrometerConfig(
    private val registry: MeterRegistry,
    @Value("\${spring.application.name}") private val appName: String
) {
    init {
        registry.config().commonTags("application", appName)
    }
}

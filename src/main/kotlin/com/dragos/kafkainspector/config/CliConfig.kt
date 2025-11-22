package com.dragos.kafkainspector.config

import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import picocli.CommandLine

/**
 * Configuration for Picocli CLI integration with Spring
 */
@Configuration
class CliConfig {
    /**
     * Spring-aware factory for Picocli that uses Spring's ApplicationContext
     * to instantiate command classes, allowing dependency injection to work
     */
    @Bean
    fun picocliFactory(applicationContext: ApplicationContext): CommandLine.IFactory =
        object : CommandLine.IFactory {
            override fun <K : Any> create(clazz: Class<K>): K = applicationContext.getBean(clazz)
        }
}

package com.dragos.kafkainspector.config

import com.dragos.kafkainspector.cli.DlqCommand
import com.dragos.kafkainspector.cli.ListTopicsCommand
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import picocli.CommandLine

/**
 * Test for CLI configuration to verify Spring-aware Picocli factory integration
 */
@SpringBootTest
@Testcontainers
class CliConfigTest {
    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))

        @JvmStatic
        @DynamicPropertySource
        fun kafkaProps(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            registry.add("cli.enabled") { "false" } // Don't auto-run CLI in tests
        }
    }

    @Autowired
    lateinit var applicationContext: ApplicationContext

    @Autowired
    lateinit var cliConfig: CliConfig

    @Test
    fun `should create Spring-aware Picocli factory`() {
        // Given
        val factory = cliConfig.picocliFactory(applicationContext)

        // Then
        assertNotNull(factory, "Factory should not be null")
    }

    @Test
    fun `factory should create command beans using Spring context`() {
        // Given
        val factory = cliConfig.picocliFactory(applicationContext)

        // When - Create a command class using the factory
        val command = factory.create(ListTopicsCommand::class.java)

        // Then - Should be properly instantiated with Spring dependencies
        assertNotNull(command, "Command should be created")
        assertInstanceOf(ListTopicsCommand::class.java, command, "Should be correct type")
    }

    @Test
    fun `CommandLine should work with Spring factory for subcommands`() {
        // Given
        val dlqCommand = applicationContext.getBean(DlqCommand::class.java)
        val factory = cliConfig.picocliFactory(applicationContext)

        // When - Create CommandLine with Spring factory
        val commandLine = CommandLine(dlqCommand, factory)

        // Then - Should parse command structure correctly
        assertNotNull(commandLine)
        val subcommands = commandLine.subcommands
        assert(subcommands.containsKey("list-topics")) { "Should have list-topics subcommand" }
        assert(subcommands.containsKey("show")) { "Should have show subcommand" }
        assert(subcommands.containsKey("aggregate")) { "Should have aggregate subcommand" }
        assert(subcommands.containsKey("replay")) { "Should have replay subcommand" }
        assert(subcommands.containsKey("export")) { "Should have export subcommand" }
    }
}

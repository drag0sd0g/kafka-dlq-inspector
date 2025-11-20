package com.dragos.kafkainspector.cli

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.service.ExportService
import com.dragos.kafkainspector.service.ReplayService
import com.dragos.kafkainspector.service.SearchService
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties

/**
 * Integration test for CLI commands functionality
 */
@SpringBootTest
@Testcontainers
class CliIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))

        @JvmStatic
        @DynamicPropertySource
        fun kafkaProps(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            registry.add("spring.kafka.consumer.group-id") { "cli-test-group" }
            registry.add("kafka.dlq.topic-pattern") { ".*\\.dlq" }
            registry.add("cli.enabled") { "true" }
        }
    }

    @Autowired
    lateinit var adminClient: AdminClient

    @Autowired
    lateinit var dlqTopicDiscovery: DlqTopicDiscovery

    @Autowired
    lateinit var searchService: SearchService

    @Autowired
    lateinit var exportService: ExportService

    @Autowired
    lateinit var replayService: ReplayService

    private lateinit var producer: KafkaProducer<ByteArray, ByteArray>

    @BeforeEach
    fun setup() {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        producer = KafkaProducer(producerProps)
    }

    @Test
    fun `CLI services should be available when CLI is enabled`() {
        // The services required for CLI operations should be available
        assertNotNull(dlqTopicDiscovery)
        assertNotNull(searchService)
        assertNotNull(exportService)
        assertNotNull(replayService)
    }

    @Test
    fun `should discover DLQ topics via CLI service`() {
        // Setup
        val topics = listOf("cli-test-1.dlq", "cli-test-2.dlq")
        topics.forEach { topic ->
            adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
        }

        await().atMost(Duration.ofSeconds(10)).until {
            adminClient
                .listTopics()
                .names()
                .get()
                .containsAll(topics)
        }

        // Test - Discovery service works
        val discovered = dlqTopicDiscovery.discover()
        assertTrue(discovered.any { it.name == "cli-test-1.dlq" })
        assertTrue(discovered.any { it.name == "cli-test-2.dlq" })
    }

    @Test
    fun `should list messages via CLI service`() {
        // Setup
        val topic = "cli-messages.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        repeat(5) { i ->
            val payload = """{"cli-message": $i}""".toByteArray()
            producer.send(ProducerRecord(topic, 0, null, payload)).get()
        }
        producer.flush()

        // Test - Search service works
        val filters =
            com.dragos.kafkainspector.model
                .SearchFilters(topics = listOf(topic))
        val messages = searchService.search(filters, 100)

        assertTrue(messages.isNotEmpty())
        assertTrue(messages.all { it.topic == topic })
    }

    @Test
    fun `should export messages via CLI service`() {
        // Setup
        val topic = "cli-export.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        repeat(3) { i ->
            val payload = """{"cli-export": $i}""".toByteArray()
            producer.send(ProducerRecord(topic, 0, null, payload)).get()
        }
        producer.flush()

        // Test - Export service works
        val filters =
            com.dragos.kafkainspector.model
                .SearchFilters(topics = listOf(topic))
        val messages = searchService.search(filters, 100)

        val jsonFile = java.io.File.createTempFile("cli-export", ".json")
        val csvFile = java.io.File.createTempFile("cli-export", ".csv")

        try {
            assertDoesNotThrow {
                exportService.exportJson(messages, jsonFile)
                exportService.exportCsv(messages, csvFile)
            }

            assertTrue(jsonFile.exists() && jsonFile.length() > 0)
            assertTrue(csvFile.exists() && csvFile.length() > 0)
        } finally {
            jsonFile.delete()
            csvFile.delete()
        }
    }

    @Test
    fun `should replay messages via CLI service`() {
        // Setup
        val sourceTopic = "cli-replay-source.dlq"
        val destTopic = "cli-replay-dest"
        adminClient
            .createTopics(
                listOf(
                    NewTopic(sourceTopic, 1, 1.toShort()),
                    NewTopic(destTopic, 1, 1.toShort()),
                ),
            ).all()
            .get()

        repeat(3) { i ->
            val payload = """{"cli-replay": $i}""".toByteArray()
            producer.send(ProducerRecord(sourceTopic, 0, null, payload)).get()
        }
        producer.flush()

        // Test - Replay service works (dry run)
        val replayRequest =
            com.dragos.kafkainspector.model.ReplayRequest(
                cluster = "test",
                sourceTopic = sourceTopic,
                destinationTopic = destTopic,
                filters =
                    com.dragos.kafkainspector.model
                        .SearchFilters(topics = listOf(sourceTopic)),
                dryRun = true,
            )

        val result =
            assertDoesNotThrow {
                replayService.replay(replayRequest)
            }

        assertNotNull(result)
        assertTrue(result.isNotEmpty())
    }

    @Test
    fun `should handle CLI operations with filters`() {
        // Setup
        val topic = "cli-filtered.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 2, 1.toShort()))).all().get()

        // Produce messages with different patterns
        for (partition in 0..1) {
            repeat(3) { i ->
                val payload = """{"partition": $partition, "index": $i}""".toByteArray()
                val record = ProducerRecord<ByteArray, ByteArray>(topic, partition, null, payload)
                record.headers().add("userId", "user-$partition".toByteArray())
                producer.send(record).get()
            }
        }
        producer.flush()

        // Test - Filter by partition
        val partitionFilter =
            com.dragos.kafkainspector.model.SearchFilters(
                topics = listOf(topic),
                partitions = listOf(0),
            )
        val partition0Messages = searchService.search(partitionFilter, 100)
        assertTrue(partition0Messages.all { it.partition == 0 })

        // Test - Filter by payload regex
        val regexFilter =
            com.dragos.kafkainspector.model.SearchFilters(
                topics = listOf(topic),
                payloadRegex = ".*partition.*0.*",
            )
        val regexMessages = searchService.search(regexFilter, 100)
        assertTrue(regexMessages.isNotEmpty())
    }

    @Test
    fun `should handle CLI operations with large message volumes`() {
        // Setup
        val topic = "cli-large-volume.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 3, 1.toShort()))).all().get()

        // Produce 100 messages
        repeat(100) { i ->
            val partition = i % 3
            val payload = """{"large-volume": $i}""".toByteArray()
            producer.send(ProducerRecord(topic, partition, null, payload)).get()
        }
        producer.flush()

        // Test - Search should handle pagination
        val allMessages =
            searchService.search(
                com.dragos.kafkainspector.model
                    .SearchFilters(topics = listOf(topic)),
                150,
            )
        assertTrue(allMessages.size >= 100)

        // Test - Export should handle large volumes
        val exportFile = java.io.File.createTempFile("cli-large", ".json")
        try {
            assertDoesNotThrow {
                exportService.exportJson(allMessages, exportFile)
            }
            assertTrue(exportFile.length() > 1000) // Should be reasonably large
        } finally {
            exportFile.delete()
        }
    }

    @Test
    fun `should handle CLI operations with exception metadata`() {
        // Setup
        val topic = "cli-exceptions.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        val exceptions =
            listOf(
                "java.lang.NullPointerException",
                "java.lang.IllegalArgumentException",
                "java.io.IOException",
            )

        exceptions.forEach { exceptionType ->
            val payload = """{"error": "test"}""".toByteArray()
            val record = ProducerRecord<ByteArray, ByteArray>(topic, 0, null, payload)
            record.headers().add("__ExceptionClass__", exceptionType.toByteArray())
            record.headers().add("__ExceptionMessage__", "Test error message".toByteArray())
            record.headers().add("__OriginalTopic__", "source-topic".toByteArray())
            producer.send(record).get()
        }
        producer.flush()

        // Test - Messages should include exception metadata
        val messages =
            searchService.search(
                com.dragos.kafkainspector.model
                    .SearchFilters(topics = listOf(topic)),
                100,
            )

        assertTrue(messages.all { it.exceptionClass != null })
        assertTrue(messages.all { it.exceptionMessage != null })
        assertTrue(messages.all { it.originalTopic == "source-topic" })
    }
}

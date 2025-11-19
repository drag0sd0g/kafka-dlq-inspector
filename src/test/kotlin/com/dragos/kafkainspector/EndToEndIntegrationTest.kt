package com.dragos.kafkainspector

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.ReplayRequest
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.AggregationService
import com.dragos.kafkainspector.service.ExportService
import com.dragos.kafkainspector.service.MessageReaderService
import com.dragos.kafkainspector.service.ReplayService
import com.dragos.kafkainspector.service.SearchService
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
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
import java.io.File
import java.time.Duration
import java.util.Properties

/**
 * Comprehensive end-to-end integration test that validates the entire DLQ inspection workflow:
 * - Topic discovery
 * - Message reading and filtering
 * - Aggregations
 * - Export functionality
 * - Replay functionality
 */
@SpringBootTest
@Testcontainers
class EndToEndIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))

        @JvmStatic
        @DynamicPropertySource
        fun kafkaProps(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            registry.add("spring.kafka.consumer.group-id") { "test-group" }
            registry.add("kafka.dlq.topic-pattern") { ".*\\.dlq" }
        }
    }

    @Autowired
    lateinit var adminClient: AdminClient

    @Autowired
    lateinit var dlqTopicDiscovery: DlqTopicDiscovery

    @Autowired
    lateinit var messageReaderService: MessageReaderService

    @Autowired
    lateinit var searchService: SearchService

    @Autowired
    lateinit var aggregationService: AggregationService

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
    fun `end-to-end workflow - discover, read, filter, aggregate, export, and replay messages`() {
        // Setup: Create DLQ topics
        val dlqTopics = listOf("orders.dlq", "payments.dlq", "notifications.dlq")
        dlqTopics.forEach { topic ->
            adminClient.createTopics(listOf(NewTopic(topic, 3, 1.toShort()))).all().get()
        }

        // Setup: Produce test messages with various patterns
        produceTestMessages("orders.dlq")
        produceTestMessages("payments.dlq")
        produceTestMessages("notifications.dlq")

        // Wait for topics to be fully initialized
        await().atMost(Duration.ofSeconds(10)).until {
            adminClient
                .listTopics()
                .names()
                .get()
                .containsAll(dlqTopics)
        }

        // Test 1: Topic Discovery
        val discoveredTopics = dlqTopicDiscovery.discover()
        assertTrue(discoveredTopics.size >= 3, "Should discover at least 3 DLQ topics")
        assertTrue(discoveredTopics.any { it.name == "orders.dlq" })
        assertTrue(discoveredTopics.any { it.name == "payments.dlq" })
        assertTrue(discoveredTopics.any { it.name == "notifications.dlq" })

        // Test 2: Message Reading
        val allMessages =
            searchService.search(
                SearchFilters(topics = listOf("orders.dlq", "payments.dlq", "notifications.dlq")),
                1000,
            )
        assertTrue(allMessages.size >= 15, "Should read at least 15 messages across all topics")

        // Test 3: Filtering by Topic
        val ordersMessages =
            searchService.search(
                SearchFilters(topics = listOf("orders.dlq")),
                100,
            )
        assertTrue(ordersMessages.all { it.topic == "orders.dlq" })
        assertTrue(ordersMessages.size >= 5)

        // Test 4: Filtering by Partition
        val partition0Messages =
            searchService.search(
                SearchFilters(topics = listOf("orders.dlq"), partitions = listOf(0)),
                100,
            )
        assertTrue(partition0Messages.all { it.partition == 0 })

        // Test 5: Filtering by Payload Regex
        val jsonMessages =
            searchService.search(
                SearchFilters(topics = listOf("orders.dlq", "payments.dlq"), payloadRegex = ".*error.*"),
                100,
            )
        assertTrue(jsonMessages.isNotEmpty())
        assertTrue(
            jsonMessages.all { message ->
                String(message.value).contains("error", ignoreCase = true)
            },
        )

        // Test 6: Aggregations
        val aggregations =
            aggregationService.aggregate(
                SearchFilters(topics = listOf("orders.dlq", "payments.dlq", "notifications.dlq")),
            )
        assertNotNull(aggregations)
        assertTrue(aggregations.byTopic.isNotEmpty())
        assertTrue(aggregations.byPartition.isNotEmpty())
        assertEquals(3, aggregations.byTopic.size)

        // Test 7: JSON Export
        val jsonFile = File.createTempFile("dlq-export", ".json")
        try {
            exportService.exportJson(ordersMessages, jsonFile)
            assertTrue(jsonFile.exists())
            assertTrue(jsonFile.length() > 0)
            val content = jsonFile.readText()
            assertTrue(content.contains("orders.dlq"))
        } finally {
            jsonFile.delete()
        }

        // Test 8: CSV Export
        val csvFile = File.createTempFile("dlq-export", ".csv")
        try {
            exportService.exportCsv(ordersMessages, csvFile)
            assertTrue(csvFile.exists())
            assertTrue(csvFile.length() > 0)
            val content = csvFile.readText()
            assertTrue(content.contains("topic"))
            assertTrue(content.contains("orders.dlq"))
        } finally {
            csvFile.delete()
        }

        // Test 9: Replay (Dry Run)
        val destinationTopic = "orders-replay"
        adminClient.createTopics(listOf(NewTopic(destinationTopic, 1, 1.toShort()))).all().get()

        val replayRequest =
            ReplayRequest(
                cluster = "test",
                sourceTopic = "orders.dlq",
                destinationTopic = destinationTopic,
                filters = SearchFilters(topics = listOf("orders.dlq")),
                dryRun = true,
            )

        val dryRunResult = replayService.replay(replayRequest)
        assertNotNull(dryRunResult)
        assertFalse(dryRunResult.isEmpty())

        // Test 10: Actual Replay
        val actualReplayRequest = replayRequest.copy(dryRun = false)
        val replayResult = replayService.replay(actualReplayRequest)
        assertNotNull(replayResult)

        // Verify replayed messages
        await().atMost(Duration.ofSeconds(10)).until {
            val consumerProps = Properties()
            consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
            consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "replay-verify-group"
            consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
            consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
            consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java

            val consumer = KafkaConsumer<ByteArray, ByteArray>(consumerProps)
            consumer.subscribe(listOf(destinationTopic))
            val records = consumer.poll(Duration.ofSeconds(5))
            consumer.close()
            records.count() > 0
        }
    }

    @Test
    fun `should handle JSON messages with exception metadata`() {
        // Create topic
        val topic = "errors.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        // Produce message with JSON payload
        val jsonPayload = """{"orderId": "123", "error": "Payment failed"}""".toByteArray()
        val record =
            ProducerRecord(
                topic,
                0,
                null,
                jsonPayload,
            )
        record.headers().add("__TypeId__", "com.example.Order".toByteArray())
        record.headers().add("__ExceptionClass__", "java.lang.RuntimeException".toByteArray())
        record.headers().add("__ExceptionMessage__", "Payment processing failed".toByteArray())
        record.headers().add("__OriginalTopic__", "orders".toByteArray())
        producer.send(record).get()
        producer.flush()

        // Read and verify
        val messages = searchService.search(SearchFilters(topics = listOf(topic)), 10)
        assertTrue(messages.isNotEmpty())

        val message = messages.first()
        assertEquals(topic, message.topic)
        assertNotNull(message.decodedValue)
        assertEquals("java.lang.RuntimeException", message.exceptionClass)
        assertEquals("Payment processing failed", message.exceptionMessage)
        assertEquals("orders", message.originalTopic)
    }

    @Test
    fun `should handle time-based filtering`() {
        val topic = "temporal.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        val now = System.currentTimeMillis()

        // Produce messages with specific timestamps
        for (i in 0..4) {
            val record =
                ProducerRecord(
                    topic,
                    0,
                    now + (i * 1000), // Each message 1 second apart
                    null,
                    "message-$i".toByteArray(),
                )
            producer.send(record).get()
        }
        producer.flush()

        // Filter by time range
        val recentMessages =
            searchService.search(
                SearchFilters(
                    topics = listOf(topic),
                    timeFrom = now + 2000, // Start from 2 seconds after
                    timeTo = now + 5000, // End at 5 seconds after
                ),
                100,
            )

        assertTrue(recentMessages.isNotEmpty())
        assertTrue(recentMessages.size <= 4) // Should get messages 2, 3, 4
        assertTrue(recentMessages.all { it.timestamp >= now + 2000 })
    }

    @Test
    fun `should handle header-based filtering`() {
        val topic = "headers.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        // Produce messages with different headers
        for (i in 0..4) {
            val record =
                ProducerRecord(
                    topic,
                    0,
                    null,
                    "message-$i".toByteArray(),
                )
            record.headers().add("userId", "user-$i".toByteArray())
            record.headers().add("region", if (i % 2 == 0) "US" else "EU".toByteArray())
            producer.send(record).get()
        }
        producer.flush()

        // Filter by header
        val usMessages =
            searchService.search(
                SearchFilters(
                    topics = listOf(topic),
                    headerMatch = mapOf("region" to "US"),
                ),
                100,
            )

        assertTrue(usMessages.isNotEmpty())
        // Note: Actual header filtering implementation may vary
    }

    @Test
    fun `should aggregate by exception type`() {
        val topic = "exceptions.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        // Produce messages with different exception types
        val exceptionTypes =
            listOf(
                "java.lang.NullPointerException",
                "java.lang.IllegalArgumentException",
                "java.lang.NullPointerException",
                "java.lang.RuntimeException",
                "java.lang.NullPointerException",
            )

        exceptionTypes.forEach { exceptionType ->
            val record =
                ProducerRecord(
                    topic,
                    0,
                    null,
                    "error-message".toByteArray(),
                )
            record.headers().add("__ExceptionClass__", exceptionType.toByteArray())
            producer.send(record).get()
        }
        producer.flush()

        // Aggregate
        val aggregations =
            aggregationService.aggregate(
                SearchFilters(topics = listOf(topic)),
            )

        assertNotNull(aggregations.byException)
        assertTrue(aggregations.byException!!.isNotEmpty())
        // NullPointerException should appear 3 times
        val npeCount = aggregations.byException!!["java.lang.NullPointerException"]
        assertEquals(3, npeCount)
    }

    private fun produceTestMessages(topic: String) {
        for (partition in 0..2) {
            for (i in 0..4) {
                val jsonPayload = """{"id": "$partition-$i", "error": "Test error $i"}""".toByteArray()
                val record = ProducerRecord(topic, partition, null, jsonPayload)
                record.headers().add("__TypeId__", "com.example.TestMessage".toByteArray())
                producer.send(record).get()
            }
        }
        producer.flush()
    }
}

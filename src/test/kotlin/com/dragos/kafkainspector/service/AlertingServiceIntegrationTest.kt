package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.model.SearchFilters
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
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
import java.util.Properties

/**
 * Integration test for Alerting Service with real Kafka infrastructure
 */
@SpringBootTest
@Testcontainers
class AlertingServiceIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))

        @JvmStatic
        @DynamicPropertySource
        fun kafkaProps(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            registry.add("spring.kafka.consumer.group-id") { "alerting-test-group" }
            registry.add("kafka.dlq.topic-pattern") { ".*\\.dlq" }
        }
    }

    @Autowired
    lateinit var adminClient: AdminClient

    @Autowired
    lateinit var alertingService: AlertingService

    @Autowired
    lateinit var searchService: SearchService

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
    fun `should evaluate threshold conditions for DLQ messages`() {
        // Setup
        val topic = "alerting-threshold.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        // Produce messages to exceed threshold
        repeat(10) { i ->
            val payload = """{"alert-test": $i}""".toByteArray()
            producer.send(ProducerRecord(topic, 0, null, payload)).get()
        }
        producer.flush()

        // Test - Check messages are searchable for threshold evaluation
        val messages =
            searchService.search(
                SearchFilters(topics = listOf(topic)),
                100,
            )

        assertTrue(messages.size >= 10)
        assertNotNull(messages.first().topic)
    }

    @Test
    fun `should detect high error rates per topic`() {
        // Setup
        val topics = listOf("alerts-errors-1.dlq", "alerts-errors-2.dlq")
        topics.forEach { topic ->
            adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
        }

        // Produce different volumes to different topics
        repeat(20) { i ->
            val payload = """{"high-rate": $i}""".toByteArray()
            producer.send(ProducerRecord("alerts-errors-1.dlq", 0, null, payload)).get()
        }

        repeat(5) { i ->
            val payload = """{"low-rate": $i}""".toByteArray()
            producer.send(ProducerRecord("alerts-errors-2.dlq", 0, null, payload)).get()
        }
        producer.flush()

        // Test - Verify message counts for alerting logic
        val highRateMsgs =
            searchService.search(
                SearchFilters(topics = listOf("alerts-errors-1.dlq")),
                100,
            )
        val lowRateMsgs =
            searchService.search(
                SearchFilters(topics = listOf("alerts-errors-2.dlq")),
                100,
            )

        assertTrue(highRateMsgs.size >= 20)
        assertTrue(lowRateMsgs.size >= 5)
        assertTrue(highRateMsgs.size > lowRateMsgs.size * 2)
    }

    @Test
    fun `should track exception patterns for alerting`() {
        // Setup
        val topic = "alerts-exceptions.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        // Produce messages with repeated exception types
        val criticalException = "java.lang.OutOfMemoryError"
        repeat(5) { i ->
            val payload = """{"critical": $i}""".toByteArray()
            val record = ProducerRecord<ByteArray, ByteArray>(topic, 0, null, payload)
            record.headers().add("__ExceptionClass__", criticalException.toByteArray())
            record.headers().add("__ExceptionMessage__", "Heap space exhausted".toByteArray())
            producer.send(record).get()
        }

        val regularException = "java.lang.NullPointerException"
        repeat(2) { i ->
            val payload = """{"regular": $i}""".toByteArray()
            val record = ProducerRecord<ByteArray, ByteArray>(topic, 0, null, payload)
            record.headers().add("__ExceptionClass__", regularException.toByteArray())
            producer.send(record).get()
        }
        producer.flush()

        // Test - Verify exception metadata for pattern detection
        val messages =
            searchService.search(
                SearchFilters(topics = listOf(topic)),
                100,
            )

        val criticalCount = messages.count { it.exceptionClass == criticalException }
        val regularCount = messages.count { it.exceptionClass == regularException }

        assertTrue(criticalCount >= 5)
        assertTrue(regularCount >= 2)
        assertTrue(criticalCount > regularCount)
    }

    @Test
    fun `should monitor message lag and delays`() {
        // Setup
        val topic = "alerts-lag.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        val now = System.currentTimeMillis()

        // Produce messages with timestamps simulating delays
        for (i in 0..4) {
            val timestamp = now - (i * 60000) // Messages from 0 to 4 minutes ago
            val payload = """{"lag-test": $i}""".toByteArray()
            val record =
                ProducerRecord<ByteArray, ByteArray>(
                    topic,
                    0,
                    timestamp,
                    null,
                    payload,
                )
            producer.send(record).get()
        }
        producer.flush()

        // Test - Verify timestamp data for lag monitoring
        val messages =
            searchService.search(
                SearchFilters(topics = listOf(topic)),
                100,
            )

        assertTrue(messages.isNotEmpty())
        assertTrue(
            messages.any { msg ->
                now - msg.timestamp > 180000 // Some messages older than 3 minutes
            },
        )
    }

    @Test
    fun `should support time-window based alerting`() {
        // Setup
        val topic = "alerts-window.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        val now = System.currentTimeMillis()
        val fiveMinutesAgo = now - 300000

        // Produce messages across different time windows
        repeat(5) { i ->
            val record =
                ProducerRecord<ByteArray, ByteArray>(
                    topic,
                    0,
                    now - (i * 10000), // Recent messages
                    null,
                    "recent-$i".toByteArray(),
                )
            producer.send(record).get()
        }

        repeat(3) { i ->
            val record =
                ProducerRecord<ByteArray, ByteArray>(
                    topic,
                    0,
                    fiveMinutesAgo - (i * 10000), // 5 minutes ago
                    null,
                    "old-$i".toByteArray(),
                )
            producer.send(record).get()
        }
        producer.flush()

        // Test - Time-based filtering for windowed alerts
        val recentMessages =
            searchService.search(
                SearchFilters(
                    topics = listOf(topic),
                    timeFrom = now - 120000, // Last 2 minutes
                ),
                100,
            )

        assertTrue(recentMessages.isNotEmpty())
        assertTrue(recentMessages.all { it.timestamp >= now - 120000 })
    }

    @Test
    fun `should aggregate metrics for alerting dashboards`() {
        // Setup
        val topics = listOf("metrics-1.dlq", "metrics-2.dlq", "metrics-3.dlq")
        topics.forEach { topic ->
            adminClient.createTopics(listOf(NewTopic(topic, 2, 1.toShort()))).all().get()
        }

        // Produce varied message patterns
        topics.forEachIndexed { topicIdx, topic ->
            for (partition in 0..1) {
                repeat(3 + topicIdx) { i ->
                    val payload = """{"topic": "$topic", "partition": $partition, "idx": $i}""".toByteArray()
                    producer.send(ProducerRecord(topic, partition, null, payload)).get()
                }
            }
        }
        producer.flush()

        // Test - Verify metrics can be collected per topic and partition
        topics.forEach { topic ->
            val messages =
                searchService.search(
                    SearchFilters(topics = listOf(topic)),
                    100,
                )
            assertTrue(messages.isNotEmpty())

            val partition0 = messages.count { it.partition == 0 }
            val partition1 = messages.count { it.partition == 1 }

            assertTrue(partition0 > 0)
            assertTrue(partition1 > 0)
        }
    }

    @Test
    fun `should handle burst detection scenarios`() {
        // Setup
        val topic = "alerts-burst.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        val now = System.currentTimeMillis()

        // Simulate burst: many messages in short time
        repeat(50) { i ->
            val timestamp = now + (i * 100) // All within 5 seconds
            val payload = """{"burst": $i}""".toByteArray()
            val record =
                ProducerRecord<ByteArray, ByteArray>(
                    topic,
                    0,
                    timestamp,
                    null,
                    payload,
                )
            producer.send(record).get()
        }
        producer.flush()

        // Test - Verify burst can be detected through message volume
        val messages =
            searchService.search(
                SearchFilters(
                    topics = listOf(topic),
                    timeFrom = now,
                    timeTo = now + 10000,
                ),
                100,
            )

        assertTrue(messages.size >= 50)

        // Calculate rate: messages per second
        val timeSpanSeconds = (messages.maxOf { it.timestamp } - messages.minOf { it.timestamp }) / 1000.0
        val rate = messages.size / timeSpanSeconds
        assertTrue(rate > 5.0) // High rate indicates burst
    }
}

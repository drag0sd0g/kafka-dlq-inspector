package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.ReplayRequest
import com.dragos.kafkainspector.model.SearchFilters
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties

/**
 * Integration test for REST API endpoints with real Kafka infrastructure
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class RestApiIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))

        @JvmStatic
        @DynamicPropertySource
        fun kafkaProps(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            registry.add("spring.kafka.consumer.group-id") { "rest-test-group" }
            registry.add("kafka.dlq.topic-pattern") { ".*\\.dlq" }
        }
    }

    @Autowired
    lateinit var mockMvc: MockMvc

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var adminClient: AdminClient

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
    fun `GET topics should return discovered DLQ topics`() {
        // Setup
        val topics = listOf("api-test-orders.dlq", "api-test-payments.dlq")
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

        // Test
        mockMvc
            .perform(get("/api/topics"))
            .andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$").isArray)
            .andExpect(jsonPath("$.length()").value(org.hamcrest.Matchers.greaterThanOrEqualTo(2)))
    }

    @Test
    fun `GET topics-topic-messages should return paginated messages`() {
        // Setup
        val topic = "api-messages-test.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        // Produce test messages
        repeat(10) { i ->
            val payload = """{"id": $i, "message": "test-$i"}""".toByteArray()
            producer.send(ProducerRecord(topic, 0, null, payload)).get()
        }
        producer.flush()

        // Test - Page 0
        mockMvc
            .perform(
                get("/api/topics/$topic/messages")
                    .param("page", "0")
                    .param("size", "5"),
            ).andExpect(status().isOk)
            .andExpect(jsonPath("$.items").isArray)
            .andExpect(jsonPath("$.items.length()").value(5))
            .andExpect(jsonPath("$.total").value(10))
            .andExpect(jsonPath("$.page").value(0))
            .andExpect(jsonPath("$.size").value(5))

        // Test - Page 1
        mockMvc
            .perform(
                get("/api/topics/$topic/messages")
                    .param("page", "1")
                    .param("size", "5"),
            ).andExpect(status().isOk)
            .andExpect(jsonPath("$.items.length()").value(5))
            .andExpect(jsonPath("$.page").value(1))
    }

    @Test
    fun `GET topics-topic-messages with partition filter should return filtered messages`() {
        // Setup
        val topic = "api-partition-test.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 3, 1.toShort()))).all().get()

        // Produce messages to different partitions
        for (partition in 0..2) {
            repeat(3) { i ->
                val payload = """{"partition": $partition, "index": $i}""".toByteArray()
                producer.send(ProducerRecord(topic, partition, null, payload)).get()
            }
        }
        producer.flush()

        // Test - Filter by partition 1
        mockMvc
            .perform(
                get("/api/topics/$topic/messages")
                    .param("partition", "1")
                    .param("size", "50"),
            ).andExpect(status().isOk)
            .andExpect(jsonPath("$.items").isArray)
            .andExpect(jsonPath("$.items[*].partition").value(org.hamcrest.Matchers.everyItem(org.hamcrest.Matchers.equalTo(1))))
    }

    @Test
    fun `GET topics-topic-messages-partition-offset should return specific message`() {
        // Setup
        val topic = "api-specific-message.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        // Produce messages
        repeat(5) { i ->
            val payload = """{"index": $i}""".toByteArray()
            producer.send(ProducerRecord(topic, 0, null, payload)).get()
        }
        producer.flush()

        // Test - Get message at offset 2
        mockMvc
            .perform(get("/api/topics/$topic/messages/0/2"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.topic").value(topic))
            .andExpect(jsonPath("$.partition").value(0))
            .andExpect(jsonPath("$.offset").value(2))
    }

    @Test
    fun `POST export-json should return JSON export`() {
        // Setup
        val topic = "api-export-json.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        repeat(3) { i ->
            val payload = """{"export-test": $i}""".toByteArray()
            producer.send(ProducerRecord(topic, 0, null, payload)).get()
        }
        producer.flush()

        val filters = SearchFilters(topics = listOf(topic))
        val filtersJson = objectMapper.writeValueAsString(filters)

        // Test
        mockMvc
            .perform(
                post("/api/export/json")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(filtersJson),
            ).andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$").isArray)
    }

    @Test
    fun `POST export-csv should return CSV export`() {
        // Setup
        val topic = "api-export-csv.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        repeat(3) { i ->
            val payload = """{"csv-test": $i}""".toByteArray()
            producer.send(ProducerRecord(topic, 0, null, payload)).get()
        }
        producer.flush()

        val filters = SearchFilters(topics = listOf(topic))
        val filtersJson = objectMapper.writeValueAsString(filters)

        // Test
        val result =
            mockMvc
                .perform(
                    post("/api/export/csv")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(filtersJson),
                ).andExpect(status().isOk)
                .andExpect(content().contentType("text/csv"))
                .andReturn()

        val csvContent = result.response.contentAsString
        assertTrue(csvContent.contains("topic"))
        assertTrue(csvContent.contains(topic))
    }

    @Test
    fun `POST replay with dryRun should return preview without replaying`() {
        // Setup
        val sourceTopic = "api-replay-source.dlq"
        val destinationTopic = "api-replay-dest"
        adminClient
            .createTopics(
                listOf(
                    NewTopic(sourceTopic, 1, 1.toShort()),
                    NewTopic(destinationTopic, 1, 1.toShort()),
                ),
            ).all()
            .get()

        repeat(3) { i ->
            val payload = """{"replay-test": $i}""".toByteArray()
            producer.send(ProducerRecord(sourceTopic, 0, null, payload)).get()
        }
        producer.flush()

        val replayRequest =
            ReplayRequest(
                cluster = "test",
                sourceTopic = sourceTopic,
                destinationTopic = destinationTopic,
                filters = SearchFilters(topics = listOf(sourceTopic)),
                dryRun = true,
            )
        val requestJson = objectMapper.writeValueAsString(replayRequest)

        // Test
        mockMvc
            .perform(
                post("/api/replay")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(requestJson),
            ).andExpect(status().isOk)
            .andExpect(jsonPath("$").isArray)
    }

    @Test
    fun `POST aggregations should return aggregated statistics`() {
        // Setup
        val topic = "api-aggregations.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 2, 1.toShort()))).all().get()

        // Produce messages to different partitions
        repeat(5) { i ->
            val partition = i % 2
            val payload = """{"agg-test": $i}""".toByteArray()
            val record = ProducerRecord(topic, partition, null, payload)
            if (i < 3) {
                record.headers().add("__ExceptionClass__", "java.lang.NullPointerException".toByteArray())
            } else {
                record.headers().add("__ExceptionClass__", "java.lang.RuntimeException".toByteArray())
            }
            producer.send(record).get()
        }
        producer.flush()

        val filters = SearchFilters(topics = listOf(topic))
        val filtersJson = objectMapper.writeValueAsString(filters)

        // Test
        mockMvc
            .perform(
                post("/api/aggregations")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(filtersJson),
            ).andExpect(status().isOk)
            .andExpect(jsonPath("$.totalMessages").exists())
            .andExpect(jsonPath("$.byTopic").isMap)
            .andExpect(jsonPath("$.byPartition").isMap)
    }

    @Test
    fun `should handle concurrent API requests`() {
        // Setup
        val topic = "api-concurrent.dlq"
        adminClient.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()

        repeat(10) { i ->
            val payload = """{"concurrent-test": $i}""".toByteArray()
            producer.send(ProducerRecord(topic, 0, null, payload)).get()
        }
        producer.flush()

        // Test - Multiple concurrent requests
        val results =
            (1..5).map {
                Thread {
                    mockMvc
                        .perform(get("/api/topics/$topic/messages"))
                        .andExpect(status().isOk)
                }
            }

        results.forEach { it.start() }
        results.forEach { it.join() }
    }
}

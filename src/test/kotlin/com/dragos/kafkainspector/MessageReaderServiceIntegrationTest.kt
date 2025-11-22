package com.dragos.kafkainspector

import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.MessageReaderService
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
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

@SpringBootTest
@Testcontainers
class MessageReaderServiceIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))

        @JvmStatic
        @DynamicPropertySource
        fun kafkaProps(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            registry.add("spring.kafka.consumer.group-id") { "test-group" }
            registry.add("kafka.dlq.topic-pattern") { "test-dlq" }
        }
    }

    @Autowired
    lateinit var adminClient: AdminClient

    @Autowired
    lateinit var readerService: MessageReaderService

    @Test
    fun `should read messages from dlq topic`() {
        adminClient.createTopics(listOf(NewTopic("test-dlq", 1, 1.toShort()))).all().get()

        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        KafkaProducer<ByteArray, ByteArray>(producerProps).use { producer ->
            producer.send(ProducerRecord("test-dlq", 0, null, "value".toByteArray()))
            producer.flush()
        }

        val messages = readerService.readMessages(listOf("test-dlq"), SearchFilters(topics = listOf("test-dlq")), 10)
        assertFalse(messages.isEmpty())
        assertTrue(messages.any { String(it.value) == "value" })
    }

    @Test
    fun `should return empty list when topic does not exist`() {
        // Test that reading from a non-existent topic doesn't crash and returns an empty list
        val messages = readerService.readMessages(listOf("non-existent-topic"), SearchFilters(topics = listOf("non-existent-topic")), 10)
        assertTrue(messages.isEmpty())
    }
}

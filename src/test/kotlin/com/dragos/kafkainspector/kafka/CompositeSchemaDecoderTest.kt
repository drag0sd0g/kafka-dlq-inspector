package com.dragos.kafkainspector.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.kafka.KafkaProperties

class CompositeSchemaDecoderTest {
    private val kafkaProperties =
        KafkaProperties().apply {
            consumer.properties["schema.registry.url"] = "mock://test"
        }
    private val decoder = CompositeSchemaDecoder(kafkaProperties)

    @Test
    fun `returns json tree when payload is valid json`() {
        val record = ConsumerRecord("topic", 0, 0, null, "{\"value\":123}".toByteArray())

        val result = decoder.decode(record)

        val number = result?.let { (it as com.fasterxml.jackson.databind.JsonNode)["value"].asInt() }

        assertEquals(123, number)
    }

    @Test
    fun `returns null for empty payload`() {
        val record = ConsumerRecord("topic", 0, 0, null, ByteArray(0))

        val result = decoder.decode(record)

        assertNull(result)
    }
}

package com.dragos.kafkainspector.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.stereotype.Component
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import java.util.Properties

interface SchemaDecoder {
    fun decode(record: ConsumerRecord<ByteArray, ByteArray>): Any?
}

@Component
class CompositeSchemaDecoder(
    private val kafkaProperties: KafkaProperties,
) : SchemaDecoder {
    private val objectMapper = ObjectMapper()

    override fun decode(record: ConsumerRecord<ByteArray, ByteArray>): Any? {
        val payload = record.value()
        if (payload.isEmpty()) return null
        
        // Try Avro decoding first, then fall back to JSON
        return decodeAvro(payload) ?: decodeJson(payload)
    }

    private fun decodeJson(bytes: ByteArray): Any? =
        try {
            objectMapper.readTree(bytes)
        } catch (_: Exception) {
            null
        }

    private fun decodeAvro(bytes: ByteArray): Any? =
        try {
            val props = Properties()
            props.putAll(kafkaProperties.consumer.properties.toMutableMap())
            props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] =
                kafkaProperties.properties[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG]
            val mapProps = props.entries.associate { it.key.toString() to it.value }
            val deserializer = KafkaAvroDeserializer().apply { configure(mapProps, false) }
            val result = deserializer.deserialize(null, bytes)
            if (result is GenericRecord) result else result?.toString()
        } catch (_: Exception) {
            null
        }
}

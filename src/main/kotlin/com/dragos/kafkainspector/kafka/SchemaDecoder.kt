package com.dragos.kafkainspector.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.stereotype.Component
// Note: Avro/Confluent imports commented out - JSON-only decoding as fallback
// Uncomment these and add dependencies to build.gradle.kts for full Avro support:
// import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
// import io.confluent.kafka.serializers.KafkaAvroDeserializer
// import org.apache.avro.generic.GenericRecord
// import java.util.Properties

interface SchemaDecoder {
    fun decode(record: ConsumerRecord<ByteArray, ByteArray>): Any?
}

@Component
class CompositeSchemaDecoder(
    private val kafkaProperties: KafkaProperties,
) : SchemaDecoder {
    private val objectMapper = ObjectMapper()

    override fun decode(record: ConsumerRecord<ByteArray, ByteArray>): Any? {
        // Try JSON (Avro support can be enabled by uncommenting code below and adding dependencies)
        val payload = record.value()
        if (payload.isEmpty()) return null
        return decodeJson(payload)
    }

    private fun decodeJson(bytes: ByteArray): Any? =
        try {
            objectMapper.readTree(bytes)
        } catch (_: Exception) {
            null
        }

    // Avro decoding - uncomment to enable (requires Confluent dependencies)
    // private fun decodeAvro(bytes: ByteArray): Any? =
    //     try {
    //         val props = Properties()
    //         props.putAll(kafkaProperties.consumer.properties.toMutableMap())
    //         props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] =
    //             kafkaProperties.properties[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG]
    //         val mapProps = props.entries.associate { it.key.toString() to it.value }
    //         val deserializer = KafkaAvroDeserializer().apply { configure(mapProps, false) }
    //         val result = deserializer.deserialize(null, bytes)
    //         if (result is GenericRecord) result else result?.toString()
    //     } catch (_: Exception) {
    //         null
    //     }
}

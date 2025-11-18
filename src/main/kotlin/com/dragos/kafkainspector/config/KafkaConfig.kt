package com.dragos.kafkainspector.config

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff
import java.time.Duration

@Configuration
class KafkaConfig(private val properties: KafkaProperties) {

    @Bean
    fun admin(): KafkaAdmin = KafkaAdmin(properties.buildAdminProperties(null))

    @Bean
    fun consumerFactory(): ConsumerFactory<ByteArray, ByteArray> {
        val config = properties.buildConsumerProperties(null).toMutableMap()
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        config[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        return DefaultKafkaConsumerFactory(config)
    }

    @Bean
    fun producerFactory(): ProducerFactory<ByteArray, ByteArray> {
        val config = properties.buildProducerProperties(null).toMutableMap()
        config[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        config[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        return DefaultKafkaProducerFactory(config)
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<ByteArray, ByteArray> = KafkaTemplate(producerFactory())

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<ByteArray, ByteArray> {
        val factory = ConcurrentKafkaListenerContainerFactory<ByteArray, ByteArray>()
        factory.consumerFactory = consumerFactory()
        factory.setCommonErrorHandler(errorHandler())
        return factory
    }

    @Bean
    fun errorHandler(): CommonErrorHandler = DefaultErrorHandler(null, FixedBackOff(5000L, FixedBackOff.UNLIMITED_ATTEMPTS))

    @Bean
    fun adminClient(): AdminClient = AdminClient.create(properties.buildAdminProperties(null))
}

package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.model.ReplayRequest
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.concurrent.TimeUnit

@Service
class ReplayService(
    private val messageReaderService: MessageReaderService,
    private val kafkaTemplate: KafkaTemplate<ByteArray, ByteArray>,
    private val meterRegistry: MeterRegistry,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun replay(request: ReplayRequest): List<String> {
        val filters = request.filters ?: return emptyList()
        val messages = messageReaderService.readMessages(listOf(request.sourceTopic), filters, 1000)
        if (request.dryRun) {
            logger.info("Dry-run replay for {} messages from {} to {}", messages.size, request.sourceTopic, request.destinationTopic)
            return messages.map { "DryRun:${it.topic}:${it.partition}:${it.offset}" }
        }

        // Force metadata refresh for destination topic before sending
        try {
            kafkaTemplate.partitionsFor(request.destinationTopic)
            logger.info("Destination topic {} metadata retrieved successfully", request.destinationTopic)
        } catch (e: Exception) {
            logger.warn("Failed to retrieve metadata for destination topic {}: {}", request.destinationTopic, e.message)
        }

        val rateLimiterNanos = request.ratePerSecond?.let { 1_000_000_000L / it } ?: 0
        val results = mutableListOf<String>()
        var lastSend = System.nanoTime()
        messages.forEach { msg ->
            if (rateLimiterNanos > 0) {
                val elapsed = System.nanoTime() - lastSend
                val sleepTime = rateLimiterNanos - elapsed
                if (sleepTime > 0) TimeUnit.NANOSECONDS.sleep(sleepTime)
            }
            val producerRecord = ProducerRecord(request.destinationTopic, msg.partition, msg.key, msg.value)
            kafkaTemplate.send(producerRecord).get(5, TimeUnit.SECONDS)
            meterRegistry.counter("dlq.replay.attempts", "topic", request.destinationTopic).increment()
            results.add("Replayed:${msg.topic}:${msg.partition}:${msg.offset}")
            lastSend = System.nanoTime()
        }
        return results
    }
}

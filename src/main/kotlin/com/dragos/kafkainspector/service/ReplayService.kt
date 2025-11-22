package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.model.ReplayRequest
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.concurrent.TimeUnit

/**
 * Service for replaying messages from DLQ topics to destination topics.
 * Supports dry-run mode for testing and rate limiting for controlled replay.
 */
@Service
class ReplayService(
    private val messageReaderService: MessageReaderService,
    private val kafkaTemplate: KafkaTemplate<ByteArray, ByteArray>,
    private val meterRegistry: MeterRegistry,
    @Value("\${kafka.replay.max-messages:1000}") private val replayMaxMessages: Int,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Replay messages from source to destination topic based on the provided request.
     *
     * @param request Replay configuration including source/destination topics, filters, and options
     * @return List of status strings indicating which messages were replayed or would be replayed
     */
    fun replay(request: ReplayRequest): List<String> {
        val filters = request.filters ?: return emptyList()
        val messages = messageReaderService.readMessages(listOf(request.sourceTopic), filters, replayMaxMessages)

        // If dry-run mode, only return what would be replayed without actually producing messages
        if (request.dryRun) {
            logger.info("Dry-run replay for {} messages from {} to {}", messages.size, request.sourceTopic, request.destinationTopic)
            return messages.map { "DryRun:${it.topic}:${it.partition}:${it.offset}" }
        }

        // Force metadata refresh for destination topic before sending to ensure it exists
        try {
            kafkaTemplate.partitionsFor(request.destinationTopic)
            logger.info("Destination topic {} metadata retrieved successfully", request.destinationTopic)
        } catch (e: Exception) {
            logger.warn("Failed to retrieve metadata for destination topic {}: {}", request.destinationTopic, e.message)
        }

        // Calculate rate limiting delay in nanoseconds if rate limit is specified
        val rateLimiterNanos = request.ratePerSecond?.let { 1_000_000_000L / it } ?: 0
        val results = mutableListOf<String>()
        var lastSend = System.nanoTime()

        // Replay each message with optional rate limiting
        messages.forEach { msg ->
            if (rateLimiterNanos > 0) {
                val elapsed = System.nanoTime() - lastSend
                val sleepTime = rateLimiterNanos - elapsed
                if (sleepTime > 0) TimeUnit.NANOSECONDS.sleep(sleepTime)
            }
            // Produce message to destination topic preserving partition and key
            val producerRecord = ProducerRecord(request.destinationTopic, msg.partition, msg.key, msg.value)
            kafkaTemplate.send(producerRecord).get(5, TimeUnit.SECONDS)
            meterRegistry.counter("dlq.replay.attempts", "topic", request.destinationTopic).increment()
            results.add("Replayed:${msg.topic}:${msg.partition}:${msg.offset}")
            lastSend = System.nanoTime()
        }
        return results
    }
}

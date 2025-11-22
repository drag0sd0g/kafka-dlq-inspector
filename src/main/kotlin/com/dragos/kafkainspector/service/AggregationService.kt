package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.AggregationResult
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.model.TimeWindowAggregation
import com.dragos.kafkainspector.model.TopicAggregation
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Service for computing aggregated statistics on DLQ messages.
 * Provides insights including message counts, exception types, size metrics, and time-based trends.
 */
@Service
class AggregationService(
    private val topicDiscovery: DlqTopicDiscovery,
    private val readerService: MessageReaderService,
    @Value("\${kafka.aggregation.limit-per-topic:500}") private val defaultLimitPerTopic: Int,
) {
    /**
     * Compute aggregations for DLQ messages based on filters.
     * Returns both per-topic statistics and time-windowed aggregations.
     *
     * @param filters Filtering criteria for messages to aggregate
     * @param limitPerTopic Maximum messages to read per topic for aggregation
     * @return Aggregation result containing topic-level and time-window statistics
     */
    fun aggregate(
        filters: SearchFilters,
        limitPerTopic: Int = defaultLimitPerTopic,
    ): AggregationResult {
        // Discover topics if none specified
        val topics = if (filters.topics.isEmpty()) topicDiscovery.discover().map { it.name } else filters.topics
        val messages = readerService.readMessages(topics, filters, limitPerTopic)

        // Compute per-topic aggregations including exception counts and size statistics
        val topicAgg =
            messages.groupBy { it.topic }.map { (topic, msgs) ->
                val exceptionCounts = msgs.groupingBy { it.exceptionClass ?: "unknown" }.eachCount().mapValues { it.value.toLong() }
                val sizes = msgs.map { it.sizeBytes }.sorted()
                val avg = if (sizes.isNotEmpty()) sizes.average() else 0.0
                // Calculate 95th percentile of message sizes
                val p95 = if (sizes.isNotEmpty()) sizes[(sizes.size * 95 / 100).coerceAtMost(sizes.lastIndex)] else 0
                TopicAggregation(
                    topic = topic,
                    partitionCount = msgs.map { it.partition }.distinct().size,
                    messageCount = msgs.size.toLong(),
                    exceptionCounts = exceptionCounts,
                    averageSizeBytes = avg,
                    p95SizeBytes = p95.toDouble(),
                )
            }

        // Compute time-window aggregations for the last hour using 5-minute windows
        val now = Instant.now()
        val windowStart = now.minus(1, ChronoUnit.HOURS).toEpochMilli()
        val windowAgg =
            messages.filter { it.timestamp >= windowStart }.groupBy { it.topic }.flatMap { (topic, msgs) ->
                val grouped = msgs.groupBy { it.timestamp / 300000 } // 5 minute windows
                grouped.map { (bucket, groupMsgs) ->
                    val start = bucket * 300000
                    val end = start + 300000
                    val rate = groupMsgs.size / 300.0 // Messages per second
                    TimeWindowAggregation(topic, start, end, groupMsgs.size.toLong(), rate)
                }
            }

        return AggregationResult(topicAgg, windowAgg)
    }
}

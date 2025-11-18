package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.AggregationResult
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.model.TimeWindowAggregation
import com.dragos.kafkainspector.model.TopicAggregation
import org.springframework.stereotype.Service
import java.time.Instant
import java.time.temporal.ChronoUnit

@Service
class AggregationService(
    private val topicDiscovery: DlqTopicDiscovery,
    private val readerService: MessageReaderService
) {
    fun aggregate(filters: SearchFilters, limitPerTopic: Int = 500): AggregationResult {
        val topics = if (filters.topics.isEmpty()) topicDiscovery.discover().map { it.name } else filters.topics
        val messages = readerService.readMessages(topics, filters, limitPerTopic)
        val topicAgg = messages.groupBy { it.topic }.map { (topic, msgs) ->
            val exceptionCounts = msgs.groupingBy { it.exceptionClass ?: "unknown" }.eachCount().mapValues { it.value.toLong() }
            val sizes = msgs.map { it.sizeBytes }.sorted()
            val avg = if (sizes.isNotEmpty()) sizes.average() else 0.0
            val p95 = if (sizes.isNotEmpty()) sizes[(sizes.size * 95 / 100).coerceAtMost(sizes.lastIndex)] else 0
            TopicAggregation(
                topic = topic,
                partitionCount = msgs.map { it.partition }.distinct().size,
                messageCount = msgs.size.toLong(),
                exceptionCounts = exceptionCounts,
                averageSizeBytes = avg,
                p95SizeBytes = p95.toDouble()
            )
        }

        val now = Instant.now()
        val windowStart = now.minus(1, ChronoUnit.HOURS).toEpochMilli()
        val windowAgg = messages.filter { it.timestamp >= windowStart }.groupBy { it.topic }.flatMap { (topic, msgs) ->
            val grouped = msgs.groupBy { it.timestamp / 300000 } // 5 minute windows
            grouped.map { (bucket, groupMsgs) ->
                val start = bucket * 300000
                val end = start + 300000
                val rate = groupMsgs.size / 300.0
                TimeWindowAggregation(topic, start, end, groupMsgs.size.toLong(), rate)
            }
        }

        return AggregationResult(topicAgg, windowAgg)
    }
}

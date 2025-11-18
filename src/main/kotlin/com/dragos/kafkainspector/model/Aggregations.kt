package com.dragos.kafkainspector.model

data class TopicAggregation(
    val topic: String,
    val partitionCount: Int,
    val messageCount: Long,
    val exceptionCounts: Map<String, Long>,
    val averageSizeBytes: Double,
    val p95SizeBytes: Double
)

data class TimeWindowAggregation(
    val topic: String,
    val windowStart: Long,
    val windowEnd: Long,
    val messageCount: Long,
    val ratePerSecond: Double
)

data class AggregationResult(
    val topics: List<TopicAggregation>,
    val windows: List<TimeWindowAggregation>
)

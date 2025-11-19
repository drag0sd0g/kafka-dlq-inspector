package com.dragos.kafkainspector.model

data class ReplayRequest(
    val cluster: String,
    val sourceTopic: String,
    val destinationTopic: String,
    val partition: Int? = null,
    val offsets: List<Long>? = null,
    val filters: SearchFilters? = null,
    val dryRun: Boolean = true,
    val ratePerSecond: Int? = null,
    val transactional: Boolean = false,
)

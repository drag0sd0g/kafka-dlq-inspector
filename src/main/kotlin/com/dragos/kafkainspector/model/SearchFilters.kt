package com.dragos.kafkainspector.model

import jakarta.validation.constraints.Pattern

/**
 * Filters used across search, export, and replay workflows.
 */
data class SearchFilters(
    val topics: List<String> = emptyList(),
    val timeFrom: Long? = null,
    val timeTo: Long? = null,
    @field:Pattern(regexp = ".*")
    val keyRegex: String? = null,
    @field:Pattern(regexp = ".*")
    val payloadRegex: String? = null,
    val headerMatch: Map<String, String> = emptyMap(),
    val exceptionTypes: List<String>? = null,
    val partitions: List<Int>? = null,
    val offsetFrom: Long? = null,
    val offsetTo: Long? = null
)

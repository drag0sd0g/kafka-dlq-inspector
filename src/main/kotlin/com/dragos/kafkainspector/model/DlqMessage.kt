package com.dragos.kafkainspector.model

data class DlqMessage(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val key: ByteArray?,
    val value: ByteArray,
    val decodedValue: Any?,
    val headers: Map<String, Any?>,
    val timestamp: Long,
    val ingestionTimestamp: Long?,
    val exceptionClass: String?,
    val exceptionMessage: String?,
    val stackTrace: String?,
    val originalTopic: String?,
    val sizeBytes: Long
)

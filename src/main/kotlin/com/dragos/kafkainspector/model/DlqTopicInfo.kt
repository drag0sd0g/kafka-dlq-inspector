package com.dragos.kafkainspector.model

data class DlqTopicInfo(
    val name: String,
    val partitions: Int,
    val messageCount: Long,
    val retentionMs: Long?
)

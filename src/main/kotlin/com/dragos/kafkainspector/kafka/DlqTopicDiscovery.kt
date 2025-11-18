package com.dragos.kafkainspector.kafka

import com.example.kafkainspector.model.DlqTopicInfo
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListOffsetsOptions
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class DlqTopicDiscovery(
    private val adminClient: AdminClient,
    @Value("\${kafka.dlq.topic-pattern:.*}") private val topicPattern: String
) {
    fun discover(): List<DlqTopicInfo> {
        val topics = adminClient.listTopics().names().get()
            .filter { it.matches(topicPattern.toRegex()) }
        if (topics.isEmpty()) return emptyList()

        val descriptions = adminClient.describeTopics(topics).all().get()
        val offsets = fetchEndOffsets(descriptions.keys)

        return descriptions.map { (name, desc) ->
            val partitions = desc.partitions().size
            val total = desc.partitions().sumOf { partition ->
                val tp = TopicPartition(name, partition.partition())
                offsets[tp] ?: 0L
            }
            DlqTopicInfo(
                name = name,
                partitions = partitions,
                messageCount = total,
                retentionMs = null
            )
        }
    }

    private fun fetchEndOffsets(topicNames: Set<String>): Map<TopicPartition, Long> {
        val topicPartitions = topicNames.flatMap { topic ->
            val partitions = adminClient.describeTopics(listOf(topic)).all().get()[topic]?.partitions() ?: emptyList()
            partitions.map { TopicPartition(topic, it.partition()) }
        }
        if (topicPartitions.isEmpty()) return emptyMap()
        return adminClient.listOffsets(
            topicPartitions.associateWith { org.apache.kafka.clients.admin.OffsetSpec.latest() },
            ListOffsetsOptions()
        ).all().get().mapValues { it.value.offset() }
    }
}

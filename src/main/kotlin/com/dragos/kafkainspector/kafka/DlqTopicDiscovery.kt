package com.dragos.kafkainspector.kafka

import com.dragos.kafkainspector.model.DlqTopicInfo
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListOffsetsOptions
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class DlqTopicDiscovery(
    private val adminClient: AdminClient,
    @Value("\${kafka.dlq.topic-pattern:.*}") private val topicPattern: String,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun discover(): List<DlqTopicInfo> {
        val allTopics =
            adminClient
                .listTopics()
                .names()
                .get()
        logger.debug("All topics from Kafka: {}", allTopics)
        logger.debug("Topic pattern: {}", topicPattern)

        val topics = allTopics.filter { it.matches(topicPattern.toRegex()) }
        logger.debug("Filtered topics matching pattern: {}", topics)

        if (topics.isEmpty()) return emptyList()

        val descriptions = adminClient.describeTopics(topics).allTopicNames().get()
        val offsets = fetchEndOffsets(descriptions.keys)

        return descriptions.map { (name, desc) ->
            val partitions = desc.partitions().size
            val total =
                desc.partitions().sumOf { partition ->
                    val tp = TopicPartition(name, partition.partition())
                    offsets[tp] ?: 0L
                }
            DlqTopicInfo(
                name = name,
                partitions = partitions,
                messageCount = total,
                retentionMs = null,
            )
        }
    }

    private fun fetchEndOffsets(topicNames: Set<String>): Map<TopicPartition, Long> {
        val topicPartitions =
            topicNames.flatMap { topic ->
                val partitions =
                    adminClient
                        .describeTopics(listOf(topic))
                        .allTopicNames()
                        .get()[topic]
                        ?.partitions() ?: emptyList()
                partitions.map { TopicPartition(topic, it.partition()) }
            }
        if (topicPartitions.isEmpty()) return emptyMap()
        return adminClient
            .listOffsets(
                topicPartitions.associateWith {
                    org.apache.kafka.clients.admin.OffsetSpec
                        .latest()
                },
                ListOffsetsOptions(),
            ).all()
            .get()
            .mapValues { it.value.offset() }
    }
}

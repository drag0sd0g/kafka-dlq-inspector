package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.SearchFilters
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class SearchService(
    private val topicDiscovery: DlqTopicDiscovery,
    private val messageReaderService: MessageReaderService,
    @Value("\${kafka.search.default-limit:200}") private val defaultLimit: Int,
) {
    fun search(
        filters: SearchFilters,
        limit: Int = defaultLimit,
    ): List<DlqMessage> {
        val topics = if (filters.topics.isNotEmpty()) filters.topics else topicDiscovery.discover().map { it.name }
        return messageReaderService.readMessages(topics, filters, limit)
    }
}

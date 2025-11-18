package com.dragos.kafkainspector.service

import com.example.kafkainspector.kafka.DlqTopicDiscovery
import com.example.kafkainspector.model.DlqMessage
import com.example.kafkainspector.model.SearchFilters
import org.springframework.stereotype.Service

@Service
class SearchService(
    private val topicDiscovery: DlqTopicDiscovery,
    private val messageReaderService: MessageReaderService
) {
    fun search(filters: SearchFilters, limit: Int = 200): List<DlqMessage> {
        val topics = if (filters.topics.isNotEmpty()) filters.topics else topicDiscovery.discover().map { it.name }
        return messageReaderService.readMessages(topics, filters, limit)
    }
}

package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.SearchFilters
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

/**
 * Service for searching and retrieving messages from DLQ topics.
 * Provides a high-level interface for filtering and limiting message retrieval.
 */
@Service
class SearchService(
    private val topicDiscovery: DlqTopicDiscovery,
    private val messageReaderService: MessageReaderService,
    @Value("\${kafka.search.default-limit:200}") private val defaultLimit: Int,
) {
    /**
     * Search for DLQ messages based on the provided filters.
     * If no topics are specified in filters, searches all discovered DLQ topics.
     *
     * @param filters Search criteria including topics, partitions, offset ranges, and timestamps
     * @param limit Maximum number of messages to return
     * @return List of DLQ messages matching the search criteria
     */
    fun search(
        filters: SearchFilters,
        limit: Int = defaultLimit,
    ): List<DlqMessage> {
        // If no specific topics are provided, discover and search all DLQ topics
        val topics = filters.topics.ifEmpty { topicDiscovery.discover().map { it.name } }
        return messageReaderService.readMessages(topics, filters, limit)
    }
}

package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.MessageReaderService
import com.dragos.kafkainspector.service.SearchService
import com.dragos.kafkainspector.util.PageResponse
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Value
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

/**
 * REST controller for DLQ message operations.
 * Provides endpoints to retrieve and filter messages from DLQ topics.
 */
@RestController
@RequestMapping("/api/topics")
class MessagesController(
    private val searchService: SearchService,
    private val messageReaderService: MessageReaderService,
    @Value("\${api.pagination.default-page-size:50}") private val defaultPageSize: Int,
) {
    /**
     * List messages from a specific DLQ topic with pagination and filtering.
     * Supports filtering by partition, offset range, and pagination.
     *
     * @param topic Topic name to retrieve messages from
     * @param partition Optional partition filter
     * @param offsetFrom Optional minimum offset (inclusive)
     * @param offsetTo Optional maximum offset (inclusive)
     * @param page Page number (0-indexed)
     * @param size Optional page size (defaults to configured default)
     * @return Paginated response containing messages and pagination metadata
     */
    @GetMapping("/{topic}/messages")
    fun listMessages(
        @PathVariable
        topic: String,
        @RequestParam(required = false)
        partition: Int?,
        @RequestParam(required = false)
        offsetFrom: Long?,
        @RequestParam(required = false)
        offsetTo: Long?,
        @RequestParam(defaultValue = "0")
        page: Int,
        @RequestParam(required = false)
        size: Int?,
    ): PageResponse<DlqMessage> {
        val pageSize = size ?: defaultPageSize
        val filters =
            SearchFilters(
                topics = listOf(topic),
                partitions = partition?.let { listOf(it) },
                offsetFrom = offsetFrom,
                offsetTo = offsetTo,
            )
        // Fetch enough messages to fill the requested page
        val result = searchService.search(filters, pageSize * (page + 1))
        val slice = result.drop(page * pageSize).take(pageSize)
        return PageResponse(slice, page, pageSize, result.size)
    }

    /**
     * Retrieve a specific message by topic, partition, and offset.
     *
     * @param topic Topic name
     * @param partition Partition number
     * @param offset Message offset
     * @return The requested message, or null if not found
     */
    @GetMapping("/{topic}/messages/{partition}/{offset}")
    fun getMessage(
        @PathVariable
        topic: String,
        @PathVariable
        partition: Int,
        @PathVariable
        offset: Long,
    ): DlqMessage? {
        val filters = SearchFilters(topics = listOf(topic), partitions = listOf(partition), offsetFrom = offset, offsetTo = offset)
        // Seek directly to the specific offset
        val seek = mapOf(TopicPartition(topic, partition) to offset)
        return messageReaderService.readMessages(listOf(topic), filters, 1, seek).firstOrNull()
    }
}

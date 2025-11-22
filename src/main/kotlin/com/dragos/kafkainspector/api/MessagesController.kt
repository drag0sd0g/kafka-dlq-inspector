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

@RestController
@RequestMapping("/api/topics")
class MessagesController(
    private val searchService: SearchService,
    private val messageReaderService: MessageReaderService,
    @Value("\${api.pagination.default-page-size:50}") private val defaultPageSize: Int,
) {
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
        val result = searchService.search(filters, pageSize * (page + 1))
        val slice = result.drop(page * pageSize).take(pageSize)
        return PageResponse(slice, page, pageSize, result.size)
    }

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
        val seek = mapOf(TopicPartition(topic, partition) to offset)
        return messageReaderService.readMessages(listOf(topic), filters, 1, seek).firstOrNull()
    }
}

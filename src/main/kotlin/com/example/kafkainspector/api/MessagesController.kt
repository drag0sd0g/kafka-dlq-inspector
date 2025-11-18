package com.example.kafkainspector.api

import com.example.kafkainspector.model.DlqMessage
import com.example.kafkainspector.model.SearchFilters
import com.example.kafkainspector.service.MessageReaderService
import com.example.kafkainspector.service.SearchService
import com.example.kafkainspector.util.PageResponse
import org.apache.kafka.common.TopicPartition
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/topics")
class MessagesController(
    private val searchService: SearchService,
    private val messageReaderService: MessageReaderService
) {
    @GetMapping("/{topic}/messages")
    fun listMessages(
        @PathVariable topic: String,
        @RequestParam(required = false) partition: Int?,
        @RequestParam(required = false) offsetFrom: Long?,
        @RequestParam(required = false) offsetTo: Long?,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "50") size: Int
    ): PageResponse<DlqMessage> {
        val filters = SearchFilters(
            topics = listOf(topic),
            partitions = partition?.let { listOf(it) },
            offsetFrom = offsetFrom,
            offsetTo = offsetTo
        )
        val result = searchService.search(filters, size * (page + 1))
        val slice = result.drop(page * size).take(size)
        return PageResponse(slice, page, size, result.size)
    }

    @GetMapping("/{topic}/messages/{partition}/{offset}")
    fun getMessage(
        @PathVariable topic: String,
        @PathVariable partition: Int,
        @PathVariable offset: Long
    ): DlqMessage? {
        val filters = SearchFilters(topics = listOf(topic), partitions = listOf(partition), offsetFrom = offset, offsetTo = offset)
        val seek = mapOf(TopicPartition(topic, partition) to offset)
        return messageReaderService.readMessages(listOf(topic), filters, 1, seek).firstOrNull()
    }
}

package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.MessageReaderService
import com.dragos.kafkainspector.service.SearchService
import com.dragos.kafkainspector.util.PageResponse
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.kafka.common.TopicPartition
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/topics")
@Tag(name = "Messages", description = "DLQ message retrieval operations")
class MessagesController(
    private val searchService: SearchService,
    private val messageReaderService: MessageReaderService,
) {
    @GetMapping("/{topic}/messages")
    @Operation(
        summary = "List messages from a topic",
        description = "Retrieve paginated messages from a DLQ topic with optional filtering by partition and offset range",
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Successfully retrieved messages"),
            ApiResponse(responseCode = "404", description = "Topic not found"),
        ],
    )
    fun listMessages(
        @Parameter(description = "Topic name", required = true)
        @PathVariable
        topic: String,
        @Parameter(description = "Filter by partition number")
        @RequestParam(required = false)
        partition: Int?,
        @Parameter(description = "Filter by offset from (inclusive)")
        @RequestParam(required = false)
        offsetFrom: Long?,
        @Parameter(description = "Filter by offset to (inclusive)")
        @RequestParam(required = false)
        offsetTo: Long?,
        @Parameter(description = "Page number (0-indexed)")
        @RequestParam(defaultValue = "0")
        page: Int,
        @Parameter(description = "Page size")
        @RequestParam(defaultValue = "50")
        size: Int,
    ): PageResponse<DlqMessage> {
        val filters =
            SearchFilters(
                topics = listOf(topic),
                partitions = partition?.let { listOf(it) },
                offsetFrom = offsetFrom,
                offsetTo = offsetTo,
            )
        val result = searchService.search(filters, size * (page + 1))
        val slice = result.drop(page * size).take(size)
        return PageResponse(slice, page, size, result.size)
    }

    @GetMapping("/{topic}/messages/{partition}/{offset}")
    @Operation(
        summary = "Get a specific message",
        description = "Retrieve a single message from a DLQ topic by partition and offset",
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Successfully retrieved message"),
            ApiResponse(responseCode = "404", description = "Message not found"),
        ],
    )
    fun getMessage(
        @Parameter(description = "Topic name", required = true)
        @PathVariable
        topic: String,
        @Parameter(description = "Partition number", required = true)
        @PathVariable
        partition: Int,
        @Parameter(description = "Message offset", required = true)
        @PathVariable
        offset: Long,
    ): DlqMessage? {
        val filters = SearchFilters(topics = listOf(topic), partitions = listOf(partition), offsetFrom = offset, offsetTo = offset)
        val seek = mapOf(TopicPartition(topic, partition) to offset)
        return messageReaderService.readMessages(listOf(topic), filters, 1, seek).firstOrNull()
    }
}

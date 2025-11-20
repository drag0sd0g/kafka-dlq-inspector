package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.DlqTopicInfo
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/topics")
@Tag(name = "Topics", description = "DLQ topic discovery operations")
class TopicsController(
    private val discovery: DlqTopicDiscovery,
) {
    @GetMapping
    @Operation(summary = "List DLQ topics", description = "Discover and list all dead letter queue topics matching the configured pattern")
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Successfully retrieved list of DLQ topics"),
        ],
    )
    fun listTopics(): List<DlqTopicInfo> = discovery.discover()
}

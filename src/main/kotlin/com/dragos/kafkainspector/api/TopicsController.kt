package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.DlqTopicInfo
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * REST controller for DLQ topic operations.
 * Provides endpoints to discover and list DLQ topics.
 */
@RestController
@RequestMapping("/api/topics")
class TopicsController(
    private val discovery: DlqTopicDiscovery,
) {
    /**
     * List all discovered DLQ topics.
     * Returns topic metadata including name and partition count.
     *
     * @return List of DLQ topic information
     */
    @GetMapping
    fun listTopics(): List<DlqTopicInfo> = discovery.discover()
}

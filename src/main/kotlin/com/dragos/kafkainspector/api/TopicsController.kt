package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.DlqTopicInfo
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/topics")
class TopicsController(
    private val discovery: DlqTopicDiscovery,
) {
    @GetMapping
    fun listTopics(): List<DlqTopicInfo> = discovery.discover()
}

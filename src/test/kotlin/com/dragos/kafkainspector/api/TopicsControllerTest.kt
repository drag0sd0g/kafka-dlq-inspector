package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.DlqTopicInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class TopicsControllerTest {
    private val discovery: DlqTopicDiscovery = mock()
    private val controller = TopicsController(discovery)

    @Test
    fun `returns discovered topics`() {
        val topics = listOf(DlqTopicInfo("dlq", 1, 10, null))
        whenever(discovery.discover()).thenReturn(topics)

        val result = controller.listTopics()

        assertEquals(topics, result)
    }
}

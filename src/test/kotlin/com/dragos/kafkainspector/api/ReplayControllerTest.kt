package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.ReplayRequest
import com.dragos.kafkainspector.service.ReplayService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ReplayControllerTest {
    private val replayService: ReplayService = mock()
    private val controller = ReplayController(replayService)

    @Test
    fun `delegates replay to service`() {
        whenever(replayService.replay(any())).thenReturn(listOf("ok"))
        val request = ReplayRequest(cluster = "local", sourceTopic = "source", destinationTopic = "dest")

        val result = controller.replay(request)

        assertEquals(listOf("ok"), result)
    }
}

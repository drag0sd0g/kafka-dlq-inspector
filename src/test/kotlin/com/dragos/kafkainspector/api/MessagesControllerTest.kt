package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.MessageReaderService
import com.dragos.kafkainspector.service.SearchService
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class MessagesControllerTest {
    private val searchService: SearchService = mock()
    private val readerService: MessageReaderService = mock()
    private val controller = MessagesController(searchService, readerService, 50)

    @Test
    fun `returns paginated messages`() {
        val messages =
            (1..5).map {
                DlqMessage(
                    topic = "topic",
                    partition = 0,
                    offset = it.toLong(),
                    key = null,
                    value = "value".toByteArray(),
                    decodedValue = null,
                    headers = emptyMap(),
                    timestamp = it.toLong(),
                    ingestionTimestamp = null,
                    exceptionClass = null,
                    exceptionMessage = null,
                    stackTrace = null,
                    originalTopic = null,
                    sizeBytes = 1,
                )
            }
        val filtersCaptor = argumentCaptor<SearchFilters>()
        whenever(searchService.search(filtersCaptor.capture(), any())).thenReturn(messages)

        val response = controller.listMessages("topic", null, null, null, page = 0, size = 2)

        assertEquals(2, response.items.size)
        assertEquals(5, response.total)
        assertEquals(listOf("topic"), filtersCaptor.firstValue.topics)
    }

    @Test
    fun `delegates to reader with seek offsets`() {
        val message =
            DlqMessage(
                topic = "topic",
                partition = 1,
                offset = 10,
                key = null,
                value = "value".toByteArray(),
                decodedValue = null,
                headers = emptyMap(),
                timestamp = 0,
                ingestionTimestamp = null,
                exceptionClass = null,
                exceptionMessage = null,
                stackTrace = null,
                originalTopic = null,
                sizeBytes = 1,
            )
        whenever(readerService.readMessages(any(), any(), any(), any())).thenReturn(listOf(message))

        val result = controller.getMessage("topic", 1, 10)

        assertNotNull(result)
        val seekCaptor = argumentCaptor<Map<TopicPartition, Long>>()
        verify(readerService).readMessages(any(), any(), any(), seekCaptor.capture())
        assertEquals(10, seekCaptor.firstValue[TopicPartition("topic", 1)])
    }
}

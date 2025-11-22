package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.SearchFilters
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class SearchServiceTest {
    private val discovery: DlqTopicDiscovery = mock()
    private val reader: MessageReaderService = mock()
    private val service = SearchService(discovery, reader, 200)

    @Test
    fun `uses discovered topics when none supplied`() {
        val filters = SearchFilters()
        whenever(discovery.discover()).thenReturn(emptyList())
        whenever(reader.readMessages(any(), any(), any(), any())).thenReturn(emptyList())

        service.search(filters, 5)

        verify(discovery).discover()
        verify(reader).readMessages(any(), any(), any(), any())
    }

    @Test
    fun `delegates search to reader when topics provided`() {
        val filters = SearchFilters(topics = listOf("dlq"))
        val expected = listOf<DlqMessage>()
        whenever(reader.readMessages(any(), any(), any(), any())).thenReturn(expected)

        val result = service.search(filters, 2)

        assertEquals(expected, result)
        verify(reader).readMessages(any(), any(), any(), any())
    }

    @Test
    fun `uses default limit when not specified`() {
        val filters = SearchFilters(topics = listOf("dlq"))
        val message =
            DlqMessage(
                topic = "dlq",
                partition = 0,
                offset = 0,
                key = null,
                value = "test".toByteArray(),
                decodedValue = null,
                headers = emptyMap(),
                timestamp = 0,
                ingestionTimestamp = null,
                exceptionClass = null,
                exceptionMessage = null,
                stackTrace = null,
                originalTopic = null,
                sizeBytes = 4,
            )
        whenever(reader.readMessages(any(), any(), any(), any())).thenReturn(listOf(message))

        val result = service.search(filters)

        assertEquals(1, result.size)
        verify(reader).readMessages(any(), any(), any(), any())
    }
}

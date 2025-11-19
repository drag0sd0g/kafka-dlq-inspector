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
    private val service = SearchService(discovery, reader)

    @Test
    fun `uses discovered topics when none supplied`() {
        val filters = SearchFilters()
        whenever(discovery.discover()).thenReturn(emptyList())
        whenever(reader.readMessages(any(), any(), any())).thenReturn(emptyList())

        service.search(filters, 5)

        verify(discovery).discover()
        verify(reader).readMessages(emptyList(), filters, 5)
    }

    @Test
    fun `delegates search to reader when topics provided`() {
        val filters = SearchFilters(topics = listOf("dlq"))
        val expected = listOf<DlqMessage>()
        whenever(reader.readMessages(any(), any(), any())).thenReturn(expected)

        val result = service.search(filters, 2)

        assertEquals(expected, result)
        verify(reader).readMessages(listOf("dlq"), filters, 2)
    }
}

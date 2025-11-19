package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.ExportService
import com.dragos.kafkainspector.service.SearchService
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ExportControllerTest {
    private val searchService: SearchService = mock()
    private val exportService: ExportService = mock()
    private val controller = ExportController(searchService, exportService)

    @Test
    fun `exports json with searched messages`() {
        val messages =
            listOf(
                DlqMessage(
                    topic = "topic",
                    partition = 0,
                    offset = 0,
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
                ),
            )
        whenever(searchService.search(any(), any())).thenReturn(messages)
        whenever(exportService.exportJson(any(), any())).thenAnswer { it.arguments[1] }

        val response = controller.exportJson(SearchFilters())

        val hasAttachmentHeader =
            response.headers.contentDisposition
                ?.toString()
                ?.contains("attachment") == true

        assertTrue(hasAttachmentHeader)
        assertTrue(response.body!!.isNotEmpty())
    }

    @Test
    fun `exports csv with searched messages`() {
        val messages =
            listOf(
                DlqMessage(
                    topic = "topic",
                    partition = 0,
                    offset = 0,
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
                ),
            )
        whenever(searchService.search(any(), any())).thenReturn(messages)
        whenever(exportService.exportCsv(any(), any())).thenAnswer { it.arguments[1] }

        val response = controller.exportCsv(SearchFilters())

        val hasAttachmentHeader =
            response.headers.contentDisposition
                ?.toString()
                ?.contains("attachment") == true

        assertTrue(hasAttachmentHeader)
        assertTrue(response.body!!.isNotEmpty())
    }
}

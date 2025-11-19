package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.model.DlqMessage
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File

class ExportServiceTest {
    private val service = ExportService()

    @Test
    fun `exports messages to json`() {
        val message = DlqMessage("topic", 0, 1, null, "value".toByteArray(), null, emptyMap(), 123, null, null, null, null, null, 5)
        val file = File.createTempFile("dlq", "json")

        service.exportJson(listOf(message), file)

        assertTrue(file.readText().contains("\"topic\""))
    }

    @Test
    fun `exports messages to csv`() {
        val message =
            DlqMessage(
                topic = "topic",
                partition = 0,
                offset = 1,
                key = "key".toByteArray(),
                value = "value".toByteArray(),
                decodedValue = null,
                headers = emptyMap(),
                timestamp = 123,
                ingestionTimestamp = null,
                exceptionClass = "IllegalStateException",
                exceptionMessage = "bad",
                stackTrace = null,
                originalTopic = null,
                sizeBytes = 5,
            )
        val file = File.createTempFile("dlq", "csv")

        service.exportCsv(listOf(message), file)

        val content = file.readLines()
        assertTrue(content.first().contains("topic"))
        assertTrue(content[1].contains("IllegalStateException"))
    }
}

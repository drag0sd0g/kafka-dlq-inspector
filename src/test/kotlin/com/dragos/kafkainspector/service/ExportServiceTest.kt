package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.model.DlqMessage
import org.junit.jupiter.api.Assertions.assertEquals
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

    @Test
    fun `exports empty list to json`() {
        val file = File.createTempFile("dlq", "json")

        service.exportJson(emptyList(), file)

        val content = file.readText()
        // Parse JSON to verify it's an empty array
        val parsed =
            com.fasterxml.jackson.module.kotlin
                .jacksonObjectMapper()
                .readTree(content)
        assertTrue(parsed.isArray)
        assertEquals(0, parsed.size())
    }

    @Test
    fun `exports empty list to csv`() {
        val file = File.createTempFile("dlq", "csv")

        service.exportCsv(emptyList(), file)

        val content = file.readLines()
        assertTrue(content.first().contains("topic"))
        assertEquals(1, content.size) // Only header
    }

    @Test
    fun `handles csv values with special characters`() {
        val message =
            DlqMessage(
                topic = "topic",
                partition = 0,
                offset = 1,
                key = "key".toByteArray(),
                value = "value,with,commas".toByteArray(),
                decodedValue = null,
                headers = emptyMap(),
                timestamp = 123,
                ingestionTimestamp = null,
                exceptionClass = "Exception",
                exceptionMessage = "message,with,commas",
                stackTrace = null,
                originalTopic = null,
                sizeBytes = 5,
            )
        val file = File.createTempFile("dlq", "csv")

        service.exportCsv(listOf(message), file)

        val content = file.readLines()
        assertEquals(2, content.size) // Header + 1 row
        assertTrue(content[1].contains("commas"))
    }
}

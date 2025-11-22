package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.model.DlqMessage
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.stereotype.Service
import java.io.File

/**
 * Service for exporting DLQ messages to various file formats.
 * Supports JSON and CSV exports for offline analysis and archival.
 */
@Service
class ExportService {
    private val objectMapper = ObjectMapper().registerKotlinModule()

    /**
     * Export DLQ messages to a JSON file with pretty printing.
     *
     * @param messages List of DLQ messages to export
     * @param file Target file for the JSON export
     * @return The file that was written to
     */
    fun exportJson(
        messages: List<DlqMessage>,
        file: File,
    ): File {
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, messages)
        return file
    }

    /**
     * Export DLQ messages to a CSV file with headers.
     * Includes all message metadata and exception information.
     *
     * @param messages List of DLQ messages to export
     * @param file Target file for the CSV export
     * @return The file that was written to
     */
    fun exportCsv(
        messages: List<DlqMessage>,
        file: File,
    ): File {
        val headers =
            listOf(
                "topic",
                "partition",
                "offset",
                "timestamp",
                "key",
                "value",
                "exceptionClass",
                "exceptionMessage",
            )
        file.bufferedWriter().use { writer ->
            writer.appendLine(headers.joinToString(","))
            messages.forEach { msg ->
                val row =
                    listOf(
                        msg.topic,
                        msg.partition.toString(),
                        msg.offset.toString(),
                        msg.timestamp.toString(),
                        msg.key?.let { String(it) } ?: "",
                        String(msg.value),
                        msg.exceptionClass ?: "",
                        msg.exceptionMessage ?: "",
                    )
                writer.appendLine(row.joinToString(","))
            }
        }
        return file
    }
}

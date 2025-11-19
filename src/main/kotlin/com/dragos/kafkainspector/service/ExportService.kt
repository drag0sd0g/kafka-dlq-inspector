package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.model.DlqMessage
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.stereotype.Service
import java.io.File

@Service
class ExportService {
    private val objectMapper = ObjectMapper().registerKotlinModule()

    fun exportJson(
        messages: List<DlqMessage>,
        file: File,
    ): File {
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, messages)
        return file
    }

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

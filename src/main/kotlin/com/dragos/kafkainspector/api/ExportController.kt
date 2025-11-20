package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.ExportService
import com.dragos.kafkainspector.service.SearchService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.io.File

@RestController
@RequestMapping("/api/export")
@Tag(name = "Export", description = "DLQ message export operations")
class ExportController(
    private val searchService: SearchService,
    private val exportService: ExportService,
    @Value("\${kafka.export.max-records:1000}") private val exportMaxRecords: Int,
) {
    @PostMapping("/json", produces = [MediaType.APPLICATION_JSON_VALUE])
    @Operation(
        summary = "Export messages to JSON",
        description = "Export DLQ messages matching the provided filters to a JSON file",
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Successfully exported messages to JSON"),
            ApiResponse(responseCode = "400", description = "Invalid search filters"),
        ],
    )
    fun exportJson(
        @RequestBody filters: SearchFilters,
    ): ResponseEntity<ByteArray> {
        val messages = searchService.search(filters, exportMaxRecords)
        val file = File.createTempFile("dlq", "json")
        exportService.exportJson(messages, file)
        return ResponseEntity
            .ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=dlq.json")
            .body(file.readBytes())
    }

    @PostMapping("/csv", produces = ["text/csv"])
    @Operation(
        summary = "Export messages to CSV",
        description = "Export DLQ messages matching the provided filters to a CSV file",
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Successfully exported messages to CSV"),
            ApiResponse(responseCode = "400", description = "Invalid search filters"),
        ],
    )
    fun exportCsv(
        @RequestBody filters: SearchFilters,
    ): ResponseEntity<ByteArray> {
        val messages = searchService.search(filters, exportMaxRecords)
        val file = File.createTempFile("dlq", "csv")
        exportService.exportCsv(messages, file)
        return ResponseEntity
            .ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=dlq.csv")
            .body(file.readBytes())
    }
}

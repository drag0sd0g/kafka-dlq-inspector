package com.example.kafkainspector.api

import com.example.kafkainspector.model.SearchFilters
import com.example.kafkainspector.service.ExportService
import com.example.kafkainspector.service.SearchService
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
class ExportController(
    private val searchService: SearchService,
    private val exportService: ExportService
) {
    @PostMapping("/json", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun exportJson(@RequestBody filters: SearchFilters): ResponseEntity<ByteArray> {
        val messages = searchService.search(filters, 1_000)
        val file = File.createTempFile("dlq", "json")
        exportService.exportJson(messages, file)
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=dlq.json")
            .body(file.readBytes())
    }

    @PostMapping("/csv", produces = ["text/csv"])
    fun exportCsv(@RequestBody filters: SearchFilters): ResponseEntity<ByteArray> {
        val messages = searchService.search(filters, 1_000)
        val file = File.createTempFile("dlq", "csv")
        exportService.exportCsv(messages, file)
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=dlq.csv")
            .body(file.readBytes())
    }
}

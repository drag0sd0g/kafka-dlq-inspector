package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.AggregationResult
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.AggregationService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/aggregations")
@Tag(name = "Aggregations", description = "DLQ message aggregation operations")
class AggregationsController(
    private val aggregationService: AggregationService,
) {
    @PostMapping
    @Operation(
        summary = "Aggregate messages",
        description = "Compute aggregations on DLQ messages by topic, partition, exception type, and time windows",
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Successfully computed aggregations"),
            ApiResponse(responseCode = "400", description = "Invalid search filters"),
        ],
    )
    fun aggregate(
        @RequestBody filters: SearchFilters,
    ): AggregationResult = aggregationService.aggregate(filters)
}

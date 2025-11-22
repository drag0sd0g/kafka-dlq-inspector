package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.AggregationResult
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.AggregationService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/aggregations")
class AggregationsController(
    private val aggregationService: AggregationService,
) {
    @PostMapping
    fun aggregate(
        @RequestBody filters: SearchFilters,
    ): AggregationResult = aggregationService.aggregate(filters)
}

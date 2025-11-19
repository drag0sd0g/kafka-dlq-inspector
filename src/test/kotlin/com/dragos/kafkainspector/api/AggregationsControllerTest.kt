package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.AggregationResult
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.model.TimeWindowAggregation
import com.dragos.kafkainspector.model.TopicAggregation
import com.dragos.kafkainspector.service.AggregationService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class AggregationsControllerTest {
    private val aggregationService: AggregationService = mock()
    private val controller = AggregationsController(aggregationService)

    @Test
    fun `returns aggregations from service`() {
        val aggregation =
            AggregationResult(
                topics = listOf(TopicAggregation("t", 1, 1, emptyMap(), 0.0, 0.0)),
                windows = listOf(TimeWindowAggregation("t", 0, 1, 1, 0.1)),
            )
        whenever(aggregationService.aggregate(any())).thenReturn(aggregation)

        val result = controller.aggregate(SearchFilters())

        assertEquals(aggregation, result)
    }
}

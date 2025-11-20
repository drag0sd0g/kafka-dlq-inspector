package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.ReplayRequest
import com.dragos.kafkainspector.service.ReplayService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/replay")
@Tag(name = "Replay", description = "DLQ message replay operations")
class ReplayController(
    private val replayService: ReplayService,
) {
    @PostMapping
    @Operation(
        summary = "Replay messages",
        description = "Replay DLQ messages to original or alternate topics with optional dry-run mode and rate limiting",
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Successfully replayed messages or performed dry-run"),
            ApiResponse(responseCode = "400", description = "Invalid replay request"),
        ],
    )
    fun replay(
        @RequestBody request: ReplayRequest,
    ): List<String> = replayService.replay(request)
}

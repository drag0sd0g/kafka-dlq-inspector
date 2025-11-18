package com.dragos.kafkainspector.api

import com.dragos.kafkainspector.model.ReplayRequest
import com.dragos.kafkainspector.service.ReplayService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/replay")
class ReplayController(private val replayService: ReplayService) {

    @PostMapping
    fun replay(@RequestBody request: ReplayRequest): List<String> = replayService.replay(request)
}

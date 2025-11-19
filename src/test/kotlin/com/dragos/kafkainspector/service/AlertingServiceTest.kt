package com.dragos.kafkainspector.service

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AlertingServiceTest {
    private val meterRegistry = SimpleMeterRegistry()

    @Test
    fun `increments counter when threshold exceeded`() {
        val service = AlertingService(meterRegistry, "", "")

        service.notifyIfThresholdExceeded("topic", 1500, threshold = 1000)

        val counter = meterRegistry.find("dlq.alerts.triggered").counter()
        assertEquals(1.0, counter?.count())
    }

    @Test
    fun `ignores counts under threshold`() {
        val service = AlertingService(meterRegistry, "", "")

        service.notifyIfThresholdExceeded("topic", 10, threshold = 1000)

        val counter = meterRegistry.find("dlq.alerts.triggered").counter()
        assertEquals(0.0, counter?.count() ?: 0.0)
    }
}

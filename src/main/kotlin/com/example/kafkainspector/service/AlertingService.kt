package com.example.kafkainspector.service

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate

@Service
class AlertingService(
    private val meterRegistry: MeterRegistry,
    @Value("\${notifications.slack.webhook-url:}") private val slackWebhook: String,
    @Value("\${notifications.webhook.url:}") private val genericWebhook: String
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val restTemplate = RestTemplate()

    fun notifyIfThresholdExceeded(topic: String, count: Long, threshold: Long = 1000) {
        if (count < threshold) return
        val message = "DLQ threshold exceeded for $topic: $count messages"
        sendSlack(message)
        sendWebhook(mapOf("topic" to topic, "count" to count))
        meterRegistry.counter("dlq.alerts.triggered", "topic", topic).increment()
    }

    private fun sendSlack(text: String) {
        if (slackWebhook.isBlank()) {
            logger.debug("Slack webhook not configured")
            return
        }
        val headers = HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }
        val body = mapOf("text" to text)
        runCatching { restTemplate.postForEntity(slackWebhook, HttpEntity(body, headers), String::class.java) }
            .onFailure { logger.warn("Failed to send Slack notification", it) }
    }

    private fun sendWebhook(payload: Map<String, Any>) {
        if (genericWebhook.isBlank()) return
        val headers = HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }
        runCatching { restTemplate.postForEntity(genericWebhook, HttpEntity(payload, headers), String::class.java) }
            .onFailure { logger.warn("Failed to send webhook notification", it) }
    }
}

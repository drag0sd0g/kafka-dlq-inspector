package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.ReplayRequest
import com.dragos.kafkainspector.model.SearchFilters
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.util.concurrent.CompletableFuture

class ReplayServiceTest {
    private val reader: MessageReaderService = mock()
    private val kafkaTemplate: KafkaTemplate<ByteArray, ByteArray> = mock()
    private val meterRegistry = SimpleMeterRegistry()
    private val service = ReplayService(reader, kafkaTemplate, meterRegistry, 1000)

    @Test
    fun `performs dry run without producing messages`() {
        val message =
            DlqMessage(
                topic = "src",
                partition = 0,
                offset = 1,
                key = null,
                value = "payload".toByteArray(),
                decodedValue = null,
                headers = emptyMap(),
                timestamp = 0,
                ingestionTimestamp = null,
                exceptionClass = null,
                exceptionMessage = null,
                stackTrace = null,
                originalTopic = null,
                sizeBytes = 7L,
            )
        whenever(reader.readMessages(any(), any(), any(), any())).thenReturn(listOf(message))

        val request =
            ReplayRequest(
                cluster = "local",
                sourceTopic = "src",
                destinationTopic = "dest",
                dryRun = true,
                filters = SearchFilters(),
            )

        val result = service.replay(request)

        assertEquals(listOf("DryRun:src:0:1"), result)
    }

    @Test
    fun `sends messages to destination topic`() {
        val message =
            DlqMessage(
                topic = "src",
                partition = 0,
                offset = 1,
                key = null,
                value = "payload".toByteArray(),
                decodedValue = null,
                headers = emptyMap(),
                timestamp = 0,
                ingestionTimestamp = null,
                exceptionClass = null,
                exceptionMessage = null,
                stackTrace = null,
                originalTopic = null,
                sizeBytes = 7L,
            )
        whenever(reader.readMessages(any(), any(), any(), any())).thenReturn(listOf(message))
        val future = CompletableFuture.completedFuture<SendResult<ByteArray, ByteArray>>(null)
        whenever(kafkaTemplate.send(any<org.apache.kafka.clients.producer.ProducerRecord<ByteArray, ByteArray>>())).thenReturn(future)

        val request =
            ReplayRequest(
                cluster = "local",
                sourceTopic = "src",
                destinationTopic = "dest",
                dryRun = false,
                filters = SearchFilters(),
            )

        val result = service.replay(request)

        assertEquals(listOf("Replayed:src:0:1"), result)
        assertTrue(meterRegistry.find("dlq.replay.attempts").counter()!!.count() > 0)
        verify(kafkaTemplate).send(any<org.apache.kafka.clients.producer.ProducerRecord<ByteArray, ByteArray>>())
    }

    @Test
    fun `returns empty list when filters are null`() {
        val request =
            ReplayRequest(
                cluster = "local",
                sourceTopic = "src",
                destinationTopic = "dest",
                dryRun = false,
                filters = null,
            )

        val result = service.replay(request)

        assertEquals(emptyList<String>(), result)
    }

    @Test
    fun `returns empty list when no messages found`() {
        whenever(reader.readMessages(any(), any(), any(), any())).thenReturn(emptyList())

        val request =
            ReplayRequest(
                cluster = "local",
                sourceTopic = "src",
                destinationTopic = "dest",
                dryRun = false,
                filters = SearchFilters(),
            )

        val result = service.replay(request)

        assertEquals(emptyList<String>(), result)
    }

    @Test
    fun `throws exception when kafka send fails`() {
        val message =
            DlqMessage(
                topic = "src",
                partition = 0,
                offset = 1,
                key = null,
                value = "payload".toByteArray(),
                decodedValue = null,
                headers = emptyMap(),
                timestamp = 0,
                ingestionTimestamp = null,
                exceptionClass = null,
                exceptionMessage = null,
                stackTrace = null,
                originalTopic = null,
                sizeBytes = 7L,
            )
        whenever(reader.readMessages(any(), any(), any(), any())).thenReturn(listOf(message))
        val future = CompletableFuture<SendResult<ByteArray, ByteArray>>()
        future.completeExceptionally(RuntimeException("Send failed"))
        whenever(kafkaTemplate.send(any<org.apache.kafka.clients.producer.ProducerRecord<ByteArray, ByteArray>>())).thenReturn(future)

        val request =
            ReplayRequest(
                cluster = "local",
                sourceTopic = "src",
                destinationTopic = "dest",
                dryRun = false,
                filters = SearchFilters(),
            )

        assertThrows<Exception> {
            service.replay(request)
        }
    }
}

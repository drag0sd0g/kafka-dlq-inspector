package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.ReplayRequest
import com.dragos.kafkainspector.model.SearchFilters
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.util.concurrent.SettableListenableFuture

class ReplayServiceTest {
    private val reader: MessageReaderService = mock()
    private val kafkaTemplate: KafkaTemplate<ByteArray, ByteArray> = mock()
    private val meterRegistry = SimpleMeterRegistry()
    private val service = ReplayService(reader, kafkaTemplate, meterRegistry)

    @Test
    fun `performs dry run without producing messages`() {
        val message = DlqMessage("src", 0, 1, null, "payload".toByteArray(), null, emptyMap(), 0, null, null, null, null, null, 10)
        whenever(reader.readMessages(any(), any(), any())).thenReturn(listOf(message))

        val request = ReplayRequest(sourceTopic = "src", destinationTopic = "dest", dryRun = true, filters = SearchFilters())

        val result = service.replay(request)

        assertEquals(listOf("DryRun:src:0:1"), result)
    }

    @Test
    fun `sends messages to destination topic`() {
        val message = DlqMessage("src", 0, 1, null, "payload".toByteArray(), null, emptyMap(), 0, null, null, null, null, null, 10)
        whenever(reader.readMessages(any(), any(), any())).thenReturn(listOf(message))
        val future = SettableListenableFuture<SendResult<ByteArray, ByteArray>>()
        future.set(null)
        whenever(kafkaTemplate.send(any<org.apache.kafka.clients.producer.ProducerRecord<ByteArray, ByteArray>>())).thenReturn(future)

        val request = ReplayRequest(sourceTopic = "src", destinationTopic = "dest", dryRun = false, filters = SearchFilters())

        val result = service.replay(request)

        assertEquals(listOf("Replayed:src:0:1"), result)
        assertTrue(meterRegistry.find("dlq.replay.attempts").counter()!!.count() > 0)
        verify(kafkaTemplate).send(any())
    }
}

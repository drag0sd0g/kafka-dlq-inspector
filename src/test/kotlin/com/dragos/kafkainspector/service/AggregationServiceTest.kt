package com.dragos.kafkainspector.service

import com.dragos.kafkainspector.kafka.DlqTopicDiscovery
import com.dragos.kafkainspector.model.DlqMessage
import com.dragos.kafkainspector.model.DlqTopicInfo
import com.dragos.kafkainspector.model.SearchFilters
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Instant

class AggregationServiceTest {
    private val discovery: DlqTopicDiscovery = mock()
    private val reader: MessageReaderService = mock()
    private val service = AggregationService(discovery, reader)

    @Test
    fun `aggregates message stats across topics`() {
        val now = Instant.now().toEpochMilli()
        val messages =
            listOf(
                DlqMessage(
                    topic = "topicA",
                    partition = 0,
                    offset = 0,
                    key = null,
                    value = "one".toByteArray(),
                    decodedValue = "one",
                    headers = emptyMap(),
                    timestamp = now,
                    ingestionTimestamp = now,
                    exceptionClass = "IllegalStateException",
                    exceptionMessage = "bad",
                    stackTrace = null,
                    originalTopic = null,
                    sizeBytes = 4,
                ),
                DlqMessage(
                    topic = "topicA",
                    partition = 1,
                    offset = 1,
                    key = null,
                    value = "two".toByteArray(),
                    decodedValue = "two",
                    headers = emptyMap(),
                    timestamp = now - 10_000,
                    ingestionTimestamp = now,
                    exceptionClass = "IllegalArgumentException",
                    exceptionMessage = "worse",
                    stackTrace = null,
                    originalTopic = null,
                    sizeBytes = 8,
                ),
                DlqMessage(
                    topic = "topicB",
                    partition = 0,
                    offset = 2,
                    key = null,
                    value = "three".toByteArray(),
                    decodedValue = "three",
                    headers = emptyMap(),
                    timestamp = now - 100_000,
                    ingestionTimestamp = now,
                    exceptionClass = null,
                    exceptionMessage = null,
                    stackTrace = null,
                    originalTopic = null,
                    sizeBytes = 12,
                ),
            )
        whenever(discovery.discover()).thenReturn(listOf(DlqTopicInfo("topicA", 2, 0, null), DlqTopicInfo("topicB", 1, 0, null)))
        whenever(reader.readMessages(any(), any(), any())).thenReturn(messages)

        val result = service.aggregate(SearchFilters())

        assertEquals(2, result.topics.size)
        val topicA = result.topics.first { it.topic == "topicA" }
        assertEquals(2, topicA.partitionCount)
        assertEquals(2, topicA.messageCount)
        assertEquals(mapOf("IllegalStateException" to 1L, "IllegalArgumentException" to 1L), topicA.exceptionCounts)
        assertEquals(6.0, topicA.averageSizeBytes)
        assertEquals(8.0, topicA.p95SizeBytes)

        val topicB = result.topics.first { it.topic == "topicB" }
        assertEquals(1, topicB.partitionCount)
        assertEquals(1, topicB.messageCount)

        assertTrue(result.windows.any { it.topic == "topicA" && it.messageCount > 0 })
    }
}

package com.dragos.kafkainspector.service

import com.example.kafkainspector.kafka.SchemaDecoder
import com.example.kafkainspector.model.DlqMessage
import com.example.kafkainspector.model.SearchFilters
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.regex.Pattern

@Service
class MessageReaderService(
    private val consumerFactory: ConsumerFactory<ByteArray, ByteArray>,
    private val decoder: SchemaDecoder,
    private val meterRegistry: MeterRegistry,
    @Value("\${kafka.dlq.poll-size:500}") private val pollSize: Int,
    @Value("\${kafka.dlq.poll-timeout-ms:1000}") private val pollTimeoutMs: Long
) {
    fun readMessages(
        topics: List<String>,
        filters: SearchFilters,
        limit: Int,
        seekOffsets: Map<TopicPartition, Long> = emptyMap()
    ): List<DlqMessage> {
        if (topics.isEmpty()) return emptyList()
        val props = consumerFactory.configurationProperties.toMutableMap()
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = pollSize
        KafkaConsumer<ByteArray, ByteArray>(props).use { consumer ->
            val partitions = topics.flatMap { topic ->
                consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
            }
            consumer.assign(partitions)
            seekOffsets.forEach { (tp, offset) -> consumer.seek(tp, offset) }
            val messages = mutableListOf<DlqMessage>()
            while (messages.size < limit) {
                val records = consumer.poll(Duration.ofMillis(pollTimeoutMs))
                if (records.isEmpty) break
                records.forEach { record ->
                    if (matchesFilters(record, filters)) {
                        val decoded = decoder.decode(record)
                        messages.add(
                            DlqMessage(
                                topic = record.topic(),
                                partition = record.partition(),
                                offset = record.offset(),
                                key = record.key(),
                                value = record.value(),
                                decodedValue = decoded,
                                headers = record.headers().associate { it.key() to String(it.value()) },
                                timestamp = record.timestamp(),
                                ingestionTimestamp = record.timestamp(),
                                exceptionClass = record.headers().lastHeader("exception-class")?.let { String(it.value()) },
                                exceptionMessage = record.headers().lastHeader("exception-message")?.let { String(it.value()) },
                                stackTrace = record.headers().lastHeader("exception-stacktrace")?.let { String(it.value()) },
                                originalTopic = record.headers().lastHeader("original-topic")?.let { String(it.value()) },
                                sizeBytes = (record.serializedKeySize() + record.serializedValueSize()).toLong()
                            )
                        )
                        meterRegistry.counter("dlq.messages.read", "topic", record.topic()).increment()
                    }
                }
            }
            return messages
        }
    }

    private fun matchesFilters(record: org.apache.kafka.clients.consumer.ConsumerRecord<ByteArray, ByteArray>, filters: SearchFilters): Boolean {
        val timestamp = record.timestamp()
        if (filters.timeFrom != null && timestamp < filters.timeFrom) return false
        if (filters.timeTo != null && timestamp > filters.timeTo) return false
        if (filters.partitions != null && record.partition() !in filters.partitions) return false
        if (filters.offsetFrom != null && record.offset() < filters.offsetFrom) return false
        if (filters.offsetTo != null && record.offset() > filters.offsetTo) return false

        filters.keyRegex?.let {
            val regex = Pattern.compile(it)
            val keyStr = record.key()?.let { k -> String(k) } ?: ""
            if (!regex.matcher(keyStr).find()) return false
        }

        filters.payloadRegex?.let {
            val regex = Pattern.compile(it)
            if (!regex.matcher(String(record.value())).find()) return false
        }

        if (filters.headerMatch.isNotEmpty()) {
            val headerMap = record.headers().associate { h -> h.key() to String(h.value()) }
            val matched = filters.headerMatch.all { (k, v) -> headerMap[k] == v }
            if (!matched) return false
        }

        filters.exceptionTypes?.let { types ->
            val exceptionHeader = record.headers().lastHeader("exception-class")?.let { String(it.value()) }
            if (exceptionHeader == null || types.none { exceptionHeader.contains(it) }) return false
        }
        return true
    }
}

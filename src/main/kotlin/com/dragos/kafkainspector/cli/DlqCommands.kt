package com.dragos.kafkainspector.cli

import com.example.kafkainspector.model.ReplayRequest
import com.example.kafkainspector.model.SearchFilters
import com.example.kafkainspector.service.AggregationService
import com.example.kafkainspector.service.ExportService
import com.example.kafkainspector.service.ReplayService
import com.example.kafkainspector.service.SearchService
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import picocli.CommandLine
import java.io.File
import java.util.concurrent.Callable

@Component
@CommandLine.Command(
    name = "dlq",
    description = ["Kafka DLQ Inspector CLI"],
    subcommands = [
        ListTopicsCommand::class,
        ShowCommand::class,
        AggregateCommand::class,
        ReplayCommand::class,
        ExportCommand::class
    ]
)
class DlqCommand : Runnable {
    override fun run() {
        println("Kafka DLQ Inspector CLI - use subcommands")
    }
}

@Component
@CommandLine.Command(name = "list-topics", description = ["List DLQ topics"])
class ListTopicsCommand(private val aggregationService: AggregationService) : Callable<Int> {
    override fun call(): Int {
        val topics = aggregationService.aggregate(SearchFilters(), 0).topics
        topics.forEach { println("${it.topic} partitions=${it.partitionCount} count=${it.messageCount}") }
        return 0
    }
}

@Component
@CommandLine.Command(name = "show", description = ["Show messages"])
class ShowCommand(private val searchService: SearchService) : Callable<Int> {
    @CommandLine.Option(names = ["--topic"], required = true)
    lateinit var topic: String

    @CommandLine.Option(names = ["--limit"], defaultValue = "20")
    var limit: Int = 20

    override fun call(): Int {
        val messages = searchService.search(SearchFilters(topics = listOf(topic)), limit)
        messages.forEach { println("${it.topic}:${it.partition}:${it.offset} ${String(it.value)}") }
        return 0
    }
}

@Component
@CommandLine.Command(name = "aggregate", description = ["Aggregate DLQ messages"])
class AggregateCommand(private val aggregationService: AggregationService) : Callable<Int> {
    override fun call(): Int {
        val result = aggregationService.aggregate(SearchFilters())
        result.topics.forEach { println("${it.topic} count=${it.messageCount} exceptions=${it.exceptionCounts}") }
        return 0
    }
}

@Component
@CommandLine.Command(name = "replay", description = ["Replay DLQ messages"])
class ReplayCommand(private val replayService: ReplayService) : Callable<Int> {
    @CommandLine.Option(names = ["--source"], required = true)
    lateinit var source: String

    @CommandLine.Option(names = ["--destination"], required = true)
    lateinit var destination: String

    @CommandLine.Option(names = ["--dry-run"], defaultValue = "true")
    var dryRun: Boolean = true

    override fun call(): Int {
        val request = ReplayRequest("default", source, destination, filters = SearchFilters(topics = listOf(source)), dryRun = dryRun)
        val responses = replayService.replay(request)
        responses.forEach { println(it) }
        return 0
    }
}

@Component
@CommandLine.Command(name = "export", description = ["Export DLQ messages to JSON"])
class ExportCommand(private val searchService: SearchService, private val exportService: ExportService) : Callable<Int> {
    @CommandLine.Option(names = ["--topic"], required = true)
    lateinit var topic: String

    @CommandLine.Option(names = ["--file"], defaultValue = "dlq.json")
    lateinit var fileName: String

    override fun call(): Int {
        val messages = searchService.search(SearchFilters(topics = listOf(topic)), 500)
        val file = File(fileName)
        exportService.exportJson(messages, file)
        println("Exported ${messages.size} messages to ${file.absolutePath}")
        return 0
    }
}

@Component
class CliRunner(
    private val dlqCommand: DlqCommand,
    @Value("\${cli.enabled:false}") private val enabled: Boolean,
    private val applicationArguments: org.springframework.boot.ApplicationArguments
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun runCli() {
        if (!enabled) return
        val args = applicationArguments.sourceArgs
        CommandLine(dlqCommand).execute(*args)
        logger.info("CLI execution completed")
    }
}

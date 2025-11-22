package com.dragos.kafkainspector.cli

import com.dragos.kafkainspector.model.ReplayRequest
import com.dragos.kafkainspector.model.SearchFilters
import com.dragos.kafkainspector.service.AggregationService
import com.dragos.kafkainspector.service.ExportService
import com.dragos.kafkainspector.service.ReplayService
import com.dragos.kafkainspector.service.SearchService
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationContext
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import picocli.CommandLine
import java.io.File
import java.util.concurrent.Callable
import kotlin.system.exitProcess

/**
 * Main CLI command for Kafka DLQ Inspector.
 * This serves as the root command with multiple subcommands for different operations.
 */
@Component
@CommandLine.Command(
    name = "dlq",
    description = ["Kafka DLQ Inspector CLI"],
    subcommands = [
        ListTopicsCommand::class,
        ShowCommand::class,
        AggregateCommand::class,
        ReplayCommand::class,
        ExportCommand::class,
    ],
)
class DlqCommand : Runnable {
    override fun run() {
        println("Kafka DLQ Inspector CLI - use subcommands")
    }
}

/**
 * CLI command to list all discovered DLQ topics.
 * Displays topic name, partition count, and message count for each topic.
 */
@Component
@CommandLine.Command(name = "list-topics", description = ["List DLQ topics"])
class ListTopicsCommand(
    private val topicDiscovery: com.dragos.kafkainspector.kafka.DlqTopicDiscovery,
) : Callable<Int> {
    override fun call(): Int {
        // Discover all DLQ topics matching the configured pattern
        val topics = topicDiscovery.discover()
        if (topics.isEmpty()) {
            println("No DLQ topics found")
        } else {
            topics.forEach { println("${it.name} partitions=${it.partitions} count=${it.messageCount}") }
        }
        System.out.flush()
        return 0
    }
}

/**
 * CLI command to display messages from a specific DLQ topic.
 * Shows messages with their topic, partition, offset, and payload.
 */
@Component
@CommandLine.Command(name = "show", description = ["Show messages"])
class ShowCommand(
    private val searchService: SearchService,
) : Callable<Int> {
    @CommandLine.Option(names = ["--topic"], required = true, description = ["Topic name to read messages from"])
    lateinit var topic: String

    @CommandLine.Option(names = ["--limit"], defaultValue = "20", description = ["Maximum number of messages to display"])
    var limit: Int = 20

    override fun call(): Int {
        // Search for messages in the specified topic with the given limit
        val messages = searchService.search(SearchFilters(topics = listOf(topic)), limit)
        messages.forEach { println("${it.topic}:${it.partition}:${it.offset} ${String(it.value)}") }
        System.out.flush()
        return 0
    }
}

/**
 * CLI command to compute and display aggregated statistics for DLQ messages.
 * Shows message counts and exception type statistics per topic.
 */
@Component
@CommandLine.Command(name = "aggregate", description = ["Aggregate DLQ messages"])
class AggregateCommand(
    private val aggregationService: AggregationService,
) : Callable<Int> {
    override fun call(): Int {
        // Compute aggregations across all DLQ topics
        val result = aggregationService.aggregate(SearchFilters())
        result.topics.forEach { println("${it.topic} count=${it.messageCount} exceptions=${it.exceptionCounts}") }
        System.out.flush()
        return 0
    }
}

/**
 * CLI command to replay messages from a source DLQ topic to a destination topic.
 * Supports dry-run mode to preview messages without actually replaying them.
 */
@Component
@CommandLine.Command(name = "replay", description = ["Replay DLQ messages"])
class ReplayCommand(
    private val replayService: ReplayService,
) : Callable<Int> {
    @CommandLine.Option(names = ["--source"], required = true, description = ["Source DLQ topic to replay from"])
    lateinit var source: String

    @CommandLine.Option(names = ["--destination"], required = true, description = ["Destination topic to replay messages to"])
    lateinit var destination: String

    @CommandLine.Option(
        names = ["--dry-run"],
        arity = "0",
        fallbackValue = "true",
        description = ["Perform a dry run without actually replaying messages"],
    )
    var dryRun: Boolean = false

    override fun call(): Int {
        // Create replay request with source, destination, and dry-run settings
        val request = ReplayRequest("default", source, destination, filters = SearchFilters(topics = listOf(source)), dryRun = dryRun)
        val responses = replayService.replay(request)
        responses.forEach { println(it) }
        System.out.flush()
        return 0
    }
}

/**
 * CLI command to export DLQ messages to a JSON file.
 * Useful for offline analysis or archival purposes.
 */
@Component
@CommandLine.Command(name = "export", description = ["Export DLQ messages to JSON"])
class ExportCommand(
    private val searchService: SearchService,
    private val exportService: ExportService,
    @Value("\${cli.default-limit:500}") private val defaultLimit: Int,
) : Callable<Int> {
    @CommandLine.Option(names = ["--topic"], required = true, description = ["Topic to export messages from"])
    lateinit var topic: String

    @CommandLine.Option(names = ["--file"], defaultValue = "dlq.json", description = ["Output file path"])
    lateinit var fileName: String

    override fun call(): Int {
        // Search for messages in the specified topic
        val messages = searchService.search(SearchFilters(topics = listOf(topic)), defaultLimit)
        val file = File(fileName)
        // Export messages to JSON format
        exportService.exportJson(messages, file)
        println("Exported ${messages.size} messages to ${file.absolutePath}")
        System.out.flush()
        return 0
    }
}

/**
 * Runner component that executes CLI commands when CLI mode is enabled.
 * This is initialized after the application is fully ready to ensure all beans
 * and Kafka connectivity are properly established.
 */
@Component
class CliRunner(
    private val dlqCommand: DlqCommand,
    private val picocliFactory: CommandLine.IFactory,
    @Value("\${cli.enabled:false}") private val enabled: Boolean,
    private val applicationArguments: org.springframework.boot.ApplicationArguments,
    private val applicationContext: ApplicationContext,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @EventListener(ApplicationReadyEvent::class)
    fun runCli() {
        if (!enabled) return

        // Get all command-line arguments passed to the application
        val args = applicationArguments.sourceArgs

        // Filter out the "dlq" command name if it's the first argument
        // This prevents "Unmatched argument" errors since DlqCommand is already the root command
        val filteredArgs =
            if (args.isNotEmpty() && args[0] == "dlq") {
                args.drop(1).toTypedArray()
            } else {
                args
            }

        val exitCode = CommandLine(dlqCommand, picocliFactory).execute(*filteredArgs)
        logger.info("CLI execution completed with exit code: $exitCode")

        // Exit the application after CLI execution
        exitProcess(exitCode)
    }
}

package com.dragos.kafkainspector

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaDlqInspectorApplication

fun main(args: Array<String>) {
    runApplication<com.dragos.kafkainspector.KafkaDlqInspectorApplication>(*args)
}

package ru.ac.uniyar.databasescourse

import ru.ac.uniyar.databasescourse.functions.QueriesOperations.closeDatabaseConnection
import ru.ac.uniyar.databasescourse.tasks.aggregationOperations
import ru.ac.uniyar.databasescourse.tasks.databaseSchemaDesign
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@OptIn(ExperimentalTime::class)
fun main() {
    println("The work has started")
    val duration = measureTimedValue {
        /*databaseSchemaDesign()*/
        aggregationOperations()
    }
    println("\nTime spent on execution: ${duration.duration}")
    closeDatabaseConnection()
}

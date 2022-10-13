package ru.ac.uniyar.databasescourse.tasks

import ru.ac.uniyar.databasescourse.config.REVIEWERS_TABLE_NAME
import ru.ac.uniyar.databasescourse.config.SOLUTIONS_TABLE_NAME
import ru.ac.uniyar.databasescourse.config.STUDENTS_TABLE_NAME
import ru.ac.uniyar.databasescourse.functions.QueriesOperations.processQueries
import java.sql.ResultSet
import java.sql.Statement

fun aggregationOperations() {
    val studentsList = mutableListOf<String>()
    val reviewersList = mutableListOf<String>()

    // 1
    requestAndReport(
        query = "SELECT score, $STUDENTS_TABLE_NAME.* FROM $STUDENTS_TABLE_NAME INNER JOIN $SOLUTIONS_TABLE_NAME " +
                "ON $SOLUTIONS_TABLE_NAME.studentID = $STUDENTS_TABLE_NAME.studentID " +
                "ORDER BY $SOLUTIONS_TABLE_NAME.score DESC;",
        targetList = studentsList,
        reportFunction = {
            it.apply {
                println(
                    "Студент, получивший наивысшую оценку:\n\t${first()}\n" +
                            "Студент, получивший наименьшую оценку:\n\t${last()}"
                )
            }
        }
    )

    // 2
    requestAndReport(
        query = "SELECT score, $REVIEWERS_TABLE_NAME.* FROM $SOLUTIONS_TABLE_NAME INNER JOIN $REVIEWERS_TABLE_NAME " +
                "ON $SOLUTIONS_TABLE_NAME.reviewerID = $REVIEWERS_TABLE_NAME.reviewerID " +
                "ORDER BY $SOLUTIONS_TABLE_NAME.score DESC;",
        targetList = reviewersList,
        reportFunction = {
            it.apply {
                println(
                    "Преподаватель, поставивший наивысшую оценку:\n\t${first()}\n" +
                            "Преподаватель, поставивший наименьшую оценку:\n\t${last()}"
                )
            }
        }
    )
}

private fun requestAndReport(
    query: String,
    targetList: MutableCollection<String>,
    reportFunction: (MutableCollection<String>) -> Unit
) {
    processQueries(query) { _: Statement, resultSet: ResultSet ->
        targetList.add(
            (1..resultSet.metaData.columnCount).joinToString(
                transform = {
                    with(resultSet.metaData.getColumnLabel(it)) {
                        "$this - ${resultSet.getString(it)}"
                    }
                }
            )
        )
    }
    reportFunction(targetList)
}
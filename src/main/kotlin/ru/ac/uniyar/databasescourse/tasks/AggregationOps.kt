package ru.ac.uniyar.databasescourse.tasks

import ru.ac.uniyar.databasescourse.config.REVIEWERS_TABLE_NAME
import ru.ac.uniyar.databasescourse.config.SOLUTIONS_TABLE_NAME
import ru.ac.uniyar.databasescourse.config.STUDENTS_TABLE_NAME
import ru.ac.uniyar.databasescourse.domain.task3.ReportNote
import ru.ac.uniyar.databasescourse.functions.QueriesOperations.processQueries
import java.sql.ResultSet
import java.sql.Statement

fun aggregationOperations() {
    val studentsList =
        mutableListOf<Map<String, String>>()
    val reviewersList =
        mutableListOf<Map<String, String>>()
    val reportList =
        mutableListOf<Triple<String, String, Collection<Double>>>()

    // 1
    taskOne(studentsList)
    // 2
    taskTwo(reviewersList)
    // 3
    taskThree(reportList)
}

private fun taskThree(reportList: MutableList<Triple<String, String, Collection<Double>>>) {
    println("Задача 3")
    requestAndReport(
        query = "select distinct reviewers.reviewerSurname, solutions.studentID, " +
                "group_concat(solutions.score) as 'scores'\n" +
                "from reviewers, solutions \n" +
                "where reviewers.reviewerID = solutions.reviewerID\n" +
                "group by reviewerSurname, studentID;",
        targetList = reportList,
        savingFunction = { _: Statement, resultSet: ResultSet ->
            @Suppress("UNCHECKED_CAST")
            reportList.add(
                with(resultSet) {
                    Triple(
                        getString("reviewerSurname"),
                        getString("studentID"),
                        getString("scores").split(",") as Collection<Double>
                    )
                }
            )
        },
        reportFunction = { list ->
            list.groupBy { triple -> triple.first }
                .map { entry ->
                    ReportNote(
                    reviewerSurname = entry.key,
                    studentAndMarks = entry.value.map { Pair(it.second, it.third) }
                ) }
                .forEach { reportNote ->
                    println("Преподаватель: ${reportNote.reviewerSurname}")
                    reportNote.studentAndMarks.forEach {
                        println("\tИдентификатор студента: ${it.first} <-> Оценки: ${it.second}")
                    }
                }
        }
    )
}

private fun taskTwo(reviewersList: MutableList<Map<String, String>>) {
    println("Задание 2")
    requestAndReport(
        query = "SELECT $REVIEWERS_TABLE_NAME.*, AVG($SOLUTIONS_TABLE_NAME.score) AS 'average' " +
                "FROM $SOLUTIONS_TABLE_NAME \n" +
                "INNER JOIN $REVIEWERS_TABLE_NAME " +
                "ON $REVIEWERS_TABLE_NAME.reviewerID = $SOLUTIONS_TABLE_NAME.reviewerID \n" +
                "GROUP BY reviewerID \n" +
                "ORDER BY AVG(score) DESC;",
        targetList = reviewersList,
        savingFunction = { _: Statement, resultSet: ResultSet ->
            reviewersList.add(
                (1..resultSet.metaData.columnCount).associate {
                    Pair(resultSet.metaData.getColumnLabel(it), resultSet.getString(it))
                }
            )
        },
        reportFunction = {
            it.apply {
                println(
                    with(first()) {
                        """Преподаватель, ставивший максимальные оценки: ${this["reviewerSurname"]}
                            |   Средняя оценка: ${this["average"]}
                            |""".trimMargin()
                    } +
                    with(last()) {
                        """Преподаватель, ставивший минимальные оценки: ${this["reviewerSurname"]}
                    |   Средняя оценка: ${this["average"]}
                    |""".trimMargin()
                    }
                )
            }
        }
    )
}

private fun taskOne(studentsList: MutableList<Map<String, String>>) {
    println("Задание 1")
    requestAndReport(
        query = "SELECT score, $STUDENTS_TABLE_NAME.* FROM $STUDENTS_TABLE_NAME " +
                "INNER JOIN $SOLUTIONS_TABLE_NAME " +
                "ON $SOLUTIONS_TABLE_NAME.studentID = $STUDENTS_TABLE_NAME.studentID " +
                "ORDER BY $SOLUTIONS_TABLE_NAME.score DESC;",
        targetList = studentsList,
        savingFunction = { _: Statement, resultSet: ResultSet ->
            studentsList.add(
                (1..resultSet.metaData.columnCount).associate {
                    Pair(resultSet.metaData.getColumnLabel(it), resultSet.getString(it))
                }
            )
        },
        reportFunction = {
            it.apply {
                println(
                    with(first()) {
                        """Студент, получивший максимальную оценку: ${this["studentName"]} ${this["studentSurname"]}
                            |   Оценка: ${this["score"]}
                            |""".trimMargin()
                    } +
                    with(last()) {
                        """Студент, получивший минимальную оценку: ${this["studentName"]} ${this["studentSurname"]}
                    |   Оценка: ${this["score"]}
                    |""".trimMargin()
                    }
                )
            }
        }
    )
}

private fun <T> requestAndReport(
    query: String,
    targetList: T,
    savingFunction: (Statement, ResultSet) -> Unit,
    reportFunction: (T) -> Unit
) {
    processQueries(query) { statement: Statement, resultSet: ResultSet ->
        savingFunction(statement, resultSet)
    }
    reportFunction(targetList)
}

private fun getStudentByID(id: Int): Student {
    return studentsList.firstOrNull { it.first.id == id }?.first ?: run {
        lateinit var studentInfo: Student

        processQueries(
            "SELECT studentName, studentSurname FROM students WHERE studentID=$id"
        ) { _: Statement, resultSet: ResultSet ->
            with(resultSet) {
                studentInfo = Student(
                    id,
                    getString("studentName"),
                    getString("studentSurname")
                )
            }
        }

        studentInfo
    }
}


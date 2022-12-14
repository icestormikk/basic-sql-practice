package ru.ac.uniyar.databasescourse.tasks

import ru.ac.uniyar.databasescourse.config.REVIEWERS_TABLE_NAME
import ru.ac.uniyar.databasescourse.config.SOLUTIONS_TABLE_NAME
import ru.ac.uniyar.databasescourse.config.STUDENTS_TABLE_NAME
import ru.ac.uniyar.databasescourse.domain.Reviewer
import ru.ac.uniyar.databasescourse.domain.Student
import ru.ac.uniyar.databasescourse.domain.task3.ReportNote
import ru.ac.uniyar.databasescourse.functions.QueriesOperations.processQueries
import java.sql.ResultSet
import java.sql.Statement

private val studentsList = mutableListOf<Pair<Student, Double>>()
fun aggregationOperations() {
    val reviewersList = mutableListOf<Pair<Reviewer, Double>>()
    val reportList =
        mutableListOf<Triple<String, Student, Collection<Double>>>()

    // 1
    taskOne()
    // 2
    taskTwo(reviewersList)
    // 3
    taskThree(reportList)
    // 4
    taskFour(reportList)
}

private fun taskFour(reportList: MutableList<Triple<String, Student, Collection<Double>>>) {
    println("\nЗадание 4\n[Студент] -> [Преподаватель, поставивший больше всего оценок]")

    reportList.groupBy { it.second }.forEach { (student, reviewerTriple) ->
        println("${student.name} ${student.surname} -> ${
            reviewerTriple.maxBy { reviewerAndMarks ->
                reviewerAndMarks.third.size
            }.first
        }")
    }
}

private fun taskThree(reportList: MutableList<Triple<String, Student, Collection<Double>>>) {
    println("Задание 3")
    requestAndReport(
        query = "SELECT DISTINCT $REVIEWERS_TABLE_NAME.reviewerSurname, $SOLUTIONS_TABLE_NAME.studentID, " +
                "group_concat($SOLUTIONS_TABLE_NAME.score) AS 'scores'\n" +
                "FROM $REVIEWERS_TABLE_NAME, $SOLUTIONS_TABLE_NAME \n" +
                "WHERE $REVIEWERS_TABLE_NAME.reviewerID = $SOLUTIONS_TABLE_NAME.reviewerID\n" +
                "GROUP BY reviewerSurname, studentID;",
        targetList = reportList,
        onSave = { _: Statement, resultSet: ResultSet ->
            @Suppress("UNCHECKED_CAST")
            reportList.add(
                with(resultSet) {
                    Triple(
                        getString("reviewerSurname"),
                        getStudentByID(getInt("studentID")),
                        getString("scores").split(",") as Collection<Double>
                    )
                }
            )
        },
        onReport = { list ->
            list.groupBy { triple -> triple.first }
                .map { entry ->
                    ReportNote(
                        reviewerSurname = entry.key,
                        studentAndMarks = entry.value.map { Pair(it.second, it.third) }
                    )
                }.forEach { reportNote ->
                    println("Преподаватель: ${reportNote.reviewerSurname}")
                    reportNote.studentAndMarks.forEach {
                        println("\t${it.first.name} ${it.first.surname} <-> " +
                                it.second.toString().replace(Regex("[\\[\\]']+"), "")
                        )
                    }
                }
        }
    )
}

private fun taskTwo(reviewersList: MutableList<Pair<Reviewer, Double>>) {
    println("Задание 2")
    requestAndReport(
        query = "SELECT $REVIEWERS_TABLE_NAME.*, AVG($SOLUTIONS_TABLE_NAME.score) AS 'average' " +
                "FROM $SOLUTIONS_TABLE_NAME \n" +
                "INNER JOIN $REVIEWERS_TABLE_NAME " +
                "ON $REVIEWERS_TABLE_NAME.reviewerID = $SOLUTIONS_TABLE_NAME.reviewerID \n" +
                "GROUP BY reviewerID \n" +
                "ORDER BY AVG(score) DESC;",
        targetList = reviewersList,
        onSave = { _: Statement, resultSet: ResultSet ->
            reviewersList.add(
                with(resultSet) {
                    Reviewer(
                        getInt("reviewerID"),
                        getString("reviewerSurname")
                    ) to getDouble("average")
                }
            )
        },
        onReport = {
            it.apply {
                println(
                    with(first()) {
                        """Преподаватель, ставивший максимальные оценки: ${first.surname}
                            |   Средняя оценка: $second
                            |""".trimMargin()
                    } +
                    with(last()) {
                        """Преподаватель, ставивший минимальные оценки: ${first.surname}
                    |   Средняя оценка: $second
                    |""".trimMargin()
                    }
                )
            }
        }
    )
}

private fun taskOne() {
    println("Задание 1")
    requestAndReport(
        query = "SELECT score, $STUDENTS_TABLE_NAME.* FROM $STUDENTS_TABLE_NAME " +
                "INNER JOIN $SOLUTIONS_TABLE_NAME " +
                "ON $SOLUTIONS_TABLE_NAME.studentID = $STUDENTS_TABLE_NAME.studentID " +
                "ORDER BY $SOLUTIONS_TABLE_NAME.score DESC;",
        targetList = studentsList,
        onSave = { _: Statement, resultSet: ResultSet ->
            studentsList.add(
                with(resultSet) {
                    Student(
                        getInt("studentID"),
                        getString("studentName"),
                        getString("studentSurname")
                    ) to getDouble("score")
                }
            )
        },
        onReport = {
            it.apply {
                println(
                    with(first()) {
                        """Студент, получивший максимальную оценку: ${first.name} ${first.surname}
                        |   Оценка: $second
                        |""".trimMargin()
                    } +
                    with(last()) {
                        """Студент, получивший минимальную оценку: ${first.name} ${first.surname}
                        |   Оценка: $second
                        |""".trimMargin()
                    }
                )
            }
        }
    )
}

/**
 * Sends a [query] request to the database and processes the request according
 * to the [onSave] function. After processing the request, calls the [onReport] function
 * to perform user output.
 * @param query request to be sent to the database for processing
 * @param targetList the collection to which the saving will take place
 * @param onSave a function that performs custom processing of the result
 * of executing a [query] request
 * @param onReport custom output of the result
 */
private inline fun <reified T> requestAndReport(
    query: String,
    targetList: T,
    crossinline onSave: (Statement, ResultSet) -> Unit,
    onReport: (T) -> Unit= {}
) {
    processQueries(query) { statement: Statement, resultSet: ResultSet ->
        onSave(statement, resultSet)
    }
    onReport(targetList)
}

/**
 * Finds the entity "student" by its unique identifier.
 * @param id unique identifier of the [Student] entity
 * @return [Student]
 */
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

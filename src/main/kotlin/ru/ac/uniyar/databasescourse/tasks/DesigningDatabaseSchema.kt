package ru.ac.uniyar.databasescourse.tasks

import de.siegmar.fastcsv.reader.NamedCsvRow
import ru.ac.uniyar.databasescourse.functions.FilesOperations.readCSVFilePerLine
import ru.ac.uniyar.databasescourse.functions.QueriesOperations.closeDatabaseConnection
import ru.ac.uniyar.databasescourse.functions.QueriesOperations.configurePreparedStatement
import ru.ac.uniyar.databasescourse.functions.QueriesOperations.insertToDatabase
import ru.ac.uniyar.databasescourse.functions.QueriesOperations.processQueries
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

private const val STUDENTS_TABLE_NAME = "students"
private const val REVIEWERS_TABLE_NAME = "reviewers"
private const val SOLUTIONS_TABLE_NAME = "solutions"

@SuppressWarnings("LongMethod")
fun databaseSchemaDesign() {
    val resultMap = mapOf(
        STUDENTS_TABLE_NAME to mutableSetOf<String>(),
        REVIEWERS_TABLE_NAME to mutableSetOf(),
        SOLUTIONS_TABLE_NAME to mutableSetOf()
    )

    processQueries(
        listOf(
            // 1
            "CREATE TABLE IF NOT EXISTS $STUDENTS_TABLE_NAME (\n" +
                    "\tstudentID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,\n" +
                    "    studentName VARCHAR(255) NOT NULL,\n" +
                    "    studentSurname VARCHAR(255) NOT NULL\n" +
                    ");\n",
            "CREATE TABLE IF NOT EXISTS $REVIEWERS_TABLE_NAME (\n" +
                    "\treviewerID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,\n" +
                    "    reviewerSurname VARCHAR(255) NOT NULL,\n" +
                    "    reviewerDepartment VARCHAR(255) NOT NULL\n" +
                    ");",
            "CREATE TABLE IF NOT EXISTS $SOLUTIONS_TABLE_NAME (\n" +
                    "\tsolutionID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,\n" +
                    "    studentID INT NOT NULL,\n" +
                    "    reviewerID INT NOT NULL,\n" +
                    "    score FLOAT NOT NULL DEFAULT 0.0,\n" +
                    "    hasPassed BOOLEAN,\n" +
                    "    FOREIGN KEY (studentID) REFERENCES students (studentID) ON DELETE CASCADE" +
                    " ON UPDATE CASCADE,\n" +
                    "    FOREIGN KEY (reviewerID) REFERENCES reviewers (reviewerID) ON DELETE CASCADE" +
                    " ON UPDATE CASCADE\n" +
                    ")"
        )
    )

    convertCSVLinesToMySQLValues("data.csv") {
        // 2
        with(resultMap) {
            get(STUDENTS_TABLE_NAME)!!.add(
                it.convertToSQLValueByFields(
                    setOf("studentID", "studentName", "studentSurname")
                )
            )
            get(REVIEWERS_TABLE_NAME)!!.add(
                it.convertToSQLValueByFields(
                    setOf("reviewerID", "reviewerSurname", "reviewerDepartment")
                )
            )
            get(SOLUTIONS_TABLE_NAME)!!.add(
                it.convertToSQLValueByFields(
                    setOf("solutionID", "studentID", "reviewerID", "score", "hasPassed")
                ) { title ->
                    if (title == "hasPassed")
                        if (it.getField(title) == "Yes") "1" else "0"
                    else it.getField(title)
                }
            )
        }
    }

    resultMap.forEach {
        processQueries(
            listOf("INSERT INTO ${it.key} VALUES ${it.value.joinToString(separator = ",")}")
        )
    }

    report()

    resultMap.keys.reversed().forEach {
        processQueries(listOf("DROP TABLE $it;"))
    }
}

private fun report() {
    println("Work completed: added ")
    processQueries(
        listOf(
            "SELECT COUNT(*) as 'students' FROM $STUDENTS_TABLE_NAME",
            "SELECT COUNT(*) as 'reviewers' FROM $REVIEWERS_TABLE_NAME",
            "SELECT COUNT(*) as 'solutions' FROM $SOLUTIONS_TABLE_NAME"
        )
    ) {
        println("\t${it.getString(1)} ${it.metaData.getColumnLabel(1)}")
    }
}

private fun NamedCsvRow.convertToSQLValueByFields(
    fieldNamesList: Set<String> = this.fields.keys,
    mapFunction: (String) -> String = { this.getField(it) },
) = fieldNamesList.map { fieldName ->
        mapFunction(fieldName)
    }.joinToString(
        prefix = "(",
        postfix = ")",
        separator = ", ",
        transform = { String.format(Locale.ENGLISH, "'$it'") }
    )

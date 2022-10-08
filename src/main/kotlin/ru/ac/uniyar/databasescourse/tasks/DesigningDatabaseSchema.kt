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
private const val DEPARTMENT_TABLE_NAME = "departments"

@SuppressWarnings("LongMethod")
fun databaseSchemaDesign() {
    val tableNameAndValues = mapOf(
        STUDENTS_TABLE_NAME to mutableSetOf<String>(),
        DEPARTMENT_TABLE_NAME to mutableSetOf(),
        REVIEWERS_TABLE_NAME to mutableSetOf(),
        SOLUTIONS_TABLE_NAME to mutableSetOf()
    )

    processQueries(
        "CREATE TABLE IF NOT EXISTS $STUDENTS_TABLE_NAME (\n" +
                "\tstudentID INT PRIMARY KEY NOT NULL,\n" +
                "    studentName VARCHAR(255) NOT NULL,\n" +
                "    studentSurname VARCHAR(255) NOT NULL\n" +
                ");\n",
        "CREATE TABLE IF NOT EXISTS $DEPARTMENT_TABLE_NAME (\n" +
                "    id INT PRIMARY KEY NOT NULL AUTO_INCREMENT," +
                "    reviewerDepartment VARCHAR(255) NOT NULL UNIQUE" +
                ");",
        "CREATE TABLE IF NOT EXISTS $REVIEWERS_TABLE_NAME (\n" +
                "\treviewerID INT PRIMARY KEY NOT NULL,\n" +
                "    reviewerSurname VARCHAR(255) NOT NULL,\n" +
                "    departmentID INT NOT NULL,\n" +
                "    FOREIGN KEY (departmentID) REFERENCES $DEPARTMENT_TABLE_NAME (id) " +
                "ON DELETE RESTRICT ON UPDATE CASCADE\n" +
                ");",
        "CREATE TABLE IF NOT EXISTS $SOLUTIONS_TABLE_NAME (\n" +
                "\tsolutionID INT PRIMARY KEY NOT NULL,\n" +
                "    studentID INT NOT NULL,\n" +
                "    reviewerID INT NOT NULL,\n" +
                "    score FLOAT NOT NULL DEFAULT 0.0,\n" +
                "    hasPassed BOOLEAN,\n" +
                "    FOREIGN KEY (studentID) REFERENCES $STUDENTS_TABLE_NAME (studentID) ON DELETE RESTRICT" +
                " ON UPDATE CASCADE,\n" +
                "    FOREIGN KEY (reviewerID) REFERENCES $REVIEWERS_TABLE_NAME (reviewerID) ON DELETE RESTRICT" +
                " ON UPDATE CASCADE\n" +
                ")"
    )

    readCSVFilePerLine("data.csv") { csvRow ->
        val tableNameToColumnNames = mapOf(
            STUDENTS_TABLE_NAME to setOf("studentID", "studentName", "studentSurname"),
            DEPARTMENT_TABLE_NAME to setOf("reviewerDepartment"),
            REVIEWERS_TABLE_NAME to setOf("reviewerID", "reviewerSurname", "departmentID"),
            SOLUTIONS_TABLE_NAME to setOf("solutionID", "studentID", "reviewerID", "score", "hasPassed")
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

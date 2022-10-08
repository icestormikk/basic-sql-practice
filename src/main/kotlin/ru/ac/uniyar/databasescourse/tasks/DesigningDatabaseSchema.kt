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
        
        tableNameToColumnNames.apply {
            entries.take(2).forEach { currentTableInfo ->
                csvRow.convertRowFieldValue(currentTableInfo.value).apply {
                    addValuesToTableIfNotExist(
                        informationMap = tableNameAndValues,
                        currentTable = currentTableInfo,
                        values = this
                    )
                }
            }

            entries.first { it.key == REVIEWERS_TABLE_NAME }.also { currentTableInfo ->
                csvRow.convertRowFieldValue(currentTableInfo.value) { columnTitle ->
                    if (columnTitle == "departmentID")
                        "${getDepartmentIdByName(csvRow.getField("reviewerDepartment"))}"
                    else csvRow.getField(columnTitle)
                }.apply {
                    addValuesToTableIfNotExist(
                        informationMap = tableNameAndValues,
                        currentTable = currentTableInfo,
                        values = this
                    )
                }
            }

            entries.last().also { currentTableInfo ->
                csvRow.convertRowFieldValue(currentTableInfo.value) { columnTitle ->
                    if (columnTitle == "hasPassed")
                        if (csvRow.getField(columnTitle).lowercase() == "yes") "1" else "0"
                    else csvRow.getField(columnTitle)
                }.apply {
                    addValuesToTableIfNotExist(
                        informationMap = tableNameAndValues,
                        currentTable = currentTableInfo,
                        values = this
                    )
                }
            }
        }
    }

    tableNameAndValues.keys.reversed().forEach {
        processQueries("DROP TABLE $it;")
    }
    closeDatabaseConnection()
}

private fun addValuesToTableIfNotExist(
    informationMap: Map<String, MutableSet<String>>,
    currentTable: Map.Entry<String, Set<String>>,
    values: Collection<String>,
) {
    val doesNotContainsYet = informationMap[currentTable.key]!!.add(values.joinToString(separator = ","))
    if (doesNotContainsYet) {
        currentTable.value.toPreparedStatement(currentTable.key)
            .fill(values)
            .insertToDatabase()
    }
}

private fun Collection<String>.toPreparedStatement(tableTitle: String): PreparedStatement =
    configurePreparedStatement(tableTitle, this)

private fun PreparedStatement.fill(values: Collection<Any>): PreparedStatement {
    values.forEachIndexed { index: Int, value: Any ->
        this.setString(index + 1, "$value")
    }
    return this
}

private fun NamedCsvRow.convertRowFieldValue(
    fieldNamesList: Set<String> = this.fields.keys,
    mapFunction: (String) -> String = { this.getField(it) }
): List<String> = fieldNamesList.map { fieldName ->
        mapFunction(fieldName)
    }

private fun getDepartmentIdByName(departmentName: String): Int {
    var departmentId = 1

    processQueries(
        "SELECT id FROM $DEPARTMENT_TABLE_NAME WHERE reviewerDepartment='$departmentName'"
    ) { _: Statement, resultSet: ResultSet ->
        if (resultSet.metaData.columnCount > 0)
            departmentId = resultSet.getInt(1)
    }

    return departmentId
}

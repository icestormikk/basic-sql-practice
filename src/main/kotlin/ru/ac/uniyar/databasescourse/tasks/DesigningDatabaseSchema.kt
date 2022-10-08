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

/**
 * Based on the information from two objects of the Map type, it sends a request to change a certain table
 * in the database using a PreparedStatement object.
 * @param informationMap map with information about the names of tables and the values(in SQL format)
 * that should be passed in them
 * @param currentTable a pair of values that contain information about the name of the table and its
 * corresponding columns in the csv file
 * @param values collection of values to be filled in with the PreparedStatement object
 */
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

/**
 * Converts a collection of strings to an object of type PreparedStatement
 * with the INSERT command inside.
 * @receiver Collection<String>
 * @param tableTitle The name of the table in the database for which the query will be built
 * @return PreparedStatement
 */
private fun Collection<String>.toPreparedStatement(tableTitle: String): PreparedStatement =
    configurePreparedStatement(tableTitle, this)

/**
 * Fills the passed object of the type PreparedStatement with the values specified in the [values] parameter.
 * @param values Collection of objects that will be placed inside the PreparedStatement.
 * @return PreparedStatement
 */
private fun PreparedStatement.fill(values: Collection<Any>): PreparedStatement {
    values.forEachIndexed { index: Int, value: Any ->
        this.setString(index + 1, "$value")
    }
    return this
}

/**
 * Uses selected headers from a csv file to compose a set
 * of header values. It is possible to change the conversion logic
 * to a function specified by the user.
 * @receiver NamedCsvRow
 * @param selectedHeaders set of selected headers
 * @param mapFunction conversion function (by default: gets the value from the field without conversions)
 * @return List of values obtained from fields with corresponding headers
 */
private fun NamedCsvRow.convertRowFieldValue(
    selectedHeaders: Set<String> = this.fields.keys,
    mapFunction: (String) -> String = { this.getField(it) }
): List<String> = selectedHeaders.map { fieldName ->
        mapFunction(fieldName)
    }

/**
 * Obtains the unique identifier of the department by its name by executing a sql query
 * to the database.
 * @param departmentName name of the department you are looking for
 * @return unique identifier of the department
 */
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

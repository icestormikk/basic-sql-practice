package ru.ac.uniyar.databasescourse.functions

import ru.ac.uniyar.databasescourse.config.DATABASE_NAME
import ru.ac.uniyar.databasescourse.config.URL
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.Locale

@Throws(SQLException::class)
private fun createConnection(): Connection {
    return DriverManager.getConnection(URL)
}

/**
 * Creates a connection to the database and sends the queries specified in the [queries] parameter to it.
 * Supports formatted output for all queries in the list.
 * @param queries List<String>
 * @param formatString String
 */
@SuppressWarnings("NestedBlockDepth")
fun processQueries(queries: List<String>, formatString: String = "%s ") {
    val fullQueriesList = listOf("USE $DATABASE_NAME").plus(queries)
    try {
        createConnection().use { conn ->
            try {
                conn.createStatement().use { smt ->
                    try {
                        fullQueriesList.forEach {
                            smt.executeQuery(it).use { rs ->
                                while (rs.next()) {
                                    for (colIndex in 1..rs.metaData.columnCount)
                                        print(String.format(
                                            Locale.ENGLISH,
                                            formatString,
                                            rs.getString(colIndex)
                                        ))
                                    println()
                                }
                            }
                        }
                    } catch (ex: SQLException) {
                        System.out.printf("Statement execution error: %s\n", ex)
                    }
                }
            } catch (ex: SQLException) {
                System.out.printf("Can't create statement: %s\n", ex)
            }
        }
    } catch (ex: SQLException) {
        System.out.printf("Can't create connection: %s\n", ex)
    }
}
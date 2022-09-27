package ru.ac.uniyar.databasescourse.functions

import ru.ac.uniyar.databasescourse.config.DATABASE_NAME
import ru.ac.uniyar.databasescourse.config.URL
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*

@Throws(SQLException::class)
private fun createConnection(): Connection {
    return DriverManager.getConnection(URL)
}

object QueriesOperations {
    /**
     * Creates a connection to the database and sends the queries specified in the [queries] parameter to it.
     * The callback parameter allows you to use the result of executing the request for your own purposes
     * (by default, the function displays the result on the screen).
     * @param queries List<String>
     * @param callback Function1<String, Unit>
     */
    @SuppressWarnings("NestedBlockDepth")
    fun processQueries(
        queries: List<String>,
        callback: (ResultSet) -> Unit = defaultCallback(),
    ) {
        val fullQueriesList = listOf("USE $DATABASE_NAME").plus(queries)

        try {
            createConnection().use { conn ->
                try {
                    conn.createStatement().use { smt ->
                        try {
                            fullQueriesList.forEach {
                                smt.executeQuery(it).use { rs ->
                                    while (rs.next())
                                        callback(rs)
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

    /**
     * Default callback for the processQueries function
     * @return (ResultSet) -> Unit
     */
    private fun defaultCallback(): (ResultSet) -> Unit = {
        for (colIndex in 1..it.metaData.columnCount)
            print(String.format(
                Locale.ENGLISH,
                "\t%s ",
                it.getString(colIndex)
            ))
        println()
    }
}

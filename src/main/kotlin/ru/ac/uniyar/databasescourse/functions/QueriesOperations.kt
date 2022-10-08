package ru.ac.uniyar.databasescourse.functions

import ru.ac.uniyar.databasescourse.config.DATABASE_NAME
import ru.ac.uniyar.databasescourse.config.URL
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.*

@Throws(SQLException::class)
private fun createConnection(): Connection {
    return DriverManager.getConnection(URL)
}

object QueriesOperations {
    private var databaseConnection: Connection = createConnection().also { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery("USE $DATABASE_NAME")
        }
    }

    fun closeDatabaseConnection() =
        databaseConnection.close()
    /**
     * Creates a connection to the database and sends the queries specified in the [queries] parameter to it.
     * The [callback] parameter allows you to use the result of executing the request for your own purposes
     * (by default, the function displays the result on the screen).
     * @param queries list of queries in SQL format
     * @param callback function processing the response received from the database
     */
    @SuppressWarnings("NestedBlockDepth")
    fun processQueries(
        vararg queries: String,
        callback: (Statement, ResultSet) -> Unit = defaultCallback(),
    ) {
        try {
            databaseConnection.also { conn ->
                try {
                    conn.createStatement().use { smt ->
                        try {
                            queries.forEach {
                                smt.executeQuery(it).use { rs ->
                                    while (rs.next())
                                        callback(smt, rs)
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
     * Generates an object of type PreparedStatement with the INSERT command and the specified parameters.
     * @param tableTitle the name of the table in which the change will be made
     * @param headers headers for which stubs will be created
     * @return PreparedStatement
     */
    fun configurePreparedStatement(
        tableTitle: String,
        headers: Collection<String>,
    ): PreparedStatement =
        databaseConnection.prepareStatement(
            "INSERT INTO $tableTitle (${ headers.toSet().joinToString(separator = ",") }) VALUES " +
                    "(${ CharArray(headers.size) {'?'}.joinToString(separator = ",") });"
        )

    /**
     * Sends a request with the INSERT command to the database.
     * @receiver PreparedStatement
     */
    @SuppressWarnings("SwallowedException")
    fun PreparedStatement.insertToDatabase() {
        try {
            executeUpdate()
        } catch (ex: SQLException) {
            System.out.printf("Statement execution error: %s\n", ex)
        }
    }

    /**
     * Default callback for the processQueries function.
     * @return (Statement, ResultSet) -> Unit
     */
    private fun defaultCallback(): (Statement, ResultSet) -> Unit =
        { _: Statement, resultSet: ResultSet ->
            with (resultSet.metaData.columnCount) {
                if (this > 0) {
                    for (colIndex in 1..this) {
                        print(
                            String.format(Locale.ENGLISH, "\t%s ", resultSet.getString(colIndex))
                        )
                    }
                    println()
                }
            }
        }
}

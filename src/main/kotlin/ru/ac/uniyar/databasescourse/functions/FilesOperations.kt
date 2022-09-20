package ru.ac.uniyar.databasescourse.functions

import ru.ac.uniyar.databasescourse.config.FILE_PATH
import java.io.File
import java.io.FileNotFoundException
import java.util.Locale

@SuppressWarnings("MagicNumber")
object FilesOperations {
    /**
     * Converts strings from a .csv file to values for a mysql database.
     * @param pathname String
     * @return List<String>
     */
    @JvmStatic
    fun csvLinesToSqlValues(pathname: String): List<String> {
        val resultList = mutableListOf<String>()

        try {
            File(pathname).bufferedReader().forEachLine { line ->
                val arguments = line.split(Regex(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*\$)"))
                val oneValue = listOf(
                    arguments[0], arguments[1], arguments[2],
                    arguments[3], arguments[4], arguments[5],
                    if (arguments[6] == "T") 1 else 0
                ).map { String.format(Locale.ENGLISH, "'%s'", it) }

                resultList.add(
                    oneValue.joinToString(
                        separator = ",",
                        prefix = "(",
                        postfix = ")",
                        transform = { value -> value.replace("\"", "") }
                    )
                )
            }
        } catch (_: FileNotFoundException) {
            println("The specified file was not found: $FILE_PATH")
        }

        return resultList
    }
}

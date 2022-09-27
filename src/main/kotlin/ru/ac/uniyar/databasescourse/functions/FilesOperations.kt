package ru.ac.uniyar.databasescourse.functions

import de.siegmar.fastcsv.reader.NamedCsvReader
import ru.ac.uniyar.databasescourse.config.FILE_PATH
import java.util.Locale
import kotlin.io.path.Path

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
            getNamedCsvReader(pathname).forEach { line ->
                with(resultList) {
                    add(
                        line.fields.map { entry ->
                            if (entry.key == "has_pass")
                                if (entry.value == "T") "1" else "0"
                            else String.format(Locale.ENGLISH, "'${entry.value}'")
                        }.joinToString(
                            separator = ",",
                            prefix = "(",
                            postfix = ")"
                        )
                    )
                }
            }
        } catch (_: NullPointerException) {
            println("The specified file was not found: $FILE_PATH")
        }

        return resultList
    }

    private fun getNamedCsvReader(pathname: String): NamedCsvReader =
        NamedCsvReader.builder().build(Path(pathname))
}

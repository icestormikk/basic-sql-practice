package ru.ac.uniyar.databasescourse.functions

import de.siegmar.fastcsv.reader.NamedCsvReader
import de.siegmar.fastcsv.reader.NamedCsvRow
import kotlin.io.path.Path

@SuppressWarnings("MagicNumber")
object FilesOperations {
    /**
     * Converts strings from the *.csv file located on the [pathname] path to values suitable
     * for use in the MySQL database, in accordance with the user-defined function.
     * @param pathname String
     * @param conversionFunction Function1<NamedCsvRow, Unit>
     */
    @JvmStatic
    fun convertCSVLinesToMySQLValues(
        pathname: String,
        conversionFunction: (NamedCsvRow) -> Unit = {},
    ) {
        try {
            getNamedCsvReader(pathname).forEach { line ->
                conversionFunction(line)
            }
        } catch (_: NoSuchFileException) {
            println("The specified file was not found: $pathname")
        } catch (ex: NoSuchElementException) {
            println(ex.localizedMessage)
        }
    }

    private fun getNamedCsvReader(pathname: String): NamedCsvReader =
        NamedCsvReader.builder().build(Path(pathname))
}

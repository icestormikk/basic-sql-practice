package ru.ac.uniyar.databasescourse.tasks

import ru.ac.uniyar.databasescourse.functions.FilesOperations
import ru.ac.uniyar.databasescourse.functions.processQueries

fun basicOperationsWithDatabase() {
    val values = FilesOperations.csvLinesToSqlValues("solutions.csv")

    processQueries(
        listOf(
            "SET SQL_SAFE_UPDATES = 0",
            // 1
            "CREATE TABLE IF NOT EXISTS solutions (\n" +
                    "\tname VARCHAR(255) NOT NULL DEFAULT '',\n" +
                    "    surname VARCHAR(255) NOT NULL DEFAULT '',\n" +
                    "    card INT PRIMARY KEY NOT NULL,\n" +
                    "    answer TEXT,\n" +
                    "    score DOUBLE NOT NULL DEFAULT 0.0,\n" +
                    "    review TEXT,\n" +
                    "    has_pass BOOLEAN DEFAULT false\n" +
                    ")",
            // 2
            "INSERT INTO solutions VALUES ${values.joinToString(",")}",
        )
    )

    println("Вывод для пункта 3:")
    processQueries(
        listOf(
            // 3
            "SELECT solutions.surname AS \"Student's surname\", solutions.answer " +
                    "AS \"Student's review\" FROM solutions WHERE has_pass = 0;",
            // 4
            "INSERT INTO solutions (name,surname,card,answer) VALUES ('Алексей', 'Ефимов', '761805'," +
                    "'Пушкин, конечно, герой, но зачем стулья ломать?')",
            // 5
            "UPDATE solutions SET score = 4.1, review = 'Стулья ломать, действительно, незачем', has_pass = 1 " +
                    "WHERE card = 761805",
        ), "\t%s "
    )

    println("Вывод для пункта 6:")
    processQueries(
        listOf(
            // 6
            "SELECT solutions.surname AS \"Student's surname\", solutions.card AS \"Card Id\" FROM solutions " +
                    "WHERE score > 2 AND has_pass = 0",
            "UPDATE solutions SET has_pass = 1 WHERE score > 2",
            // 7
            "DELETE FROM solutions WHERE has_pass = 0",
            "DROP TABLE solutions"
        ), "\t%s "
    )
}

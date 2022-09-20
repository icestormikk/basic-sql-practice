package ru.ac.uniyar.databasescourse

import ru.ac.uniyar.databasescourse.config.DATABASE_NAME
import ru.ac.uniyar.databasescourse.functions.FilesOperations
import ru.ac.uniyar.databasescourse.functions.sendQueries

fun main(args: Array<String>) {
    println("The work has started")
    FilesOperations.csvLinesToSqlValues("solutions.csv")

    /*sendQueries(listOf(
        "USE $DATABASE_NAME",
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
    ))

    println("Вывод для пункта 3:")
    sendQueries(listOf(
        "USE $DATABASE_NAME",
        // 3
        "SELECT solutions.surname AS \"Student's surname\", solutions.answer " +
                "AS \"Student's review\" FROM solutions WHERE has_score = 0;",
        // 4
        "INSERT INTO solutions (name,surname,card,answer) VALUES ('Алексей', 'Ефимов', '761805'," +
                "'Пушкин, конечно, герой, но зачем стулья ломать?')",
        // 5
        "UPDATE solutions SET score = 4.1, review = 'Стулья ломать, действительно, незачем', has_pass = 1 " +
                "WHERE card = 761805",
    ))

    println("Вывод для пункта 6:")
    sendQueries(listOf(
        "USE $DATABASE_NAME",
        // 6
        "SELECT solutions.surname AS \"Student's surname\", solutions.card AS \"Card Id\" FROM solutions " +
                "WHERE score > 2 AND has_pass = 0",
        "UPDATE solutions SET has_pass = 1 WHERE score > 2",
        // 7
        "DELETE FROM solutions WHERE has_pass = 0",
        "DROP TABLE solutions"
    ))*/
}
package ru.ac.uniyar.databasescourse

import ru.ac.uniyar.databasescourse.functions.FilesOperations
import ru.ac.uniyar.databasescourse.functions.sendQueries

fun main(args: Array<String>) {
    println("The work has started")
    val values = FilesOperations.csvLinesToSqlValues("solutions.csv")

    sendQueries(listOf(
        "use userdb",
        "set SQL_SAFE_UPDATES = 0",
        // 1
        "create table if not exists solutions (\n" +
                "\tname varchar(255) not null default '',\n" +
                "    surname varchar(255) not null default '',\n" +
                "    card int primary key not null,\n" +
                "    answer text,\n" +
                "    score double not null default 0.0,\n" +
                "    review text,\n" +
                "    has_pass boolean default false\n" +
                ")",
        // 2
        "insert into solutions values ${values.joinToString(",")}",
    ))

    println("Вывод для пункта 3:")
    sendQueries(listOf(
        "use userdb",
        // 3
        "select solutions.surname as \"Student's surname\", solutions.answer " +
                "as \"Student's review\" from solutions where score = 0;",
        // 4
        "insert into solutions (name,surname,card,answer) values ('Алексей', 'Ефимов', '761805'," +
                "'Пушкин, конечно, герой, но зачем стулья ломать?')",
        // 5
        "update solutions set score = 4.1, review = 'Стулья ломать, действительно, незачем', has_pass = 1 " +
                "where card = 761805",
    ))

    println("Вывод для пункта 6:")
    sendQueries(listOf(
        "use userdb",
        // 6
        "select solutions.surname as \"Student's surname\", solutions.card as \"Card Id\" from solutions " +
                "where score > 2 and has_pass = 0",
        "update solutions set has_pass = 1 where score > 2",
        // 7
        "delete from solutions where has_pass = 0"
    ))
}
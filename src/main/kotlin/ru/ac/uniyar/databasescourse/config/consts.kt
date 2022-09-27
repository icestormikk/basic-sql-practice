package ru.ac.uniyar.databasescourse.config

import java.util.Locale

val HOST: String = System.getenv("MARIADB_HOST") ?: "DEFAULT_HOST"
val USER: String = System.getenv("MARIADB_USER") ?: "DEFAULT_USER"
val PASSWORD: String = System.getenv("MARIADB_PASSWORD") ?: "DEFAULT_PASSWORD"
const val DATABASE_NAME = "PavelJigalovDB"

val URL = String.format(
    Locale.ENGLISH,
    "jdbc:mariadb://%s:3306/?user=%s&password=%s&allowPublicKeyRetrieval=true",
    HOST, USER, PASSWORD
)

package ru.ac.uniyar.databasescourse.config

import java.util.Locale

val HOST: String = System.getenv("MARIADB_HOST") ?: "HOST"
val USER: String = System.getenv("MARIADB_USER") ?: "USER"
val PASSWORD: String = System.getenv("MARIADB_PASSWORD") ?: "PASSWORD"

val URL = String.format(
    Locale.ENGLISH,
    "jdbc:mariadb://%s:3306/?user=%s&password=%s&allowPublicKeyRetrieval=true",
    HOST, USER, PASSWORD
)

const val DATABASE_NAME = "userdb"
const val FILE_PATH = "solutions.csv"
package ru.ac.uniyar.databasescourse.domain.task3

data class ReportNote(
    val reviewerSurname: String,
    val studentAndMarks: List<Pair<String, Collection<Double>>>
)

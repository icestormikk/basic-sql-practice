package ru.ac.uniyar.databasescourse.domain.task3

import ru.ac.uniyar.databasescourse.domain.Student

data class ReportNote(
    val reviewerSurname: String,
    val studentAndMarks: List<Pair<Student, Collection<Double>>>
)

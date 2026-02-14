package xyz.robinjoon.api.model

import java.time.Instant

data class JobSummary(
    val jobName: String,
    val namespace: String,
    val status: JobStatus,
    val flinkImage: String?,
    val createdAt: Instant?,
    val parallelism: Int?
)

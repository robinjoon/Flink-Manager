package xyz.robinjoon.api.model

import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern

data class CreateJobRequest(
    @field:NotBlank(message = "jobName is required")
    @field:Pattern(
        regexp = "^[a-z0-9]([a-z0-9\\-]{0,61}[a-z0-9])?\$",
        message = "jobName must be a valid Kubernetes name (lowercase, hyphens, max 63 chars)"
    )
    val jobName: String,

    @field:NotBlank(message = "pipelineYaml is required")
    val pipelineYaml: String,

    @field:NotBlank(message = "flinkImage is required")
    val flinkImage: String,

    @field:Valid
    val resources: ResourceSpec? = null,

    @field:Min(value = 1, message = "parallelism must be at least 1")
    val parallelism: Int? = null,

    @field:Valid
    val flink: FlinkSpec? = null,

    val namespace: String? = null
)

data class ResourceSpec(
    @field:Valid
    val jobManager: ManagerResource? = null,

    @field:Valid
    val taskManager: TaskManagerResource? = null
)

data class ManagerResource(
    @field:Min(value = 1, message = "cpu must be at least 1")
    val cpu: Int? = null,
    val memory: String? = null
)

data class TaskManagerResource(
    @field:Min(value = 1, message = "cpu must be at least 1")
    val cpu: Int? = null,
    val memory: String? = null,
    @field:Min(value = 1, message = "replicas must be at least 1")
    val replicas: Int? = null
)

data class FlinkSpec(
    val version: String? = null,
    val serviceAccount: String? = null,
    val extraConfig: Map<String, String>? = null
)

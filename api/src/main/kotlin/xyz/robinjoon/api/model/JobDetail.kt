package xyz.robinjoon.api.model

import java.time.Instant

data class JobDetail(
    val jobName: String,
    val namespace: String,
    val status: JobStatus,
    val flinkImage: String?,
    val createdAt: Instant?,
    val pipelineYaml: String?,
    val resources: ResourceInfo?,
    val parallelism: Int?,
    val kubernetes: KubernetesStatus?,
    val flinkUiUrl: String?,
    val events: List<EventInfo>?
)

data class ResourceInfo(
    val jobManager: ManagerResourceInfo?,
    val taskManager: TaskManagerResourceInfo?
)

data class ManagerResourceInfo(
    val cpu: Any?,
    val memory: String?
)

data class TaskManagerResourceInfo(
    val cpu: Any?,
    val memory: String?,
    val replicas: Int?
)

data class KubernetesStatus(
    val lifecycleState: String?,
    val jobManagerDeploymentStatus: String?,
    val jobStatus: FlinkJobStatus?,
    val error: String?
)

data class FlinkJobStatus(
    val state: String?,
    val jobId: String?,
    val startTime: String?
)

data class EventInfo(
    val type: String?,
    val reason: String?,
    val message: String?,
    val timestamp: Instant?
)

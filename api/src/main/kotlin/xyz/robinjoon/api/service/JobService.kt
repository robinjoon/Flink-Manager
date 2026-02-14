package xyz.robinjoon.api.service

import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.api.model.events.v1.Event
import io.fabric8.kubernetes.client.KubernetesClientException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import xyz.robinjoon.api.config.KubernetesProperties
import xyz.robinjoon.api.exception.JobAlreadyExistsException
import xyz.robinjoon.api.exception.JobNotFoundException
import xyz.robinjoon.api.exception.KubernetesOperationException
import xyz.robinjoon.api.kubernetes.FlinkDeploymentClient
import xyz.robinjoon.api.kubernetes.ResourceBuilder
import xyz.robinjoon.api.kubernetes.nestedInt
import xyz.robinjoon.api.kubernetes.nestedMap
import xyz.robinjoon.api.kubernetes.nestedString
import xyz.robinjoon.api.model.*
import java.net.HttpURLConnection
import java.time.Instant

@Service
class JobService(
    private val flinkDeploymentClient: FlinkDeploymentClient,
    private val resourceBuilder: ResourceBuilder,
    private val statusMapper: StatusMapper,
    private val k8sProperties: KubernetesProperties
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun createJob(request: CreateJobRequest): JobDetail {
        val configMap = resourceBuilder.buildConfigMap(request)
        val flinkDeployment = resourceBuilder.buildFlinkDeployment(request)

        // Step 1: Create ConfigMap
        val createdConfigMap = try {
            flinkDeploymentClient.createConfigMap(configMap)
        } catch (e: KubernetesClientException) {
            if (e.code == HttpURLConnection.HTTP_CONFLICT) {
                throw JobAlreadyExistsException(request.jobName)
            }
            throw KubernetesOperationException("Failed to create ConfigMap: ${e.message}", e)
        }

        // Step 2: Create FlinkDeployment (rollback ConfigMap on failure)
        val createdDeployment = try {
            flinkDeploymentClient.createFlinkDeployment(flinkDeployment)
        } catch (e: KubernetesClientException) {
            log.warn("FlinkDeployment creation failed, rolling back ConfigMap: {}", e.message)
            try {
                flinkDeploymentClient.deleteConfigMap(
                    createdConfigMap.metadata.name,
                    createdConfigMap.metadata.namespace
                )
            } catch (rollbackEx: Exception) {
                log.error("Failed to rollback ConfigMap: {}", rollbackEx.message)
            }
            if (e.code == HttpURLConnection.HTTP_CONFLICT) {
                throw JobAlreadyExistsException(request.jobName)
            }
            throw KubernetesOperationException("Failed to create FlinkDeployment: ${e.message}", e)
        }

        // Step 3: Patch ownerReference (warn-only on failure)
        try {
            val ownerRef = resourceBuilder.buildOwnerReference(
                createdDeployment.metadata.name,
                createdDeployment.metadata.uid
            )
            createdConfigMap.metadata.ownerReferences = listOf(ownerRef)
            flinkDeploymentClient.patchConfigMapOwnerRef(createdConfigMap)
        } catch (e: Exception) {
            log.warn(
                "Failed to patch ownerReference on ConfigMap '{}'. Manual cleanup may be needed on deletion: {}",
                createdConfigMap.metadata.name, e.message
            )
        }

        val namespace = request.namespace ?: k8sProperties.namespace
        return JobDetail(
            jobName = request.jobName,
            namespace = namespace,
            status = JobStatus.DEPLOYING,
            flinkImage = request.flinkImage,
            createdAt = Instant.now(),
            pipelineYaml = request.pipelineYaml,
            resources = buildResourceInfo(request),
            parallelism = request.parallelism ?: 1,
            kubernetes = null,
            flinkUiUrl = null,
            events = null
        )
    }

    fun listJobs(): List<JobSummary> {
        val deployments = flinkDeploymentClient.listFlinkDeployments(
            labels = resourceBuilder.managedBySelector()
        )
        return deployments.map { toJobSummary(it) }
    }

    fun getJob(jobName: String): JobDetail {
        val deployment = flinkDeploymentClient.getFlinkDeployment(jobName)
            ?: throw JobNotFoundException(jobName)

        val namespace = deployment.metadata.namespace
        val configMap = flinkDeploymentClient.getConfigMap("$jobName-pipeline", namespace)
        val events = flinkDeploymentClient.getEvents(namespace, jobName)

        return toJobDetail(deployment, configMap?.data?.get("pipeline.yaml"), events)
    }

    fun deleteJob(jobName: String) {
        val deployment = flinkDeploymentClient.getFlinkDeployment(jobName)
            ?: throw JobNotFoundException(jobName)

        val namespace = deployment.metadata.namespace

        // Primary: Delete FlinkDeployment (ownerRef cascades ConfigMap)
        try {
            flinkDeploymentClient.deleteFlinkDeployment(jobName, namespace)
        } catch (e: KubernetesClientException) {
            throw KubernetesOperationException("Failed to delete FlinkDeployment: ${e.message}", e)
        }

        // Fallback: Clean up ConfigMaps by label (defensive, in case ownerRef was not set)
        flinkDeploymentClient.deleteConfigMapsByLabel(
            namespace,
            mapOf(
                ResourceBuilder.MANAGED_BY_LABEL to ResourceBuilder.MANAGED_BY_VALUE,
                ResourceBuilder.NAME_LABEL to jobName
            )
        )
    }

    private fun toJobSummary(resource: GenericKubernetesResource): JobSummary {
        val createdAt = parseCreatedAt(resource)

        return JobSummary(
            jobName = resource.metadata.name,
            namespace = resource.metadata.namespace,
            status = statusMapper.mapStatus(resource),
            flinkImage = resource.nestedString("spec", "image"),
            createdAt = createdAt,
            parallelism = resource.nestedInt("spec", "job", "parallelism")
        )
    }

    private fun toJobDetail(
        resource: GenericKubernetesResource,
        pipelineYaml: String?,
        events: List<Event>
    ): JobDetail {
        val createdAt = parseCreatedAt(resource)

        val statusMap = resource.nestedMap("status")
        val jobStatusMap = resource.nestedMap("status", "jobStatus")

        val kubernetesStatus = if (statusMap != null) {
            KubernetesStatus(
                lifecycleState = resource.nestedString("status", "lifecycleState"),
                jobManagerDeploymentStatus = resource.nestedString("status", "jobManagerDeploymentStatus"),
                jobStatus = if (jobStatusMap != null) {
                    FlinkJobStatus(
                        state = resource.nestedString("status", "jobStatus", "state"),
                        jobId = resource.nestedString("status", "jobStatus", "jobId"),
                        startTime = resource.nestedString("status", "jobStatus", "startTime")
                    )
                } else null,
                error = resource.nestedString("status", "error")
            )
        } else null

        val jmResource = resource.nestedMap("spec", "jobManager", "resource")
        val tmResource = resource.nestedMap("spec", "taskManager", "resource")
        val tmReplicas = resource.nestedInt("spec", "taskManager", "replicas")

        val resourceInfo = ResourceInfo(
            jobManager = jmResource?.let {
                ManagerResourceInfo(cpu = it["cpu"], memory = it["memory"] as? String)
            },
            taskManager = tmResource?.let {
                TaskManagerResourceInfo(
                    cpu = it["cpu"],
                    memory = it["memory"] as? String,
                    replicas = tmReplicas
                )
            }
        )

        val namespace = resource.metadata.namespace
        val flinkUiUrl = "http://${resource.metadata.name}-rest.$namespace:8081"

        val eventInfos = events.map { event ->
            val ts = event.metadata?.creationTimestamp
                ?.let { runCatching { Instant.parse(it) }.getOrNull() }
            EventInfo(
                type = event.type ?: "",
                reason = event.reason ?: "",
                message = event.note ?: "",
                timestamp = ts
            )
        }

        return JobDetail(
            jobName = resource.metadata.name,
            namespace = namespace,
            status = statusMapper.mapStatus(resource),
            flinkImage = resource.nestedString("spec", "image"),
            createdAt = createdAt,
            pipelineYaml = pipelineYaml,
            resources = resourceInfo,
            parallelism = resource.nestedInt("spec", "job", "parallelism"),
            kubernetes = kubernetesStatus,
            flinkUiUrl = flinkUiUrl,
            events = eventInfos
        )
    }

    private fun parseCreatedAt(resource: GenericKubernetesResource): Instant? {
        return resource.metadata.annotations?.get(ResourceBuilder.CREATED_AT_ANNOTATION)
            ?.let { runCatching { Instant.parse(it) }.getOrNull() }
            ?: resource.metadata.creationTimestamp?.let {
                runCatching { Instant.parse(it) }.getOrNull()
            }
    }

    private fun buildResourceInfo(request: CreateJobRequest): ResourceInfo {
        return ResourceInfo(
            jobManager = ManagerResourceInfo(
                cpu = request.resources?.jobManager?.cpu ?: 1,
                memory = request.resources?.jobManager?.memory ?: "1024m"
            ),
            taskManager = TaskManagerResourceInfo(
                cpu = request.resources?.taskManager?.cpu ?: 1,
                memory = request.resources?.taskManager?.memory ?: "2048m",
                replicas = request.resources?.taskManager?.replicas ?: 1
            )
        )
    }
}

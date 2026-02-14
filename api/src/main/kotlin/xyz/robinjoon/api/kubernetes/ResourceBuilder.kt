package xyz.robinjoon.api.kubernetes

import io.fabric8.kubernetes.api.model.ConfigMap
import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder
import io.fabric8.kubernetes.api.model.ObjectMeta
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder
import io.fabric8.kubernetes.api.model.OwnerReference
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder
import org.springframework.stereotype.Component
import xyz.robinjoon.api.config.FlinkCdcDefaults
import xyz.robinjoon.api.config.KubernetesProperties
import xyz.robinjoon.api.model.CreateJobRequest
import java.time.Instant

@Component
class ResourceBuilder(
    private val k8sProperties: KubernetesProperties,
    private val flinkDefaults: FlinkCdcDefaults
) {

    companion object {
        const val MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
        const val MANAGED_BY_VALUE = "flink-cdc-admin"
        const val NAME_LABEL = "app.kubernetes.io/name"
        const val COMPONENT_LABEL = "app.kubernetes.io/component"
        const val VERSION_LABEL = "flink-cdc-admin/version"
        const val CREATED_AT_ANNOTATION = "flink-cdc-admin/created-at"
        const val FLINK_DEPLOYMENT_API_VERSION = "flink.apache.org/v1beta1"
        const val FLINK_DEPLOYMENT_KIND = "FlinkDeployment"
    }

    fun buildConfigMap(request: CreateJobRequest): ConfigMap {
        val namespace = request.namespace ?: k8sProperties.namespace
        val labels = commonLabels(request.jobName, "pipeline-config")
        val data = mapOf<String, String>("pipeline.yaml" to request.pipelineYaml)

        val meta = ObjectMeta()
        meta.name = "${request.jobName}-pipeline"
        meta.namespace = namespace
        meta.labels = labels

        val cm = ConfigMap()
        cm.metadata = meta
        cm.data = data
        return cm
    }

    fun buildFlinkDeployment(request: CreateJobRequest): GenericKubernetesResource {
        val namespace = request.namespace ?: k8sProperties.namespace
        val flinkVersion = request.flink?.version ?: flinkDefaults.flinkVersion
        val serviceAccount = request.flink?.serviceAccount ?: flinkDefaults.serviceAccount
        val parallelism = request.parallelism ?: 1
        val jmCpu = request.resources?.jobManager?.cpu ?: 1
        val jmMemory = request.resources?.jobManager?.memory ?: "1024m"
        val tmCpu = request.resources?.taskManager?.cpu ?: 1
        val tmMemory = request.resources?.taskManager?.memory ?: "2048m"
        val tmReplicas = request.resources?.taskManager?.replicas ?: 1

        val flinkConfiguration = mutableMapOf<String, String>(
            "classloader.resolve-order" to "parent-first",
            "taskmanager.numberOfTaskSlots" to parallelism.toString()
        )
        request.flink?.extraConfig?.let { flinkConfiguration.putAll(it) }

        val spec = linkedMapOf<String, Any>(
            "image" to request.flinkImage,
            "imagePullPolicy" to "IfNotPresent",
            "flinkVersion" to flinkVersion,
            "serviceAccount" to serviceAccount,
            "flinkConfiguration" to flinkConfiguration,
            "jobManager" to linkedMapOf<String, Any>(
                "replicas" to 1,
                "resource" to linkedMapOf<String, Any>(
                    "cpu" to jmCpu,
                    "memory" to jmMemory
                )
            ),
            "taskManager" to linkedMapOf<String, Any>(
                "replicas" to tmReplicas,
                "resource" to linkedMapOf<String, Any>(
                    "cpu" to tmCpu,
                    "memory" to tmMemory
                )
            ),
            "job" to linkedMapOf<String, Any>(
                "jarURI" to flinkDefaults.jarUri,
                "entryClass" to flinkDefaults.entryClass,
                "args" to listOf("${flinkDefaults.pipelineMountPath}/pipeline.yaml"),
                "parallelism" to parallelism,
                "state" to "running",
                "upgradeMode" to "savepoint"
            ),
            "podTemplate" to linkedMapOf<String, Any>(
                "apiVersion" to "v1",
                "kind" to "Pod",
                "spec" to linkedMapOf<String, Any>(
                    "containers" to listOf(
                        linkedMapOf<String, Any>(
                            "name" to "flink-main-container",
                            "volumeMounts" to listOf(
                                linkedMapOf<String, Any>(
                                    "name" to "cdc-pipeline-config",
                                    "mountPath" to flinkDefaults.pipelineMountPath,
                                    "readOnly" to true
                                )
                            )
                        )
                    ),
                    "volumes" to listOf(
                        linkedMapOf<String, Any>(
                            "name" to "cdc-pipeline-config",
                            "configMap" to linkedMapOf<String, Any>(
                                "name" to "${request.jobName}-pipeline"
                            )
                        )
                    )
                )
            )
        )

        val labels = commonLabels(request.jobName, "flink-cdc-pipeline")
        val annotations = mapOf<String, String>(CREATED_AT_ANNOTATION to Instant.now().toString())

        val meta = ObjectMeta()
        meta.name = request.jobName
        meta.namespace = namespace
        meta.labels = labels
        meta.annotations = annotations

        val resource = GenericKubernetesResource()
        resource.apiVersion = FLINK_DEPLOYMENT_API_VERSION
        resource.kind = FLINK_DEPLOYMENT_KIND
        resource.metadata = meta
        resource.additionalProperties["spec"] = spec
        return resource
    }

    fun buildOwnerReference(
        flinkDeploymentName: String,
        flinkDeploymentUid: String
    ): OwnerReference {
        val ownerRef = OwnerReference()
        ownerRef.apiVersion = FLINK_DEPLOYMENT_API_VERSION
        ownerRef.kind = FLINK_DEPLOYMENT_KIND
        ownerRef.name = flinkDeploymentName
        ownerRef.uid = flinkDeploymentUid
        ownerRef.controller = true
        ownerRef.blockOwnerDeletion = true
        return ownerRef
    }

    fun managedBySelector(): Map<String, String> {
        return mapOf(MANAGED_BY_LABEL to MANAGED_BY_VALUE)
    }

    private fun commonLabels(jobName: String, component: String): Map<String, String> {
        return mapOf(
            MANAGED_BY_LABEL to MANAGED_BY_VALUE,
            NAME_LABEL to jobName,
            COMPONENT_LABEL to component,
            VERSION_LABEL to "v0"
        )
    }
}

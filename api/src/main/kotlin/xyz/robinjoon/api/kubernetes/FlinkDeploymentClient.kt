package xyz.robinjoon.api.kubernetes

import io.fabric8.kubernetes.api.model.ConfigMap
import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import xyz.robinjoon.api.config.KubernetesProperties

@Component
class FlinkDeploymentClient(
    private val kubernetesClient: KubernetesClient,
    private val k8sProperties: KubernetesProperties,
    private val retryExecutor: KubernetesRetryExecutor
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val flinkDeploymentContext = ResourceDefinitionContext.Builder()
        .withGroup("flink.apache.org")
        .withVersion("v1beta1")
        .withPlural("flinkdeployments")
        .withNamespaced(true)
        .build()

    fun createConfigMap(configMap: ConfigMap): ConfigMap {
        return retryExecutor.execute("createConfigMap") {
            kubernetesClient.configMaps()
                .inNamespace(configMap.metadata.namespace)
                .resource(configMap)
                .create()
        }
    }

    fun createFlinkDeployment(resource: GenericKubernetesResource): GenericKubernetesResource {
        return retryExecutor.execute("createFlinkDeployment") {
            kubernetesClient.genericKubernetesResources(flinkDeploymentContext)
                .inNamespace(resource.metadata.namespace)
                .resource(resource)
                .create()
        }
    }

    fun patchConfigMapOwnerRef(configMap: ConfigMap): ConfigMap {
        return retryExecutor.execute("patchConfigMapOwnerRef") {
            kubernetesClient.configMaps()
                .inNamespace(configMap.metadata.namespace)
                .resource(configMap)
                .patch()
        }
    }

    fun getFlinkDeployment(name: String, namespace: String? = null): GenericKubernetesResource? {
        val ns = namespace ?: k8sProperties.namespace
        return retryExecutor.execute("getFlinkDeployment") {
            kubernetesClient.genericKubernetesResources(flinkDeploymentContext)
                .inNamespace(ns)
                .withName(name)
                .get()
        }
    }

    fun listFlinkDeployments(
        namespace: String? = null,
        labels: Map<String, String> = emptyMap()
    ): List<GenericKubernetesResource> {
        val ns = namespace ?: k8sProperties.namespace
        return retryExecutor.execute("listFlinkDeployments") {
            kubernetesClient.genericKubernetesResources(flinkDeploymentContext)
                .inNamespace(ns)
                .withLabels(labels)
                .list()
                .items
        }
    }

    fun deleteFlinkDeployment(name: String, namespace: String? = null) {
        val ns = namespace ?: k8sProperties.namespace
        retryExecutor.execute("deleteFlinkDeployment") {
            kubernetesClient.genericKubernetesResources(flinkDeploymentContext)
                .inNamespace(ns)
                .withName(name)
                .delete()
        }
    }

    fun getConfigMap(name: String, namespace: String? = null): ConfigMap? {
        val ns = namespace ?: k8sProperties.namespace
        return retryExecutor.execute("getConfigMap") {
            kubernetesClient.configMaps()
                .inNamespace(ns)
                .withName(name)
                .get()
        }
    }

    fun deleteConfigMap(name: String, namespace: String? = null) {
        val ns = namespace ?: k8sProperties.namespace
        retryExecutor.execute("deleteConfigMap") {
            kubernetesClient.configMaps()
                .inNamespace(ns)
                .withName(name)
                .delete()
        }
    }

    fun deleteConfigMapsByLabel(
        namespace: String? = null,
        labels: Map<String, String>
    ) {
        val ns = namespace ?: k8sProperties.namespace
        try {
            retryExecutor.execute("deleteConfigMapsByLabel") {
                kubernetesClient.configMaps()
                    .inNamespace(ns)
                    .withLabels(labels)
                    .delete()
            }
        } catch (e: Exception) {
            log.warn("Failed to delete ConfigMaps by label {}: {}", labels, e.message)
        }
    }

    fun getEvents(namespace: String? = null, fieldSelector: String): List<io.fabric8.kubernetes.api.model.events.v1.Event> {
        val ns = namespace ?: k8sProperties.namespace
        return try {
            retryExecutor.execute("getEvents") {
                kubernetesClient.resources(io.fabric8.kubernetes.api.model.events.v1.Event::class.java)
                    .inNamespace(ns)
                    .list()
                    .items
                    .filter { event ->
                        event.regarding?.name?.let { fieldSelector.contains(it) || it == fieldSelector } ?: false
                    }
            }
        } catch (e: Exception) {
            log.warn("Failed to fetch events: {}", e.message)
            emptyList()
        }
    }
}

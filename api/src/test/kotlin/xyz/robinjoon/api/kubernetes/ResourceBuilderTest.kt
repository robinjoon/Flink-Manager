package xyz.robinjoon.api.kubernetes

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.robinjoon.api.config.FlinkCdcDefaults
import xyz.robinjoon.api.config.KubernetesProperties
import xyz.robinjoon.api.model.CreateJobRequest
import xyz.robinjoon.api.model.FlinkSpec
import xyz.robinjoon.api.model.ManagerResource
import xyz.robinjoon.api.model.ResourceSpec
import xyz.robinjoon.api.model.TaskManagerResource

class ResourceBuilderTest {

    private val k8sProperties = KubernetesProperties(namespace = "test-ns")
    private val flinkDefaults = FlinkCdcDefaults(
        flinkVersion = "v1_18",
        serviceAccount = "flink",
        jarUri = "local:///opt/flink/lib/flink-cdc-dist-3.3.0.jar",
        entryClass = "org.apache.flink.cdc.cli.CliFrontend",
        pipelineMountPath = "/opt/flink/cdc-pipeline"
    )
    private val builder = ResourceBuilder(k8sProperties, flinkDefaults)

    private val minimalRequest = CreateJobRequest(
        jobName = "test-job",
        pipelineYaml = "source:\n  type: mysql",
        flinkImage = "flink-cdc:latest"
    )

    @Test
    fun `buildConfigMap creates correct metadata`() {
        val cm = builder.buildConfigMap(minimalRequest)

        assertEquals("test-job-pipeline", cm.metadata.name)
        assertEquals("test-ns", cm.metadata.namespace)
        assertEquals("flink-cdc-admin", cm.metadata.labels["app.kubernetes.io/managed-by"])
        assertEquals("test-job", cm.metadata.labels["app.kubernetes.io/name"])
        assertEquals("pipeline-config", cm.metadata.labels["app.kubernetes.io/component"])
    }

    @Test
    fun `buildConfigMap stores pipeline yaml in data`() {
        val cm = builder.buildConfigMap(minimalRequest)

        assertEquals("source:\n  type: mysql", cm.data["pipeline.yaml"])
    }

    @Test
    fun `buildConfigMap uses request namespace when provided`() {
        val request = minimalRequest.copy(namespace = "custom-ns")
        val cm = builder.buildConfigMap(request)

        assertEquals("custom-ns", cm.metadata.namespace)
    }

    @Test
    fun `buildFlinkDeployment creates correct apiVersion and kind`() {
        val fd = builder.buildFlinkDeployment(minimalRequest)

        assertEquals("flink.apache.org/v1beta1", fd.apiVersion)
        assertEquals("FlinkDeployment", fd.kind)
    }

    @Test
    fun `buildFlinkDeployment creates correct metadata`() {
        val fd = builder.buildFlinkDeployment(minimalRequest)

        assertEquals("test-job", fd.metadata.name)
        assertEquals("test-ns", fd.metadata.namespace)
        assertEquals("flink-cdc-admin", fd.metadata.labels["app.kubernetes.io/managed-by"])
        assertNotNull(fd.metadata.annotations["flink-cdc-admin/created-at"])
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `buildFlinkDeployment sets correct spec fields`() {
        val fd = builder.buildFlinkDeployment(minimalRequest)
        val spec = fd.additionalProperties["spec"] as Map<String, Any>

        assertEquals("flink-cdc:latest", spec["image"])
        assertEquals("v1_18", spec["flinkVersion"])
        assertEquals("flink", spec["serviceAccount"])
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `buildFlinkDeployment sets classloader parent-first`() {
        val fd = builder.buildFlinkDeployment(minimalRequest)
        val spec = fd.additionalProperties["spec"] as Map<String, Any>
        val flinkConfig = spec["flinkConfiguration"] as Map<String, String>

        assertEquals("parent-first", flinkConfig["classloader.resolve-order"])
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `buildFlinkDeployment sets job jar and entry class`() {
        val fd = builder.buildFlinkDeployment(minimalRequest)
        val spec = fd.additionalProperties["spec"] as Map<String, Any>
        val job = spec["job"] as Map<String, Any>

        assertEquals("local:///opt/flink/lib/flink-cdc-dist-3.3.0.jar", job["jarURI"])
        assertEquals("org.apache.flink.cdc.cli.CliFrontend", job["entryClass"])
        assertEquals(listOf("/opt/flink/cdc-pipeline/pipeline.yaml"), job["args"])
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `buildFlinkDeployment uses custom resources`() {
        val request = minimalRequest.copy(
            resources = ResourceSpec(
                jobManager = ManagerResource(cpu = 2, memory = "2048m"),
                taskManager = TaskManagerResource(cpu = 4, memory = "4096m", replicas = 3)
            ),
            parallelism = 4
        )
        val fd = builder.buildFlinkDeployment(request)
        val spec = fd.additionalProperties["spec"] as Map<String, Any>
        val jm = spec["jobManager"] as Map<String, Any>
        val tm = spec["taskManager"] as Map<String, Any>
        val jmResource = jm["resource"] as Map<String, Any>
        val tmResource = tm["resource"] as Map<String, Any>

        assertEquals(2, jmResource["cpu"])
        assertEquals("2048m", jmResource["memory"])
        assertEquals(4, tmResource["cpu"])
        assertEquals("4096m", tmResource["memory"])
        assertEquals(3, tm["replicas"])
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `buildFlinkDeployment includes podTemplate with volume mount`() {
        val fd = builder.buildFlinkDeployment(minimalRequest)
        val spec = fd.additionalProperties["spec"] as Map<String, Any>
        val podTemplate = spec["podTemplate"] as Map<String, Any>
        val podSpec = podTemplate["spec"] as Map<String, Any>
        val volumes = podSpec["volumes"] as List<Map<String, Any>>
        val containers = podSpec["containers"] as List<Map<String, Any>>

        assertEquals(1, volumes.size)
        assertEquals("cdc-pipeline-config", volumes[0]["name"])

        val configMapRef = volumes[0]["configMap"] as Map<String, Any>
        assertEquals("test-job-pipeline", configMapRef["name"])

        assertEquals(1, containers.size)
        val volumeMounts = containers[0]["volumeMounts"] as List<Map<String, Any>>
        assertEquals("/opt/flink/cdc-pipeline", volumeMounts[0]["mountPath"])
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `buildFlinkDeployment merges extra flink config`() {
        val request = minimalRequest.copy(
            flink = FlinkSpec(extraConfig = mapOf("state.checkpoints.dir" to "file:///tmp/checkpoints"))
        )
        val fd = builder.buildFlinkDeployment(request)
        val spec = fd.additionalProperties["spec"] as Map<String, Any>
        val flinkConfig = spec["flinkConfiguration"] as Map<String, String>

        assertEquals("parent-first", flinkConfig["classloader.resolve-order"])
        assertEquals("file:///tmp/checkpoints", flinkConfig["state.checkpoints.dir"])
    }

    @Test
    fun `buildOwnerReference creates correct reference`() {
        val ref = builder.buildOwnerReference("test-deploy", "uid-123")

        assertEquals("flink.apache.org/v1beta1", ref.apiVersion)
        assertEquals("FlinkDeployment", ref.kind)
        assertEquals("test-deploy", ref.name)
        assertEquals("uid-123", ref.uid)
        assertTrue(ref.controller)
        assertTrue(ref.blockOwnerDeletion)
    }

    @Test
    fun `managedBySelector returns correct label`() {
        val selector = builder.managedBySelector()

        assertEquals("flink-cdc-admin", selector["app.kubernetes.io/managed-by"])
        assertEquals(1, selector.size)
    }
}

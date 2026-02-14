package xyz.robinjoon.api.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "flink-cdc-admin.flink-defaults")
data class FlinkCdcDefaults(
    val flinkVersion: String = "v1_18",
    val serviceAccount: String = "flink",
    val jarUri: String = "local:///opt/flink/lib/flink-cdc-dist-3.3.0.jar",
    val entryClass: String = "org.apache.flink.cdc.cli.CliFrontend",
    val pipelineMountPath: String = "/opt/flink/cdc-pipeline"
)

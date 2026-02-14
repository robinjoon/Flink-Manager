package xyz.robinjoon.api.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "flink-cdc-admin.kubernetes")
data class KubernetesProperties(
    val namespace: String = "flink-jobs"
)

package xyz.robinjoon.api.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "flink-cdc-admin.retry")
data class RetryProperties(
    val maxAttempts: Int = 3,
    val initialDelayMs: Long = 1000,
    val multiplier: Double = 2.0
)

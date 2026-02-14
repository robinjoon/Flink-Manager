package xyz.robinjoon.api.config

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(
    KubernetesProperties::class,
    FlinkCdcDefaults::class,
    RetryProperties::class
)
class AppConfig

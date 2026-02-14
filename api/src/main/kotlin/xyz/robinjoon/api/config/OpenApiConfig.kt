package xyz.robinjoon.api.config

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenApiConfig {

    @Bean
    fun openAPI(): OpenAPI {
        return OpenAPI()
            .info(
                Info()
                    .title("Flink CDC Admin API")
                    .description("Pipeline YAML 기반 Flink CDC 작업의 생성/조회/삭제를 위한 REST API")
                    .version("v0")
            )
    }
}

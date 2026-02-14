package xyz.robinjoon.api.kubernetes

import io.fabric8.kubernetes.client.KubernetesClientException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import xyz.robinjoon.api.config.RetryProperties
import java.net.HttpURLConnection

@Component
class KubernetesRetryExecutor(
    private val retryProperties: RetryProperties
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun <T> execute(operationName: String, operation: () -> T): T {
        var lastException: Exception? = null
        var delayMs = retryProperties.initialDelayMs

        for (attempt in 1..retryProperties.maxAttempts) {
            try {
                return operation()
            } catch (e: KubernetesClientException) {
                if (!isRetryable(e)) {
                    throw e
                }
                lastException = e
                if (attempt < retryProperties.maxAttempts) {
                    log.warn(
                        "Retryable error on '{}' (attempt {}/{}): {}",
                        operationName, attempt, retryProperties.maxAttempts, e.message
                    )
                    Thread.sleep(delayMs)
                    delayMs = (delayMs * retryProperties.multiplier).toLong()
                }
            }
        }

        throw lastException!!
    }

    private fun isRetryable(e: KubernetesClientException): Boolean {
        val code = e.code
        return code >= HttpURLConnection.HTTP_INTERNAL_ERROR || code == 0
    }
}

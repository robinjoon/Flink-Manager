package xyz.robinjoon.api.kubernetes

import io.fabric8.kubernetes.client.KubernetesClientException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import xyz.robinjoon.api.config.RetryProperties

class KubernetesRetryExecutorTest {

    private val retryProperties = RetryProperties(
        maxAttempts = 3,
        initialDelayMs = 10,
        multiplier = 2.0
    )
    private val executor = KubernetesRetryExecutor(retryProperties)

    @Test
    fun `succeeds on first attempt`() {
        val result = executor.execute("test") { "success" }
        assertEquals("success", result)
    }

    @Test
    fun `retries on 5xx error and succeeds`() {
        var attempts = 0
        val result = executor.execute("test") {
            attempts++
            if (attempts < 3) {
                throw KubernetesClientException("server error", 500, null)
            }
            "success"
        }
        assertEquals("success", result)
        assertEquals(3, attempts)
    }

    @Test
    fun `does not retry on 4xx error`() {
        var attempts = 0
        assertThrows(KubernetesClientException::class.java) {
            executor.execute("test") {
                attempts++
                throw KubernetesClientException("not found", 404, null)
            }
        }
        assertEquals(1, attempts)
    }

    @Test
    fun `does not retry on 409 conflict`() {
        var attempts = 0
        assertThrows(KubernetesClientException::class.java) {
            executor.execute("test") {
                attempts++
                throw KubernetesClientException("conflict", 409, null)
            }
        }
        assertEquals(1, attempts)
    }

    @Test
    fun `throws after max retries exhausted`() {
        var attempts = 0
        assertThrows(KubernetesClientException::class.java) {
            executor.execute("test") {
                attempts++
                throw KubernetesClientException("server error", 500, null)
            }
        }
        assertEquals(3, attempts)
    }

    @Test
    fun `retries on connection error (code 0)`() {
        var attempts = 0
        val result = executor.execute("test") {
            attempts++
            if (attempts < 2) {
                throw KubernetesClientException("connection refused", 0, null)
            }
            "connected"
        }
        assertEquals("connected", result)
        assertEquals(2, attempts)
    }
}

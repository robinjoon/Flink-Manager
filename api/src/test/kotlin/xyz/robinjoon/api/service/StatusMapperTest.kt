package xyz.robinjoon.api.service

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import xyz.robinjoon.api.model.JobStatus

class StatusMapperTest {

    private val statusMapper = StatusMapper()

    @ParameterizedTest
    @CsvSource(
        "CREATED,,DEPLOYING",
        "DEPLOYED,CREATED,DEPLOYING",
        "DEPLOYED,RECONCILING,DEPLOYING",
        "DEPLOYED,RUNNING,DEPLOYING",
        "STABLE,RUNNING,RUNNING",
        "STABLE,FINISHED,RUNNING",
        "SUSPENDED,,SUSPENDED",
        "SUSPENDED,RUNNING,SUSPENDED",
        "UPGRADING,,UPGRADING",
        "UPGRADING,RUNNING,UPGRADING",
        "ROLLING_BACK,,UPGRADING",
        "ROLLING_BACK,RUNNING,UPGRADING",
        "ROLLED_BACK,RUNNING,RUNNING",
        "FAILED,,FAILED",
        "FAILED,RUNNING,FAILED",
    )
    fun `maps lifecycle and job state to UI status`(
        lifecycleState: String?,
        jobState: String?,
        expected: String
    ) {
        val result = statusMapper.mapStatus(lifecycleState, jobState)
        assertEquals(JobStatus.valueOf(expected), result)
    }

    @Test
    fun `FAILED job state takes priority over lifecycle state`() {
        assertEquals(JobStatus.FAILED, statusMapper.mapStatus("STABLE", "FAILED"))
        assertEquals(JobStatus.FAILED, statusMapper.mapStatus("DEPLOYED", "FAILING"))
        assertEquals(JobStatus.FAILED, statusMapper.mapStatus("UPGRADING", "FAILED"))
    }

    @Test
    fun `null lifecycle and job state returns UNKNOWN`() {
        assertEquals(JobStatus.UNKNOWN, statusMapper.mapStatus(null, null))
    }

    @Test
    fun `unknown lifecycle state returns UNKNOWN`() {
        assertEquals(JobStatus.UNKNOWN, statusMapper.mapStatus("UNKNOWN_STATE", null))
    }

    @Test
    fun `ROLLED_BACK with non-RUNNING job state returns UNKNOWN`() {
        assertEquals(JobStatus.UNKNOWN, statusMapper.mapStatus("ROLLED_BACK", "CREATED"))
    }
}

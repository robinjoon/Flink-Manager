package xyz.robinjoon.api.controller

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.doThrow
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import xyz.robinjoon.api.exception.JobAlreadyExistsException
import xyz.robinjoon.api.exception.JobNotFoundException
import xyz.robinjoon.api.model.*
import xyz.robinjoon.api.service.JobService
import java.time.Instant

@WebMvcTest(JobController::class)
class JobControllerTest {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @MockitoBean
    private lateinit var jobService: JobService

    private val sampleJobDetail = JobDetail(
        jobName = "test-job",
        namespace = "flink-jobs",
        status = JobStatus.DEPLOYING,
        flinkImage = "flink-cdc:latest",
        createdAt = Instant.parse("2026-02-13T12:00:00Z"),
        pipelineYaml = "source:\n  type: mysql",
        resources = null,
        parallelism = 1,
        kubernetes = null,
        flinkUiUrl = null,
        events = null
    )

    @Test
    fun `POST creates job and returns 201`() {
        whenever(jobService.createJob(any())).thenReturn(sampleJobDetail)

        val request = mapOf(
            "jobName" to "test-job",
            "pipelineYaml" to "source:\n  type: mysql",
            "flinkImage" to "flink-cdc:latest"
        )

        mockMvc.perform(
            post("/api/v1/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.jobName").value("test-job"))
            .andExpect(jsonPath("$.status").value("DEPLOYING"))
    }

    @Test
    fun `POST returns 400 for invalid jobName`() {
        val request = mapOf(
            "jobName" to "INVALID_NAME",
            "pipelineYaml" to "source:\n  type: mysql",
            "flinkImage" to "flink-cdc:latest"
        )

        mockMvc.perform(
            post("/api/v1/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isBadRequest)
    }

    @Test
    fun `POST returns 400 for missing required fields`() {
        val request = mapOf("jobName" to "test-job")

        mockMvc.perform(
            post("/api/v1/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isBadRequest)
    }

    @Test
    fun `POST returns 409 when job already exists`() {
        whenever(jobService.createJob(any())).thenThrow(JobAlreadyExistsException("test-job"))

        val request = mapOf(
            "jobName" to "test-job",
            "pipelineYaml" to "source:\n  type: mysql",
            "flinkImage" to "flink-cdc:latest"
        )

        mockMvc.perform(
            post("/api/v1/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isConflict)
            .andExpect(jsonPath("$.status").value(409))
    }

    @Test
    fun `GET list returns 200 with jobs`() {
        val summary = JobSummary(
            jobName = "test-job",
            namespace = "flink-jobs",
            status = JobStatus.RUNNING,
            flinkImage = "flink-cdc:latest",
            createdAt = Instant.parse("2026-02-13T12:00:00Z"),
            parallelism = 2
        )
        whenever(jobService.listJobs()).thenReturn(listOf(summary))

        mockMvc.perform(get("/api/v1/jobs"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$[0].jobName").value("test-job"))
            .andExpect(jsonPath("$[0].status").value("RUNNING"))
    }

    @Test
    fun `GET detail returns 200 with job`() {
        whenever(jobService.getJob("test-job")).thenReturn(sampleJobDetail)

        mockMvc.perform(get("/api/v1/jobs/test-job"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.jobName").value("test-job"))
    }

    @Test
    fun `GET detail returns 404 when not found`() {
        whenever(jobService.getJob("missing-job")).thenThrow(JobNotFoundException("missing-job"))

        mockMvc.perform(get("/api/v1/jobs/missing-job"))
            .andExpect(status().isNotFound)
            .andExpect(jsonPath("$.status").value(404))
    }

    @Test
    fun `DELETE returns 200 on success`() {
        doNothing().whenever(jobService).deleteJob("test-job")

        mockMvc.perform(delete("/api/v1/jobs/test-job"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.message").exists())
    }

    @Test
    fun `DELETE returns 404 when not found`() {
        doThrow(JobNotFoundException("missing-job")).whenever(jobService).deleteJob("missing-job")

        mockMvc.perform(delete("/api/v1/jobs/missing-job"))
            .andExpect(status().isNotFound)
    }
}

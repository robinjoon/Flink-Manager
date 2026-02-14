package xyz.robinjoon.api.controller

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.ArraySchema
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import xyz.robinjoon.api.model.CreateJobRequest
import xyz.robinjoon.api.model.ErrorResponse
import xyz.robinjoon.api.model.JobDetail
import xyz.robinjoon.api.model.JobSummary
import xyz.robinjoon.api.service.JobService

@Tag(name = "Jobs", description = "Flink CDC 작업 관리 API")
@RestController
@RequestMapping("/api/v1/jobs")
class JobController(
    private val jobService: JobService
) {

    @Operation(summary = "작업 생성", description = "Pipeline YAML 기반 Flink CDC 작업을 생성합니다.")
    @ApiResponses(
        ApiResponse(responseCode = "201", description = "작업 생성 성공",
            content = [Content(schema = Schema(implementation = JobDetail::class))]),
        ApiResponse(responseCode = "400", description = "잘못된 요청 (유효성 검증 실패)",
            content = [Content(schema = Schema(implementation = ErrorResponse::class))]),
        ApiResponse(responseCode = "409", description = "동일한 이름의 작업이 이미 존재",
            content = [Content(schema = Schema(implementation = ErrorResponse::class))])
    )
    @PostMapping
    fun createJob(@Valid @RequestBody request: CreateJobRequest): ResponseEntity<JobDetail> {
        val jobDetail = jobService.createJob(request)
        return ResponseEntity.status(HttpStatus.CREATED).body(jobDetail)
    }

    @Operation(summary = "작업 목록 조회", description = "관리 중인 모든 Flink CDC 작업 목록을 조회합니다.")
    @ApiResponse(responseCode = "200", description = "조회 성공",
        content = [Content(array = ArraySchema(schema = Schema(implementation = JobSummary::class)))])
    @GetMapping
    fun listJobs(): ResponseEntity<List<JobSummary>> {
        val jobs = jobService.listJobs()
        return ResponseEntity.ok(jobs)
    }

    @Operation(summary = "작업 상세 조회", description = "특정 Flink CDC 작업의 상세 정보를 조회합니다.")
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "조회 성공",
            content = [Content(schema = Schema(implementation = JobDetail::class))]),
        ApiResponse(responseCode = "404", description = "작업을 찾을 수 없음",
            content = [Content(schema = Schema(implementation = ErrorResponse::class))])
    )
    @GetMapping("/{jobName}")
    fun getJob(@PathVariable jobName: String): ResponseEntity<JobDetail> {
        val jobDetail = jobService.getJob(jobName)
        return ResponseEntity.ok(jobDetail)
    }

    @Operation(summary = "작업 삭제", description = "Flink CDC 작업과 관련 리소스(ConfigMap)를 삭제합니다.")
    @ApiResponses(
        ApiResponse(responseCode = "200", description = "삭제 성공"),
        ApiResponse(responseCode = "404", description = "작업을 찾을 수 없음",
            content = [Content(schema = Schema(implementation = ErrorResponse::class))])
    )
    @DeleteMapping("/{jobName}")
    fun deleteJob(@PathVariable jobName: String): ResponseEntity<Map<String, String>> {
        jobService.deleteJob(jobName)
        return ResponseEntity.ok(mapOf("message" to "Job '$jobName' deleted successfully"))
    }
}

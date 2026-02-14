package xyz.robinjoon.api.controller

import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import xyz.robinjoon.api.model.CreateJobRequest
import xyz.robinjoon.api.model.JobDetail
import xyz.robinjoon.api.model.JobSummary
import xyz.robinjoon.api.service.JobService

@RestController
@RequestMapping("/api/v1/jobs")
class JobController(
    private val jobService: JobService
) {

    @PostMapping
    fun createJob(@Valid @RequestBody request: CreateJobRequest): ResponseEntity<JobDetail> {
        val jobDetail = jobService.createJob(request)
        return ResponseEntity.status(HttpStatus.CREATED).body(jobDetail)
    }

    @GetMapping
    fun listJobs(): ResponseEntity<List<JobSummary>> {
        val jobs = jobService.listJobs()
        return ResponseEntity.ok(jobs)
    }

    @GetMapping("/{jobName}")
    fun getJob(@PathVariable jobName: String): ResponseEntity<JobDetail> {
        val jobDetail = jobService.getJob(jobName)
        return ResponseEntity.ok(jobDetail)
    }

    @DeleteMapping("/{jobName}")
    fun deleteJob(@PathVariable jobName: String): ResponseEntity<Map<String, String>> {
        jobService.deleteJob(jobName)
        return ResponseEntity.ok(mapOf("message" to "Job '$jobName' deleted successfully"))
    }
}

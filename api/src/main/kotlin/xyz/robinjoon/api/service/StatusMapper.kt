package xyz.robinjoon.api.service

import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import org.springframework.stereotype.Component
import xyz.robinjoon.api.kubernetes.nestedString
import xyz.robinjoon.api.model.JobStatus

@Component
class StatusMapper {

    fun mapStatus(resource: GenericKubernetesResource): JobStatus {
        val lifecycleState = resource.nestedString("status", "lifecycleState")
        val jobState = resource.nestedString("status", "jobStatus", "state")

        return mapStatus(lifecycleState, jobState)
    }

    fun mapStatus(lifecycleState: String?, jobState: String?): JobStatus {
        if (lifecycleState == null && jobState == null) {
            return JobStatus.UNKNOWN
        }

        // FAILED takes priority regardless of lifecycleState
        if (jobState == "FAILED" || jobState == "FAILING") {
            return JobStatus.FAILED
        }
        if (lifecycleState == "FAILED") {
            return JobStatus.FAILED
        }

        return when (lifecycleState) {
            "CREATED" -> JobStatus.DEPLOYING
            "DEPLOYED" -> JobStatus.DEPLOYING
            "STABLE" -> when (jobState) {
                "RUNNING", "FINISHED" -> JobStatus.RUNNING
                else -> JobStatus.RUNNING
            }
            "SUSPENDED" -> JobStatus.SUSPENDED
            "UPGRADING", "ROLLING_BACK" -> JobStatus.UPGRADING
            "ROLLED_BACK" -> when (jobState) {
                "RUNNING" -> JobStatus.RUNNING
                else -> JobStatus.UNKNOWN
            }
            else -> JobStatus.UNKNOWN
        }
    }
}

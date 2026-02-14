package xyz.robinjoon.api.exception

class JobNotFoundException(jobName: String) :
    RuntimeException("Job '$jobName' not found")

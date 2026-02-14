package xyz.robinjoon.api.exception

class JobAlreadyExistsException(jobName: String) :
    RuntimeException("Job '$jobName' already exists")

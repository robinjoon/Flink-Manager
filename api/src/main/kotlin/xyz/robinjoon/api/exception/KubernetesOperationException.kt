package xyz.robinjoon.api.exception

class KubernetesOperationException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

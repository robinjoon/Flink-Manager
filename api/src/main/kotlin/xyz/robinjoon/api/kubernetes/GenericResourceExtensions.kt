package xyz.robinjoon.api.kubernetes

import io.fabric8.kubernetes.api.model.GenericKubernetesResource

fun GenericKubernetesResource.nestedString(vararg keys: String): String? {
    return nestedValue<String>(*keys)
}

fun GenericKubernetesResource.nestedInt(vararg keys: String): Int? {
    return when (val value = nestedValue<Any>(*keys)) {
        is Int -> value
        is Number -> value.toInt()
        is String -> value.toIntOrNull()
        else -> null
    }
}

@Suppress("UNCHECKED_CAST")
fun <T> GenericKubernetesResource.nestedValue(vararg keys: String): T? {
    var current: Any? = additionalProperties
    for (key in keys) {
        current = when (current) {
            is Map<*, *> -> current[key]
            else -> return null
        }
    }
    return current as? T
}

@Suppress("UNCHECKED_CAST")
fun GenericKubernetesResource.nestedMap(vararg keys: String): Map<String, Any>? {
    return nestedValue<Map<String, Any>>(*keys)
}

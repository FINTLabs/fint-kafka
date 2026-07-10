package no.novari.kafka.health

import org.springframework.boot.SpringApplication
import org.springframework.boot.env.EnvironmentPostProcessor
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.MapPropertySource

class KafkaHealthGroupEnvironmentPostProcessor : EnvironmentPostProcessor {
    override fun postProcessEnvironment(
        environment: ConfigurableEnvironment,
        application: SpringApplication,
    ) {
        if (!environment.getProperty("fint.kafka.health.enabled", Boolean::class.java, true)) {
            return
        }

        val defaults = mutableMapOf<String, Any>()
        if (environment.getProperty(READINESS_INCLUDE) == null) {
            defaults[READINESS_INCLUDE] = "readinessState,kafkaConnectivity"
        }
        if (environment.getProperty(LIVENESS_INCLUDE) == null) {
            defaults[LIVENESS_INCLUDE] = "livenessState,kafkaConsumers"
        }
        if (environment.getProperty(VALIDATE_GROUP_MEMBERSHIP) == null) {
            defaults[VALIDATE_GROUP_MEMBERSHIP] = "false"
        }

        if (defaults.isNotEmpty()) {
            environment.propertySources.addLast(MapPropertySource("fintKafkaHealthGroupDefaults", defaults))
        }
    }

    private companion object {
        private const val READINESS_INCLUDE = "management.endpoint.health.group.readiness.include"
        private const val LIVENESS_INCLUDE = "management.endpoint.health.group.liveness.include"
        private const val VALIDATE_GROUP_MEMBERSHIP = "management.endpoint.health.validate-group-membership"
    }
}

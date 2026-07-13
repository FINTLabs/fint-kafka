package no.novari.kafka.health

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.springframework.boot.SpringApplication
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment

class KafkaHealthGroupEnvironmentPostProcessorTest {
    private val postProcessor = KafkaHealthGroupEnvironmentPostProcessor()

    @Test
    fun `adds default health groups when they are absent`() {
        val environment = StandardEnvironment()

        postProcessor.postProcessEnvironment(environment, mock<SpringApplication>())

        assertThat(environment.getProperty(READINESS_INCLUDE)).contains("kafkaConnectivityHealthIndicator")
        assertThat(environment.getProperty(LIVENESS_INCLUDE)).contains("kafkaConsumersHealthIndicator")
    }

    @Test
    fun `does not override an explicit application config`() {
        val environment = StandardEnvironment()
        environment.propertySources.addFirst(
            MapPropertySource("app", mapOf(READINESS_INCLUDE to "readinessState")),
        )

        postProcessor.postProcessEnvironment(environment, mock<SpringApplication>())

        assertThat(environment.getProperty(READINESS_INCLUDE)).isEqualTo("readinessState")
        assertThat(environment.getProperty(LIVENESS_INCLUDE)).contains("kafkaConsumersHealthIndicator")
    }

    @Test
    fun `is skipped when health is disabled`() {
        val environment = StandardEnvironment()
        environment.propertySources.addFirst(
            MapPropertySource("app", mapOf("fint.kafka.health.enabled" to "false")),
        )

        postProcessor.postProcessEnvironment(environment, mock<SpringApplication>())

        assertThat(environment.getProperty(READINESS_INCLUDE)).isNull()
        assertThat(environment.getProperty(LIVENESS_INCLUDE)).isNull()
    }

    private companion object {
        private const val READINESS_INCLUDE = "management.endpoint.health.group.readiness.include"
        private const val LIVENESS_INCLUDE = "management.endpoint.health.group.liveness.include"
    }
}

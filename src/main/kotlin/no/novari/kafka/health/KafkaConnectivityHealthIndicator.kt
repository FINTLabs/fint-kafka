package no.novari.kafka.health

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.DescribeClusterOptions
import org.slf4j.LoggerFactory
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class KafkaConnectivityHealthIndicator(
    private val adminClient: AdminClient,
    private val producerFailureTracker: ProducerFailureTracker,
    properties: KafkaHealthProperties.Connectivity,
) : HealthIndicator {
    private val timeoutMillis: Long = properties.timeout.toMillis()
    private val cacheTtlMillis: Long = properties.cacheTtl.toMillis()
    private val failureThreshold: Int = properties.failureThreshold

    private var lastConnectivityHealth: Health? = null
    private var lastProbeAt: Long = 0
    private var consecutiveFailures: Int = 0

    override fun health(): Health {
        if (producerFailureTracker.isUnhealthy()) {
            return Health
                .down()
                .withDetail("reason", "Producer send failures exceeded threshold")
                .build()
        }
        return connectivityHealth()
    }

    @Synchronized
    private fun connectivityHealth(): Health {
        val now = System.currentTimeMillis()
        lastConnectivityHealth?.let {
            if (now - lastProbeAt < cacheTtlMillis) {
                return it
            }
        }
        lastProbeAt = now

        val health =
            try {
                val clusterId =
                    adminClient
                        .describeCluster(DescribeClusterOptions().timeoutMs(timeoutMillis.toInt()))
                        .clusterId()
                        .get(timeoutMillis, TimeUnit.MILLISECONDS)
                consecutiveFailures = 0
                Health.up().withDetail("clusterId", clusterId ?: "unknown").build()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                onProbeFailure(e)
            } catch (e: ExecutionException) {
                onProbeFailure(e)
            } catch (e: TimeoutException) {
                onProbeFailure(e)
            } catch (e: RuntimeException) {
                onProbeFailure(e)
            }

        lastConnectivityHealth = health
        return health
    }

    private fun onProbeFailure(e: Exception): Health {
        consecutiveFailures++
        log.warn("Kafka cluster not reachable ({} consecutive failure(s)): {}", consecutiveFailures, e.message)
        return if (consecutiveFailures >= failureThreshold) {
            Health
                .down()
                .withDetail("reason", "Kafka cluster not reachable")
                .withDetail("consecutiveFailures", consecutiveFailures)
                .withDetail("error", e.message.toString())
                .build()
        } else {
            Health
                .up()
                .withDetail("degraded", "Transient connectivity failure below threshold")
                .withDetail("consecutiveFailures", consecutiveFailures)
                .build()
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(KafkaConnectivityHealthIndicator::class.java)
    }
}

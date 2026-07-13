package no.novari.kafka.health

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.DescribeClusterOptions
import org.apache.kafka.clients.admin.DescribeClusterResult
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.boot.actuate.health.Status
import java.time.Duration

class KafkaConnectivityHealthIndicatorTest {
    private fun properties(failureThreshold: Int): KafkaHealthProperties.Connectivity =
        KafkaHealthProperties.Connectivity().apply {
            timeout = Duration.ofMillis(500)
            cacheTtl = Duration.ZERO
            this.failureThreshold = failureThreshold
        }

    private fun clusterResult(clusterId: KafkaFuture<String>): DescribeClusterResult {
        val result = mock<DescribeClusterResult>()
        whenever(result.clusterId()).thenReturn(clusterId)
        return result
    }

    @Test
    fun `is up when the cluster is reachable`() {
        val result = clusterResult(KafkaFuture.completedFuture("cluster-1"))
        val adminClient = mock<AdminClient>()
        whenever(adminClient.describeCluster(any<DescribeClusterOptions>())).thenReturn(result)
        val tracker =
            mock<ProducerFailureTracker> {
                on { isUnhealthy() } doReturn false
            }

        val indicator = KafkaConnectivityHealthIndicator(adminClient, tracker, properties(2))

        assertThat(indicator.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `goes down only after consecutive failures reach the threshold`() {
        val failed = KafkaFutureImpl<String>().apply { completeExceptionally(RuntimeException("no route to broker")) }
        val result = clusterResult(failed)
        val adminClient = mock<AdminClient>()
        whenever(adminClient.describeCluster(any<DescribeClusterOptions>())).thenReturn(result)
        val tracker =
            mock<ProducerFailureTracker> {
                on { isUnhealthy() } doReturn false
            }

        val indicator = KafkaConnectivityHealthIndicator(adminClient, tracker, properties(2))

        assertThat(indicator.health().status).isEqualTo(Status.UP)
        assertThat(indicator.health().status).isEqualTo(Status.DOWN)
    }

    @Test
    fun `is down when the producer is unhealthy`() {
        val result = clusterResult(KafkaFuture.completedFuture("cluster-1"))
        val adminClient = mock<AdminClient>()
        whenever(adminClient.describeCluster(any<DescribeClusterOptions>())).thenReturn(result)
        val tracker =
            mock<ProducerFailureTracker> {
                on { isUnhealthy() } doReturn true
            }

        val indicator = KafkaConnectivityHealthIndicator(adminClient, tracker, properties(2))

        assertThat(indicator.health().status).isEqualTo(Status.DOWN)
    }

    @Test
    fun `merges producer and connectivity details when both are unhealthy`() {
        val failed = KafkaFutureImpl<String>().apply { completeExceptionally(RuntimeException("no route to broker")) }
        val result = clusterResult(failed)
        val adminClient = mock<AdminClient>()
        whenever(adminClient.describeCluster(any<DescribeClusterOptions>())).thenReturn(result)
        val tracker =
            mock<ProducerFailureTracker> {
                on { isUnhealthy() } doReturn true
            }

        val indicator = KafkaConnectivityHealthIndicator(adminClient, tracker, properties(2))
        indicator.health()
        val health = indicator.health()

        assertThat(health.status).isEqualTo(Status.DOWN)
        assertThat(health.details)
            .containsEntry("producerFailure", "Producer send failures exceeded threshold")
            .containsEntry("reason", "Kafka cluster not reachable")
            .containsKey("consecutiveFailures")
            .containsKey("error")
    }
}

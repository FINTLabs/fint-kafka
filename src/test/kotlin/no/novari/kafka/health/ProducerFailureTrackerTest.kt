package no.novari.kafka.health

import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

class ProducerFailureTrackerTest {
    private val record = ProducerRecord<Any, Any>("topic", "key", "value")

    @Test
    fun `is healthy when failure count is below threshold`() {
        val tracker = ProducerFailureTracker(3, Duration.ofMinutes(1))

        tracker.onError(record, null, RuntimeException("boom"))
        tracker.onError(record, null, RuntimeException("boom"))

        assertThat(tracker.isUnhealthy()).isFalse()
    }

    @Test
    fun `is unhealthy when failure count reaches threshold`() {
        val tracker = ProducerFailureTracker(3, Duration.ofMinutes(1))

        tracker.onError(record, null, RuntimeException("boom"))
        tracker.onError(record, null, RuntimeException("boom"))
        tracker.onError(record, null, RuntimeException("boom"))

        assertThat(tracker.isUnhealthy()).isTrue()
    }

    @Test
    fun `forgets failures once they fall outside the failure window`() {
        val tracker = ProducerFailureTracker(2, Duration.ofMillis(20))

        tracker.onError(record, null, RuntimeException("boom"))
        tracker.onError(record, null, RuntimeException("boom"))
        assertThat(tracker.isUnhealthy()).isTrue()

        Thread.sleep(40)

        assertThat(tracker.isUnhealthy()).isFalse()
    }
}

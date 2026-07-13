package no.novari.kafka.health

import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.health.Status
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.kafka.event.ConsumerStoppedEvent
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = ["fint.kafka.health.producer.failure-window=300ms"])
class KafkaHealthIntegrationTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val kafkaConsumers: KafkaConsumersHealthIndicator,
    @Autowired private val kafkaConnectivity: KafkaConnectivityHealthIndicator,
    @Autowired private val producerFailureTracker: ProducerFailureTracker,
) {
    @Test
    fun `registers the health indicators under their expected bean names`() {
        assertThat(applicationContext.containsBean("kafkaConsumersHealthIndicator")).isTrue()
        assertThat(applicationContext.containsBean("kafkaConnectivityHealthIndicator")).isTrue()
        assertThat(applicationContext.getBeansOfType(ProducerFailureTracker::class.java)).isNotEmpty()
    }

    @Test
    fun `reports consumers as healthy when nothing has fatally stopped`() {
        assertThat(kafkaConsumers.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `stays up when a consumer stops with the normal reason`() {
        val source = Any()

        applicationContext.publishEvent(ConsumerStoppedEvent(source, Any(), ConsumerStoppedEvent.Reason.NORMAL))

        assertThat(kafkaConsumers.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `goes down when a consumer stops with a fatal reason`() {
        val source = Any()

        applicationContext.publishEvent(ConsumerStoppedEvent(source, Any(), ConsumerStoppedEvent.Reason.AUTH))
        assertThat(kafkaConsumers.health().status).isEqualTo(Status.DOWN)

        // Compensate so later tests in this shared context start from a healthy state.
        applicationContext.publishEvent(ConsumerStoppedEvent(source, Any(), ConsumerStoppedEvent.Reason.NORMAL))
        assertThat(kafkaConsumers.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `reports connectivity as healthy against the embedded broker`() {
        // Guards against producer-failure state left behind by other tests in this shared context.
        Thread.sleep(PRODUCER_FAILURE_DECAY_MILLIS)

        assertThat(kafkaConnectivity.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `stays up when the producer has not failed`() {
        Thread.sleep(PRODUCER_FAILURE_DECAY_MILLIS)

        assertThat(kafkaConnectivity.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `stays up when producer failures are below the threshold`() {
        Thread.sleep(PRODUCER_FAILURE_DECAY_MILLIS)

        repeat(PRODUCER_FAILURE_THRESHOLD - 1) {
            producerFailureTracker.onError(TEST_PRODUCER_RECORD, null, RuntimeException("boom"))
        }

        assertThat(kafkaConnectivity.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `goes down when producer failures reach the threshold`() {
        Thread.sleep(PRODUCER_FAILURE_DECAY_MILLIS)

        repeat(PRODUCER_FAILURE_THRESHOLD) {
            producerFailureTracker.onError(TEST_PRODUCER_RECORD, null, RuntimeException("boom"))
        }

        assertThat(kafkaConnectivity.health().status).isEqualTo(Status.DOWN)
    }

    private companion object {
        // Longer than the 300ms failure-window above, so each producer test starts from a
        // clean slate regardless of what earlier tests left behind or the execution order.
        private const val PRODUCER_FAILURE_DECAY_MILLIS = 350L
        private const val PRODUCER_FAILURE_THRESHOLD = 5
        private val TEST_PRODUCER_RECORD = ProducerRecord<Any, Any>("test-topic", "key", "value")
    }
}

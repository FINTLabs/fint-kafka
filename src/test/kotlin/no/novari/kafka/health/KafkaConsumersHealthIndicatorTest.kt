package no.novari.kafka.health

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.SpringApplication
import org.springframework.boot.actuate.health.Status
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.kafka.event.ConsumerStoppedEvent
import org.springframework.kafka.listener.MessageListenerContainer
import java.time.Duration

class KafkaConsumersHealthIndicatorTest {
    private val consumer = Any()

    private fun emptyContainers(): ObjectProvider<MessageListenerContainer> = mock()

    @Test
    fun `is up when no consumer has stopped`() {
        val indicator = KafkaConsumersHealthIndicator(emptyContainers())

        assertThat(indicator.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `is down when a consumer stops with a fatal reason`() {
        val indicator = KafkaConsumersHealthIndicator(emptyContainers())

        indicator.onConsumerStopped(ConsumerStoppedEvent(consumer, Any(), ConsumerStoppedEvent.Reason.AUTH))

        assertThat(indicator.health().status).isEqualTo(Status.DOWN)
    }

    @Test
    fun `stays up when a consumer stops with the normal reason`() {
        val indicator = KafkaConsumersHealthIndicator(emptyContainers())

        indicator.onConsumerStopped(ConsumerStoppedEvent(consumer, Any(), ConsumerStoppedEvent.Reason.NORMAL))

        assertThat(indicator.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `recovers to up once the same consumer later stops with the normal reason`() {
        val indicator = KafkaConsumersHealthIndicator(emptyContainers())

        indicator.onConsumerStopped(ConsumerStoppedEvent(consumer, Any(), ConsumerStoppedEvent.Reason.ERROR))
        assertThat(indicator.health().status).isEqualTo(Status.DOWN)

        indicator.onConsumerStopped(ConsumerStoppedEvent(consumer, Any(), ConsumerStoppedEvent.Reason.NORMAL))
        assertThat(indicator.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `ignores a stopped container until the application is ready`() {
        val container =
            mock<MessageListenerContainer> {
                on { isAutoStartup } doReturn true
                on { isRunning } doReturn false
            }
        val containers = mock<ObjectProvider<MessageListenerContainer>>()
        whenever(containers.stream()).thenAnswer { listOf(container).stream() }

        val indicator = KafkaConsumersHealthIndicator(containers)

        assertThat(indicator.health().status).isEqualTo(Status.UP)

        indicator.onApplicationReady(applicationReadyEvent())

        assertThat(indicator.health().status).isEqualTo(Status.DOWN)
    }

    private fun applicationReadyEvent(): ApplicationReadyEvent =
        ApplicationReadyEvent(SpringApplication(), arrayOf(), mock<ConfigurableApplicationContext>(), Duration.ZERO)
}

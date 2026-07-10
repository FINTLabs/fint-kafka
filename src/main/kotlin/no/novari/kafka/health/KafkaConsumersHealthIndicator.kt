package no.novari.kafka.health

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.kafka.event.ConsumerStoppedEvent
import org.springframework.kafka.listener.MessageListenerContainer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class KafkaConsumersHealthIndicator(
    private val containers: ObjectProvider<MessageListenerContainer>,
) : HealthIndicator {
    private val fatalStops = ConcurrentHashMap<String, String>()
    private val applicationReady = AtomicBoolean(false)

    @EventListener
    fun onApplicationReady(event: ApplicationReadyEvent) {
        applicationReady.set(true)
    }

    @EventListener
    fun onConsumerStopped(event: ConsumerStoppedEvent) {
        val source = event.source.toString()
        val reason = event.reason
        if (reason == ConsumerStoppedEvent.Reason.NORMAL) {
            fatalStops.remove(source)
        } else {
            log.error("Kafka consumer stopped with fatal reason {} ({}); marking liveness DOWN", reason, source)
            fatalStops[source] = reason.name
        }
    }

    override fun health(): Health {
        if (fatalStops.isNotEmpty()) {
            return Health
                .down()
                .withDetail("reason", "Consumer container(s) fatally stopped")
                .withDetail("fatalStops", fatalStops.toMap())
                .build()
        }

        val stoppedContainers = stoppedAutoStartupContainers()
        if (stoppedContainers.isNotEmpty()) {
            return Health
                .down()
                .withDetail("reason", "Consumer container(s) not running")
                .withDetail("stoppedContainers", stoppedContainers)
                .build()
        }

        return Health.up().build()
    }

    private fun stoppedAutoStartupContainers(): List<String> {
        if (!applicationReady.get()) {
            return emptyList()
        }
        return containers
            .stream()
            .filter { it.isAutoStartup && !it.isRunning }
            .map { containerId(it) }
            .toList()
    }

    private fun containerId(container: MessageListenerContainer): String = container.listenerId ?: container.toString()

    private companion object {
        private val log = LoggerFactory.getLogger(KafkaConsumersHealthIndicator::class.java)
    }
}

package no.novari.kafka.health

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.ProducerListener
import java.time.Duration
import java.util.ArrayDeque

class ProducerFailureTracker(
    private val failureThreshold: Int,
    failureWindow: Duration,
) : ProducerListener<Any, Any> {
    private val windowMillis: Long = failureWindow.toMillis()
    private val failureTimestamps = ArrayDeque<Long>()

    override fun onError(
        producerRecord: ProducerRecord<Any, Any>?,
        recordMetadata: RecordMetadata?,
        exception: Exception?,
    ) {
        synchronized(failureTimestamps) {
            failureTimestamps.addLast(System.currentTimeMillis())
            prune()
        }
        log.warn("Kafka producer send failed for topic {}", producerRecord?.topic() ?: "?", exception)
    }

    fun isUnhealthy(): Boolean =
        synchronized(failureTimestamps) {
            prune()
            failureTimestamps.size >= failureThreshold
        }

    private fun prune() {
        val cutoff = System.currentTimeMillis() - windowMillis
        while (failureTimestamps.isNotEmpty() && failureTimestamps.peekFirst() < cutoff) {
            failureTimestamps.pollFirst()
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(ProducerFailureTracker::class.java)
    }
}

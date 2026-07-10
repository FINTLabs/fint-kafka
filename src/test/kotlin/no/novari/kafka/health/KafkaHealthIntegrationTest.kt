package no.novari.kafka.health

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.health.Status
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KafkaHealthIntegrationTest(
    @Autowired private val applicationContext: ApplicationContext,
    @Autowired private val kafkaConsumers: KafkaConsumersHealthIndicator,
    @Autowired private val kafkaConnectivity: KafkaConnectivityHealthIndicator,
) {
    @Test
    fun `registers the health indicators under their expected bean names`() {
        assertThat(applicationContext.containsBean("kafkaConsumers")).isTrue()
        assertThat(applicationContext.containsBean("kafkaConnectivity")).isTrue()
        assertThat(applicationContext.getBeansOfType(ProducerFailureTracker::class.java)).isNotEmpty()
    }

    @Test
    fun `reports consumers as healthy when nothing has fatally stopped`() {
        assertThat(kafkaConsumers.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `reports connectivity as healthy against the embedded broker`() {
        assertThat(kafkaConnectivity.health().status).isEqualTo(Status.UP)
    }
}

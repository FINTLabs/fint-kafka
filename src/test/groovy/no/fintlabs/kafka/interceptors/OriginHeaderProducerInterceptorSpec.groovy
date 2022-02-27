package no.fintlabs.kafka.interceptors

import no.fintlabs.kafka.KafkaConsumer
import no.fintlabs.kafka.OriginHeaderProducerInterceptor

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@SpringBootTest
@EmbeddedKafka
class OriginHeaderProducerInterceptorSpec extends Specification {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate

    @Autowired
    KafkaConsumer kafkaConsumer


    def 'kafka template should add origin application id header to producer record'() {
        given:
        kafkaTemplate.send("test-topic", "test-message")

        when:
        kafkaConsumer.getLatch().await(2, TimeUnit.SECONDS);

        then:
        kafkaConsumer.getLatch().getCount() == 0L
        kafkaConsumer.getPayload().value() == "test-message"
        kafkaConsumer.getPayload()
                .headers()
                .headers(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG)
                .size() == 1
        new String(kafkaConsumer.getPayload()
                .headers()
                .lastHeader(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG)
                .value(), StandardCharsets.UTF_8) == "test"
    }

}

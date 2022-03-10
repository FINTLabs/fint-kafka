package no.fintlabs.kafka.interceptors

import no.fintlabs.kafka.OriginHeaderProducerInterceptor
import no.fintlabs.kafka.common.FintListenerBeanRegistrationService
import no.fintlabs.kafka.common.FintListenerContainerFactoryService
import no.fintlabs.kafka.common.FintTemplateFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@SpringBootTest
@EmbeddedKafka
class OriginHeaderProducerInterceptorSpec extends Specification {

    @Autowired
    FintTemplateFactory fintTemplateFactory

    @Autowired
    FintListenerContainerFactoryService fintListenerContainerFactoryService

    @Autowired
    FintListenerBeanRegistrationService fintListenerBeanRegistrationService

    def 'kafka template should add origin application id header to producer record'() {
        given:
        KafkaTemplate<String, String> kafkaTemplate = fintTemplateFactory.createTemplate(String.class)
        CountDownLatch countDownLatch = new CountDownLatch(1)
        ArrayList<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>()
        def consumer =
                fintListenerContainerFactoryService.createListenerFactory(
                        String.class,
                        (consumerRecord) -> {
                            consumerRecords.add(consumerRecord)
                            countDownLatch.countDown()
                        },
                        false,
                        null
                ).createContainer("test-topic")
        fintListenerBeanRegistrationService.registerBean(consumer)

        when:
        kafkaTemplate.send("test-topic", "test-message")
        countDownLatch.await(10, TimeUnit.SECONDS);

        then:
        consumerRecords.size() == 1
        consumerRecords.get(0).value() == "test-message"
        consumerRecords.get(0)
                .headers()
                .headers(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG)
                .size() == 1
        new String(consumerRecords.get(0)
                .headers()
                .lastHeader(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG)
                .value(), StandardCharsets.UTF_8) == "test"
    }

}

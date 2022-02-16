package no.fintlabs.kafka.producer


import no.fintlabs.kafka.KafkaTestListener
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import spock.lang.Specification

import java.nio.charset.StandardCharsets

@SpringBootTest(properties = ["spring.main.allow-bean-definition-overriding=true"])
@ContextConfiguration(classes = Configuration.class)
@TestPropertySource(properties = ["spring.config.location = classpath:application-test.yaml"])
@EmbeddedKafka(brokerProperties = ['listeners=PLAINTEXT:${spring.kafka.bootstrap-servers}'], partitions = 1)
@DirtiesContext
class OriginHeaderProducerInterceptorSpec extends Specification {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate

    @Autowired
    KafkaTestListener kafkaTestListener

    @TestConfiguration
    static class Configuration {

        @Bean
        KafkaTestListener testListener() {
            return new KafkaTestListener();
        }

        @Bean
        ConcurrentMessageListenerContainer<String, String> testListenerContainer(
                ConsumerFactory<String, String> consumerFactory,
                KafkaTestListener testListener
        ) {
            ContainerProperties containerProperties = new ContainerProperties("test-topic")
            ConcurrentMessageListenerContainer<String, String> container =
                    new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties)
            container.getContainerProperties().setGroupId("testConsumerGroup")
            container.getContainerProperties().setMessageListener(testListener)
            container.start()
            return container

        }
    }

    def 'kafka template should add origin application id header to producer record'() {
        given:
        kafkaTestListener.trackNextRecords(1);
        when:
        kafkaTemplate.send("test-topic", "test-message")
        kafkaTestListener.waitForRecords()
        then:
        kafkaTestListener.consumedRecords.size() == 1
        def consumedRecord = kafkaTestListener.consumedRecords.get(0)
        consumedRecord.topic() == "test-topic"
        consumedRecord.value() == "test-message"
        consumedRecord.headers().headers(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG).size() == 1
        consumedRecord.headers().lastHeader(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG).value()
                == "test.application.id".getBytes(StandardCharsets.UTF_8)
    }

}

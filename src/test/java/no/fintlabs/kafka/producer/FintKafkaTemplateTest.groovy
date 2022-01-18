package no.fintlabs.kafka.producer

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import spock.lang.Specification

@SpringBootTest(properties = ["spring.main.allow-bean-definition-overriding=true"])
@TestPropertySource(properties = ["spring.config.location = classpath:application-test.yaml"])
@ContextConfiguration(classes = Configuration.class)
@DirtiesContext
class FintKafkaTemplateTest extends Specification {

    @Autowired
    FintKafkaTemplate fintKafkaTemplate

    @Autowired
    MockProducer<String, String> mockProducer

    @TestConfiguration
    static class Configuration {

        @Bean
        MockProducer<String, String> mockProducer() {
            return new MockProducer<String, String>(
                    false,
                    null,
                    new StringSerializer(),
                    new StringSerializer(),
            )
        }

        @Bean
        @Primary
        KafkaTemplate<String, String> kafkaTemplate(MockProducer<String, String> mockProducer) {
            return new FintKafkaTemplate(
                    new ProducerFactory<String, String>() {
                        @Override
                        Producer<String, String> createProducer() {
                            return mockProducer
                        }
                    },
                    "testApplicationId"
            )
        }
    }

    def 'should add application id to record headers'() {
        when:
        fintKafkaTemplate.send("test-topic", "test message")
        then:
        mockProducer.history().size() == 1
        mockProducer.history().get(0).topic() == "test-topic"
        mockProducer.history().get(0).value() == "test message"
        mockProducer.history().get(0).headers().headers("originApplicationId").size() == 1
        mockProducer.history().get(0).headers().lastHeader("originApplicationId").value() == "testApplicationId".getBytes()
    }

}

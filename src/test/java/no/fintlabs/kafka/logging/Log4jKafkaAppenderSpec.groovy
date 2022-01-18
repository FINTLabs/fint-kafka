package no.fintlabs.kafka.logging

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.Appender
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.parser.JsonLogEventParser
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import spock.lang.Specification

@SpringBootTest(properties = ["spring.main.allow-bean-definition-overriding=true"])
@ContextConfiguration(classes = Configuration.class)
@TestPropertySource(properties = [
        "spring.config.location = classpath:application-test.yaml",
        "fint.kafka.logging.logToKafka:true"
])
@EmbeddedKafka(
        brokerProperties = ['listeners=PLAINTEXT:${spring.kafka.bootstrap-servers}'],
        partitions = 1,
        topics = ['test-org-id.log']
)
@DirtiesContext
class Log4jKafkaAppenderSpec extends Specification {

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
            return new KafkaTemplate<String, String>(
                    new ProducerFactory<String, String>() {
                        @Override
                        Producer<String, String> createProducer() {
                            return mockProducer
                        }
                    }
            )
        }
    }

    def cleanupSpec() {
        Appender kafkaAppender = LoggerContext.getContext(false).getConfiguration().getAppender("Kafka")
        LoggerContext.getContext(false).getLoggers().forEach(logger -> {
            logger.removeAppender(kafkaAppender)
        })
        kafkaAppender.stop()
    }

    def 'should log on info level to kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("root").info("message content")

        then:
        mockProducer.history().size() == 1
        def sentValue = new JsonLogEventParser().parseFrom(mockProducer.history().get(0).value())
        sentValue.getLevel() == Level.INFO
        sentValue.getMessage().getFormattedMessage() == "message content"
    }

    def 'should log on warn level to kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("root").warn("message content")

        then:
        mockProducer.history().size() == 1
        def sentValue = new JsonLogEventParser().parseFrom(mockProducer.history().get(0).value())
        sentValue.getLevel() == Level.WARN
        sentValue.getMessage().getFormattedMessage() == "message content"
    }

    def 'should log on error level to kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("root").error("message content")

        then:
        mockProducer.history().size() == 1
        def sentValue = new JsonLogEventParser().parseFrom(mockProducer.history().get(0).value())
        sentValue.getLevel() == Level.ERROR
        sentValue.getMessage().getFormattedMessage() == "message content"
    }

    def 'should not log to kafka if logger name starts with org-apache-kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("org.apache.kafka").info("message content")

        then:
        mockProducer.history().size() == 0
    }

    def 'should not log on debug level to kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("root").debug("message content")

        then:
        mockProducer.history().size() == 0
    }
}

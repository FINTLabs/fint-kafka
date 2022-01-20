package no.fintlabs.kafka.logging


import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringSerializer
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
class LoggingConfigurationSpec extends Specification {

    @Autowired
    MockProducer<String, String> mockProducer

    @Autowired
    ObjectMapper objectMapper

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
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)
        Appender<ILoggingEvent> kafkaAppender = rootLogger.getAppender("Kafka")
        rootLogger.detachAppender(kafkaAppender)
        kafkaAppender.stop()
    }

    def 'should log on info level to kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("root").info("test message")

        then:
        mockProducer.history().size() == 1
        def sentValue = objectMapper.readValue(mockProducer.history().get(0).value(), LogEvent.class)
        sentValue.getLevel() == LogEvent.Level.INFO
        sentValue.getThreadName() == "main"
        sentValue.getMessage() == "test message"
    }

    def 'should log on warn level to kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("root").warn("test message")

        then:
        mockProducer.history().size() == 1
        def sentValue = objectMapper.readValue(mockProducer.history().get(0).value(), LogEvent.class)
        sentValue.getLevel() == LogEvent.Level.WARN
        sentValue.getThreadName() == "main"
        sentValue.getMessage() == "test message"
    }

    def 'should log on error level to kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("root").error("test message")

        then:
        mockProducer.history().size() == 1
        def sentValue = objectMapper.readValue(mockProducer.history().get(0).value(), LogEvent.class)
        sentValue.getLevel() == LogEvent.Level.ERROR
        sentValue.getThreadName() == "main"
        sentValue.getMessage() == "test message"
    }

    def 'should not log to kafka if logger name starts with org-apache-kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("org.apache.kafka").info("test message")

        then:
        mockProducer.history().size() == 0
    }

    def 'should not log on debug level to kafka'() {
        given:
        mockProducer.clear()

        when:
        LoggerFactory.getLogger("root").debug("test message")

        then:
        mockProducer.history().size() == 0
    }

}

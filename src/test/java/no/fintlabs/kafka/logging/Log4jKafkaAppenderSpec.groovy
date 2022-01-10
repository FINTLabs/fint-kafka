package no.fintlabs.kafka.logging

import no.fintlabs.kafka.KafkaTestListener
import no.fintlabs.kafka.topic.TopicNameService
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.Appender
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.parser.JsonLogEventParser
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
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
    TopicNameService topicNameService

    @Autowired
    KafkaTestListener testListener

    @TestConfiguration
    static class Configuration {

        @Bean
        KafkaTestListener testListener() {
            return new KafkaTestListener(1)
        }

        @Bean
        ConcurrentMessageListenerContainer<String, String> testListenerContainer(
                TopicNameService topicNameService,
                ConsumerFactory<String, String> consumerFactory,
                KafkaTestListener testListener
        ) {
            ContainerProperties containerProperties = new ContainerProperties(topicNameService.getLogTopicName())
            ConcurrentMessageListenerContainer<String, String> container =
                    new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties)
            container.getContainerProperties().setGroupId("testConsumerGroup")
            container.getContainerProperties().setMessageListener(testListener)
            container.start()
            return container
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
        testListener.trackNextRecords(1)
        when:
        LoggerFactory.getLogger("root").info("message content")
        testListener.waitForRecords()
        then:
        testListener.consumedRecords.size() == 1

        LogEvent logEvent = new JsonLogEventParser().parseFrom(testListener.consumedRecords.get(0).value())
        logEvent.getLevel() == Level.INFO
        logEvent.getMessage().getFormattedMessage() == "message content"
    }

    def 'should log on warn level to kafka'() {
        given:
        testListener.trackNextRecords(1)
        when:
        LoggerFactory.getLogger("root").warn("message content")
        testListener.waitForRecords()
        then:
        testListener.consumedRecords.size() == 1

        LogEvent logEvent = new JsonLogEventParser().parseFrom(testListener.consumedRecords.get(0).value())
        logEvent.getLevel() == Level.WARN
        logEvent.getMessage().getFormattedMessage() == "message content"
    }

    def 'should log on error level to kafka'() {
        given:
        testListener.trackNextRecords(1)
        when:
        LoggerFactory.getLogger("root").error("message content")
        testListener.waitForRecords()
        then:
        testListener.consumedRecords.size() == 1

        LogEvent logEvent = new JsonLogEventParser().parseFrom(testListener.consumedRecords.get(0).value())
        logEvent.getLevel() == Level.ERROR
        logEvent.getMessage().getFormattedMessage() == "message content"
    }

    def 'should not log to kafka if logger name starts with org-apache-kafka'() {
        given:
        testListener.trackNextRecords(1)
        when:
        LoggerFactory.getLogger("org.apache.kafka").info("message content")
        testListener.waitForRecords()
        then:
        def e = thrown(IllegalStateException)
        e.getMessage() == "Did not receive the expected number of records"
    }

    def 'should not log on debug level to kafka'() {
        given:
        testListener.trackNextRecords(1)
        when:
        LoggerFactory.getLogger("root").debug("message content")
        testListener.waitForRecords()
        then:
        def e = thrown(IllegalStateException)
        e.getMessage() == "Did not receive the expected number of records"
    }
}

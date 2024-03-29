package no.fintlabs.kafka

import no.fintlabs.kafka.common.ListenerBeanRegistrationService
import no.fintlabs.kafka.entity.EntityConsumerConfiguration
import no.fintlabs.kafka.entity.EntityConsumerFactoryService
import no.fintlabs.kafka.entity.EntityProducerFactory
import no.fintlabs.kafka.entity.EntityProducerRecord
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import no.fintlabs.kafka.event.EventConsumerConfiguration
import no.fintlabs.kafka.event.EventConsumerFactoryService
import no.fintlabs.kafka.event.EventProducerFactory
import no.fintlabs.kafka.event.EventProducerRecord
import no.fintlabs.kafka.event.error.*
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.kafka.requestreply.*
import no.fintlabs.kafka.requestreply.topic.ReplyTopicNameParameters
import no.fintlabs.kafka.requestreply.topic.RequestTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@SpringBootTest
@EmbeddedKafka
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ProducerConsumerIntegrationSpec extends Specification {

    @Autowired
    EventProducerFactory fintKafkaEventProducerFactory
    @Autowired
    EventConsumerFactoryService fintKafkaEventConsumerFactory

    @Autowired
    ErrorEventProducer errorEventProducer
    @Autowired
    ErrorEventConsumerFactoryService fintKafkaErrorEventConsumerFactory

    @Autowired
    EntityProducerFactory fintKafkaEntityProducerFactory
    @Autowired
    EntityConsumerFactoryService fintKafkaEntityConsumerFactory

    @Autowired
    RequestProducerFactory fintKafkaRequestProducerFactory
    @Autowired
    RequestConsumerFactoryService fintKafkaRequestConsumerFactory

    @Autowired
    ListenerBeanRegistrationService fintListenerBeanRegistrationService

    private static class TestObject {
        private Integer integer
        private String string

        Integer getInteger() {
            return integer
        }

        void setInteger(Integer integer) {
            this.integer = integer
        }

        String getString() {
            return string
        }

        void setString(String string) {
            this.string = string
        }

        boolean equals(o) {
            if (this.is(o)) return true
            if (getClass() != o.class) return false

            TestObject that = (TestObject) o

            if (integer != that.integer) return false
            if (string != that.string) return false

            return true
        }

        int hashCode() {
            int result
            result = (integer != null ? integer.hashCode() : 0)
            result = 31 * result + (string != null ? string.hashCode() : 0)
            return result
        }
    }

    def 'event'() {
        given:
        CountDownLatch eventCDL = new CountDownLatch(1)
        ArrayList<ConsumerRecord<String, TestObject>> consumedEvents = new ArrayList<>()
        def eventProducer = fintKafkaEventProducerFactory.createProducer(TestObject.class)
        def eventConsumer = fintKafkaEventConsumerFactory.createRecordConsumerFactory(
                TestObject.class,
                (consumerRecord) -> {
                    consumedEvents.add(consumerRecord)
                    eventCDL.countDown()
                },
                EventConsumerConfiguration.empty()
        ).createContainer(EventTopicNameParameters.builder().eventName("event").build())
        fintListenerBeanRegistrationService.registerBean(eventConsumer)

        when:
        TestObject testObject = new TestObject()
        testObject.setInteger(2)
        testObject.setString("testObjectString")
        eventProducer.send(
                EventProducerRecord.builder()
                        .topicNameParameters(EventTopicNameParameters.builder()
                                .eventName("event")
                                .build())
                        .value(testObject)
                        .build()
        )

        eventCDL.await(10, TimeUnit.SECONDS)

        then:
        consumedEvents.size() == 1
        consumedEvents.get(0).value() == testObject
    }

    def 'error event'() {
        given:
        CountDownLatch eventCDL = new CountDownLatch(1)
        ArrayList<ConsumerRecord<String, ErrorCollection>> consumedEvents = new ArrayList<>()
        def eventConsumer = fintKafkaErrorEventConsumerFactory.createRecordConsumerFactory(
                (consumerRecord) -> {
                    consumedEvents.add(consumerRecord)
                    eventCDL.countDown()
                },
                ErrorEventConsumerConfiguration.empty()
        ).createContainer(ErrorEventTopicNameParameters.builder().errorEventName("event").build())
        fintListenerBeanRegistrationService.registerBean(eventConsumer)

        when:
        ErrorCollection errorCollection = new ErrorCollection(List.of(
                Error.builder()
                        .errorCode("ERROR_CODE_1")
                        .args(Map.of("arg1", "argValue1", "arg2", "argValue2"))
                        .build(),
                Error.builder()
                        .errorCode("ERROR_CODE_2")
                        .args(Map.of("arg1", "argValue1", "arg2", "argValue2"))
                        .build(),
                Error.builder()
                        .errorCode("ERROR_CODE_3")
                        .args(Map.of("arg1", "argValue1", "arg2", "argValue2"))
                        .build()
        ))

        errorEventProducer.send(
                ErrorEventProducerRecord
                        .builder()
                        .topicNameParameters(ErrorEventTopicNameParameters.builder()
                                .errorEventName("event")
                                .build())
                        .errorCollection(errorCollection)
                        .build()
        )

        eventCDL.await(10, TimeUnit.SECONDS)

        then:
        consumedEvents.size() == 1
        consumedEvents.get(0).value() == errorCollection
    }

    def 'entity'() {
        given:
        CountDownLatch entityCDL = new CountDownLatch(1)
        ArrayList<ConsumerRecord<String, String>> consumedEntities = new ArrayList<>()
        def entityProducer = fintKafkaEntityProducerFactory.createProducer(String.class)
        def entityConsumer = fintKafkaEntityConsumerFactory.createRecordConsumerFactory(
                String.class,
                (consumerRecord) -> {
                    consumedEntities.add(consumerRecord)
                    entityCDL.countDown()
                },
                EntityConsumerConfiguration.empty()
        ).createContainer(EntityTopicNameParameters.builder().resource("resource").build())
        fintListenerBeanRegistrationService.registerBean(entityConsumer)

        when:
        entityProducer.send(
                EntityProducerRecord.builder()
                        .topicNameParameters(
                                EntityTopicNameParameters.builder()
                                        .resource("resource")
                                        .build()
                        )
                        .value("valueString")
                        .build()
        )

        entityCDL.await(10, TimeUnit.SECONDS)

        then:
        consumedEntities.size() == 1
        consumedEntities.get(0).value() == "valueString"
    }

    def 'request reply'() {
        given:
        def requestProducer = fintKafkaRequestProducerFactory.createProducer(
                ReplyTopicNameParameters.builder()
                        .applicationId("application")
                        .resource("resource")
                        .build(),
                String.class,
                Integer.class,
                RequestProducerConfiguration
                        .builder()
                        .defaultReplyTimeout(Duration.ofSeconds(10))
                        .build()
        )

        def requestConsumer = fintKafkaRequestConsumerFactory.createRecordConsumerFactory(
                String.class,
                Integer.class,
                (consumerRecord) -> ReplyProducerRecord.builder().value(32).build()
        ).createContainer(RequestTopicNameParameters.builder().resource("resource").build())
        fintListenerBeanRegistrationService.registerBean(requestConsumer)

        when:
        Optional<ConsumerRecord<String, Integer>> reply = requestProducer.requestAndReceive(
                RequestProducerRecord.builder()
                        .topicNameParameters(RequestTopicNameParameters.builder()
                                .resource("resource")
                                .build())
                        .value("requestValueString")
                        .build()
        )

        then:
        reply.isPresent()
        reply.get().value() == 32
    }

    def 'request reply long processing time'() {
        given:
        def requestProducer = fintKafkaRequestProducerFactory.createProducer(
                ReplyTopicNameParameters.builder()
                        .applicationId("application")
                        .resource("resource")
                        .build(),
                String.class,
                Integer.class,
                RequestProducerConfiguration
                        .builder()
                        .defaultReplyTimeout(Duration.ofMinutes(20))
                        .build()
        )

        def requestConsumer = fintKafkaRequestConsumerFactory.createRecordConsumerFactory(
                String.class,
                Integer.class,
                (consumerRecord) -> {
                    Thread.sleep(10000)
                    return ReplyProducerRecord.builder().value(32).build()
                },
                RequestConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(15000)
                        .build()
        ).createContainer(RequestTopicNameParameters.builder().resource("resource").build())
        fintListenerBeanRegistrationService.registerBean(requestConsumer)

        when:
        Optional<ConsumerRecord<String, Integer>> reply = requestProducer.requestAndReceive(
                RequestProducerRecord.builder()
                        .topicNameParameters(RequestTopicNameParameters.builder()
                                .resource("resource")
                                .build())
                        .value("requestValueString")
                        .build()
        )

        then:
        reply.isPresent()
        reply.get().value() == 32
    }

}

package no.fintlabs.kafka

import no.fintlabs.kafka.common.FintListenerBeanRegistrationService
import no.fintlabs.kafka.entity.EntityProducerRecord
import no.fintlabs.kafka.entity.FintKafkaEntityConsumerFactory
import no.fintlabs.kafka.entity.FintKafkaEntityProducerFactory
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import no.fintlabs.kafka.event.EventProducerRecord
import no.fintlabs.kafka.event.FintKafkaEventConsumerFactory
import no.fintlabs.kafka.event.FintKafkaEventProducerFactory
import no.fintlabs.kafka.event.error.*
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.kafka.requestreply.FintKafkaRequestConsumerFactory
import no.fintlabs.kafka.requestreply.FintKafkaRequestProducerFactory
import no.fintlabs.kafka.requestreply.ReplyProducerRecord
import no.fintlabs.kafka.requestreply.RequestProducerRecord
import no.fintlabs.kafka.requestreply.topic.ReplyTopicNameParameters
import no.fintlabs.kafka.requestreply.topic.RequestTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@SpringBootTest
@EmbeddedKafka
class ProducerConsumerIntegrationSpec extends Specification {

    @Autowired
    FintKafkaEventProducerFactory fintKafkaEventProducerFactory
    @Autowired
    FintKafkaEventConsumerFactory fintKafkaEventConsumerFactory

    @Autowired
    ErrorEventProducer errorEventProducer
    @Autowired
    FintKafkaErrorEventConsumerFactory fintKafkaErrorEventConsumerFactory

    @Autowired
    FintKafkaEntityProducerFactory fintKafkaEntityProducerFactory
    @Autowired
    FintKafkaEntityConsumerFactory fintKafkaEntityConsumerFactory

    @Autowired
    FintKafkaRequestProducerFactory fintKafkaRequestProducerFactory
    @Autowired
    FintKafkaRequestConsumerFactory fintKafkaRequestConsumerFactory

    @Autowired
    FintListenerBeanRegistrationService fintListenerBeanRegistrationService

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
        def eventConsumer = fintKafkaEventConsumerFactory.createConsumer(
                TestObject.class,
                (consumerRecord) -> {
                    consumedEvents.add(consumerRecord)
                    eventCDL.countDown()
                },
                null,
                false
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
        def eventConsumer = fintKafkaErrorEventConsumerFactory.createConsumer(
                (consumerRecord) -> {
                    consumedEvents.add(consumerRecord)
                    eventCDL.countDown()
                },
                null,
                false
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
        def entityConsumer = fintKafkaEntityConsumerFactory.createConsumer(
                String.class,
                (consumerRecord) -> {
                    consumedEntities.add(consumerRecord)
                    entityCDL.countDown()
                },
                null
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
                Integer.class
        )

        def requestConsumer = fintKafkaRequestConsumerFactory.createConsumer(
                String.class,
                Integer.class,
                (consumerRecord) -> ReplyProducerRecord.builder().value(32).build(),
                null
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

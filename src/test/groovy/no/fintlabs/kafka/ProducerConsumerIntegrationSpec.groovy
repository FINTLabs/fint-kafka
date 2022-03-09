package no.fintlabs.kafka

import no.fintlabs.kafka.common.FintListenerBeanRegistrationService
import no.fintlabs.kafka.entity.EntityTopicNameParameters
import no.fintlabs.kafka.entity.FintKafkaEntityConsumerFactory
import no.fintlabs.kafka.entity.FintKafkaEntityProducerFactory
import no.fintlabs.kafka.event.EventTopicNameParameters
import no.fintlabs.kafka.event.FintKafkaEventConsumerFactory
import no.fintlabs.kafka.event.FintKafkaEventProducerFactory
import no.fintlabs.kafka.requestreply.FintKafkaRequestConsumerFactory
import no.fintlabs.kafka.requestreply.FintKafkaRequestProducerFactory
import no.fintlabs.kafka.requestreply.ReplyTopicNameParameters
import no.fintlabs.kafka.requestreply.RequestTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
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
    TopicNameService topicNameService;

    @Autowired
    FintKafkaEventProducerFactory fintKafkaEventProducerFactory
    @Autowired
    FintKafkaEventConsumerFactory fintKafkaEventConsumerFactory

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
                EventTopicNameParameters.builder().orgId("orgId").domainContext("context").eventName("event").build(),
                TestObject.class,
                (consumerRecord) -> {
                    consumedEvents.add(consumerRecord)
                    eventCDL.countDown()
                },
                null
        )
        fintListenerBeanRegistrationService.registerBean(eventConsumer)

        when:
        TestObject testObject = new TestObject()
        testObject.setInteger(2)
        testObject.setString("testObjectString")
        eventProducer.send(new ProducerRecord<String, TestObject>(
                topicNameService.generateEventTopicName(EventTopicNameParameters.builder().orgId("orgId").domainContext("context").eventName("event").build()),
                testObject
        ))

        eventCDL.await(10, TimeUnit.SECONDS)

        then:
        consumedEvents.size() == 1
        consumedEvents.get(0).value() == testObject
    }

    def 'entity'() {
        given:
        CountDownLatch entityCDL = new CountDownLatch(1)
        ArrayList<ConsumerRecord<String, String>> consumedEntities = new ArrayList<>()
        def entityProducer = fintKafkaEntityProducerFactory.createProducer(String.class)
        def entityConsumer = fintKafkaEntityConsumerFactory.createConsumer(
                EntityTopicNameParameters.builder().orgId("orgId").domainContext("context").resource("resource").build(),
                String.class,
                (consumerRecord) -> {
                    consumedEntities.add(consumerRecord)
                    entityCDL.countDown()
                },
                null
        )
        fintListenerBeanRegistrationService.registerBean(entityConsumer)

        when:
        entityProducer.send(new ProducerRecord<String, String>(
                topicNameService.generateEntityTopicName(EntityTopicNameParameters.builder().orgId("orgId").domainContext("context").resource("resource").build()),
                "valueString"
        ))

        entityCDL.await(10, TimeUnit.SECONDS)

        then:
        consumedEntities.size() == 1
        consumedEntities.get(0).value() == "valueString"
    }

    def 'request'() {
        given:
        def requestProducer = fintKafkaRequestProducerFactory.createProducer(
                ReplyTopicNameParameters.builder().orgId("orgId").domainContext("context").resource("resource").build(),
                String.class,
                Integer.class
        )

        def requestConsumer = fintKafkaRequestConsumerFactory.createConsumer(
                RequestTopicNameParameters.builder().orgId("orgId").domainContext("context").resource("resource").build(),
                String.class,
                Integer.class,
                (consumerRecord) -> 32,
                null
        )
        fintListenerBeanRegistrationService.registerBean(requestConsumer)

        when:
        Optional<ConsumerRecord<String, Integer>> reply = requestProducer.requestAndReceive(new ProducerRecord<String, String>(
                topicNameService.generateRequestTopicName(RequestTopicNameParameters.builder().orgId("orgId").domainContext("context").resource("resource").build()),
                "requestValueString"
        ))

        then:
        reply.isPresent()
        reply.get().value() == 32
    }

}

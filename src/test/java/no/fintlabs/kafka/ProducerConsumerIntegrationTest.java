package no.fintlabs.kafka;


import lombok.*;
import no.fintlabs.kafka.consuming.ListenerConfiguration;
import no.fintlabs.kafka.consuming.ListenerContainerFactoryService;
import no.fintlabs.kafka.consuming.ParameterizedListenerContainerFactoryService;
import no.fintlabs.kafka.consuming.RequestListenerContainerFactory;
import no.fintlabs.kafka.model.Error;
import no.fintlabs.kafka.model.*;
import no.fintlabs.kafka.producing.ParameterizedTemplate;
import no.fintlabs.kafka.producing.ParameterizedTemplateFactory;
import no.fintlabs.kafka.producing.RequestTemplate;
import no.fintlabs.kafka.producing.RequestTemplateFactory;
import no.fintlabs.kafka.topic.name.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ProducerConsumerIntegrationTest {

    @Autowired
    ParameterizedTemplateFactory parameterizedTemplateFactory;

    @Autowired
    ParameterizedListenerContainerFactoryService parameterizedListenerContainerFactoryService;

    @Autowired
    RequestTemplateFactory requestTemplateFactory;

    @Autowired
    RequestListenerContainerFactory requestListenerContainerFactory;
    @Autowired
    private ListenerContainerFactoryService listenerContainerFactoryService;

    @Setter
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    private static class TestObject {
        private Integer integer;
        private String string;
    }

    @Test
    void event() throws InterruptedException {
        CountDownLatch eventCDL = new CountDownLatch(1);
        ArrayList<ConsumerRecord<String, TestObject>> consumedRecords = new ArrayList<>();

        EventTopicNameParameters eventTopicNameParameters = EventTopicNameParameters.builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .builder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .eventName("test-event-name")
                .build();

        ConcurrentMessageListenerContainer<String, TestObject> listenerContainer =
                parameterizedListenerContainerFactoryService.createRecordListenerContainerFactory(
                        TestObject.class,
                        consumerRecord -> {
                            consumedRecords.add(consumerRecord);
                            eventCDL.countDown();
                        },
                        ListenerConfiguration.builder().build()
                ).createContainer(eventTopicNameParameters);

        ParameterizedTemplate<TestObject> parameterizedTemplate = parameterizedTemplateFactory.createTemplate(TestObject.class);
        parameterizedTemplate.send(
                ParameterizedProducerRecord
                        .<TestObject>builder()
                        .topicNameParameters(eventTopicNameParameters)
                        .key("test-key")
                        .value(new TestObject(2, "testObjectString"))
                        .build()
        );

        listenerContainer.start();

        assertThat(eventCDL.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(consumedRecords).hasSize(1);
        ConsumerRecord<String, TestObject> consumedRecord = consumedRecords.get(0);
        assertThat(consumedRecord.topic()).isEqualTo("test-org-id.test-domain-context.event.test-event-name");
        assertThat(consumedRecord.key()).isEqualTo("testKey");
        assertThat(consumedRecord.value()).isEqualTo(new TestObject(2, "testObjectString"));
    }

    @Test
    void errorEvent() throws InterruptedException {
        CountDownLatch eventCDL = new CountDownLatch(1);
        ArrayList<ConsumerRecord<String, ErrorCollection>> consumedRecords = new ArrayList<>();

        ErrorEventTopicNameParameters errorEventTopicNameParameters = ErrorEventTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .builder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .errorEventName("test-error-event-name")
                .build();

        ConcurrentMessageListenerContainer<String, ErrorCollection> listenerContainer =
                parameterizedListenerContainerFactoryService.createRecordListenerContainerFactory(
                        ErrorCollection.class,
                        consumerRecord -> {
                            consumedRecords.add(consumerRecord);
                            eventCDL.countDown();
                        },
                        ListenerConfiguration.builder().build()
                ).createContainer(errorEventTopicNameParameters);

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
        ));

        ParameterizedTemplate<ErrorCollection> template = parameterizedTemplateFactory.createTemplate(ErrorCollection.class);
        template.send(
                ParameterizedProducerRecord
                        .<ErrorCollection>builder()
                        .topicNameParameters(errorEventTopicNameParameters)
                        .key("test-key")
                        .value(errorCollection)
                        .build()
        );

        listenerContainer.start();

        assertThat(eventCDL.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(consumedRecords).hasSize(1);
        ConsumerRecord<String, ErrorCollection> consumedRecord = consumedRecords.get(0);
        assertThat(consumedRecord.topic()).isEqualTo("test-org-id.test-domain-context.error-event.test-error-event-name");
        assertThat(consumedRecord.key()).isEqualTo("testKey");
        assertThat(consumedRecord.value()).isEqualTo(errorCollection);
    }

    @Test
    void entity() throws InterruptedException {
        CountDownLatch entityCDL = new CountDownLatch(1);
        ArrayList<ConsumerRecord<String, TestObject>> consumedRecords = new ArrayList<>();

        EntityTopicNameParameters entityTopicNameParameters = EntityTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .builder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .resourceName("test-resource-name")
                .build();

        ConcurrentMessageListenerContainer<String, TestObject> listenerContainer =
                parameterizedListenerContainerFactoryService.createRecordListenerContainerFactory(
                        TestObject.class,
                        (consumerRecord) -> {
                            consumedRecords.add(consumerRecord);
                            entityCDL.countDown();
                        },
                        ListenerConfiguration.builder().build()
                ).createContainer(entityTopicNameParameters);

        ParameterizedTemplate<TestObject> parameterizedTemplate =
                parameterizedTemplateFactory.createTemplate(TestObject.class);
        parameterizedTemplate.send(
                ParameterizedProducerRecord
                        .<TestObject>builder()
                        .topicNameParameters(entityTopicNameParameters)
                        .key("test-key")
                        .value(new TestObject(2, "testObjectString"))
                        .build()
        );

        listenerContainer.start();

        assertThat(entityCDL.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(consumedRecords).hasSize(1);
        ConsumerRecord<String, TestObject> consumedRecord = consumedRecords.get(0);
        assertThat(consumedRecord.topic()).isEqualTo("test-org-id.test-domain-context.entity.test-resource-name");
        assertThat(consumedRecord.key()).isEqualTo("testKey");
        assertThat(consumedRecord.value()).isEqualTo(new TestObject(2, "testObjectString"));
    }

    @Test
    void requestReply() {
        RequestTopicNameParameters requestTopicNameParameters = RequestTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .builder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .resourceName("test-resource-name")
                .parameterName("test-parameter-name")
                .build();

        ReplyTopicNameParameters replyTopicNameParameters = ReplyTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .builder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .applicationId("test-application-id")
                .resourceName("test-resource-name")
                .build();

        RequestTemplate<Integer, TestObject> requestTemplate = requestTemplateFactory.createTemplate(
                replyTopicNameParameters,
                Integer.class,
                TestObject.class
        );

        List<ConsumerRecord<String, Integer>> consumedRequests = new ArrayList<>();

        ConcurrentMessageListenerContainer<String, Integer> requestListenerContainer =
                requestListenerContainerFactory.createRecordConsumerFactory(
                        requestTopicNameParameters,
                        Integer.class,
                        TestObject.class,
                        consumerRecord -> {
                            consumedRequests.add(consumerRecord);
                            return ReplyProducerRecord
                                    .<TestObject>builder()
                                    .value(new TestObject(2, "testObjectString"))
                                    .build();
                        }
                );

        requestListenerContainer.start();

        Optional<ConsumerRecord<String, TestObject>> reply = requestTemplate.requestAndReceive(
                RequestProducerRecord.<Integer>builder()
                        .topicNameParameters(requestTopicNameParameters)
                        .key("testKey")
                        .value(4)
                        .build()
        );

        assertThat(consumedRequests).hasSize(1);
        ConsumerRecord<String, Integer> consumedRequest = consumedRequests.get(0);
        assertThat(consumedRequest.topic()).isEqualTo(
                "test-org-id.test-domain-context.request.test-resource-name.by.test-parameter-name"
        );
        assertThat(consumedRequest.key()).isEqualTo("testKey");
        assertThat(consumedRequest.value()).isEqualTo(4);

        assertThat(reply).isPresent();
        ConsumerRecord<String, TestObject> consumedReply = reply.get();
        assertThat(consumedReply.topic()).isEqualTo(
                "test-org-id.test-domain-context.reply.test-application-id.test-resource-name"
        );
        assertThat(consumedReply.key()).isEqualTo("testKey");
        assertThat(consumedReply.value()).isEqualTo(new TestObject(2, "testObjectString"));
    }

    @Test
    void givenReplyThatTakesLongerThanRequestTimeoutWhenRequestShouldReturnOptionalEmpty() throws InterruptedException {
        RequestTopicNameParameters requestTopicNameParameters = RequestTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .builder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .resourceName("test-resource-name")
                .parameterName("test-parameter-name")
                .build();

        ReplyTopicNameParameters replyTopicNameParameters = ReplyTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .builder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .applicationId("test-application-id")
                .resourceName("test-resource-name")
                .build();

        RequestTemplate<Integer, TestObject> requestTemplate = requestTemplateFactory.createTemplate(
                replyTopicNameParameters,
                Integer.class,
                TestObject.class,
                Duration.ofMillis(100)
        );

        List<ConsumerRecord<String, Integer>> consumedRequests = new ArrayList<>();
        CountDownLatch replyLatch = new CountDownLatch(1);

        ConcurrentMessageListenerContainer<String, Integer> requestListenerContainer =
                requestListenerContainerFactory.createRecordConsumerFactory(
                        requestTopicNameParameters,
                        Integer.class,
                        TestObject.class,
                        consumerRecord -> {
                            consumedRequests.add(consumerRecord);
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            replyLatch.countDown();
                            return ReplyProducerRecord
                                    .<TestObject>builder()
                                    .value(new TestObject(2, "testObjectString"))
                                    .build();
                        }
                );

        Optional<ConsumerRecord<String, TestObject>> reply = requestTemplate.requestAndReceive(
                RequestProducerRecord.<Integer>builder()
                        .topicNameParameters(requestTopicNameParameters)
                        .key("testKey")
                        .value(4)
                        .build()
        );

        requestListenerContainer.start();

        assertThat(replyLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(consumedRequests).hasSize(1);
        ConsumerRecord<String, Integer> consumedRequest = consumedRequests.get(0);
        assertThat(consumedRequest.topic()).isEqualTo(
                "test-org-id.test-domain-context.request.test-resource-name.by.test-parameter-name"
        );
        assertThat(consumedRequest.key()).isEqualTo("testKey");
        assertThat(consumedRequest.value()).isEqualTo(4);

        assertThat(reply).isNotPresent();
    }
//
//    def 'request reply long processing time'()
//
//    {
//        given:
//        def requestProducer = fintKafkaRequestProducerFactory.createTemplate(
//                ReplyTopicNameParameters.builder()
//                        .applicationId("application")
//                        .resource("resource")
//                        .build(),
//                String.class,
//                Integer.class,
//                RequestTemplateConfiguration
//                        .builder()
//                        .defaultReplyTimeout(Duration.ofMinutes(20))
//                        .build()
//        )
//
//        def requestConsumer = fintKafkaRequestConsumerFactory.createRecordConsumerFactory(
//                String.class,
//                Integer.class,
//                (consumerRecord) -> {
//                    Thread.sleep(10000)
//                    return ReplyProducerRecord.builder().value(32).build()
//                },
//                RequestListenerConfiguration
//                        .builder()
//                        .maxPollIntervalMs(15000)
//                        .build()
//        ).createContainer(RequestTopicNameParameters.builder().resource("resource").build())
//        fintListenerBeanRegistrationService.registerBean(requestConsumer)
//
//        when:
//        Optional<ConsumerRecord<String, Integer>> reply = requestProducer.requestAndReceive(
//                RequestProducerRecord.builder()
//                        .topicNameParameters(RequestTopicNameParameters.builder()
//                                .resource("resource")
//                                .build())
//                        .value("requestValueString")
//                        .build()
//        )
//
//        then:
//        reply.isPresent()
//        reply.get().value() == 32
//    }

}

package no.novari.kafka;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import no.novari.kafka.consuming.ErrorHandlerFactory;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService;
import no.novari.kafka.producing.ParameterizedProducerRecord;
import no.novari.kafka.producing.ParameterizedTemplate;
import no.novari.kafka.producing.ParameterizedTemplateFactory;
import no.novari.kafka.topic.name.EntityTopicNameParameters;
import no.novari.kafka.topic.name.ErrorEventTopicNameParameters;
import no.novari.kafka.topic.name.EventTopicNameParameters;
import no.novari.kafka.topic.name.EventTopicNamePatternParameters;
import no.novari.kafka.topic.name.TopicNameParameters;
import no.novari.kafka.topic.name.TopicNamePatternParameterPattern;
import no.novari.kafka.topic.name.TopicNamePatternPrefixParameters;
import no.novari.kafka.topic.name.TopicNamePrefixParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ProducerConsumerIntegrationTest {

    ParameterizedTemplateFactory parameterizedTemplateFactory;
    ParameterizedListenerContainerFactoryService parameterizedListenerContainerFactoryService;
    ErrorHandlerFactory errorHandlerFactory;

    public ProducerConsumerIntegrationTest(
            @Autowired ParameterizedTemplateFactory parameterizedTemplateFactory,
            @Autowired ParameterizedListenerContainerFactoryService parameterizedListenerContainerFactoryService,
            @Autowired ErrorHandlerFactory errorHandlerFactory
    ) {
        this.parameterizedTemplateFactory = parameterizedTemplateFactory;
        this.parameterizedListenerContainerFactoryService = parameterizedListenerContainerFactoryService;
        this.errorHandlerFactory = errorHandlerFactory;
    }

    @Setter
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    private static class TestObject {
        private Integer integer;
        private String string;
    }

    static Stream<Arguments> testTopicNameParameters() {
        return Stream.of(
                Arguments.of(
                        EventTopicNameParameters
                                .builder()
                                .topicNamePrefixParameters(
                                        TopicNamePrefixParameters
                                                .stepBuilder()
                                                .orgId("test-org-id")
                                                .domainContext("test-domain-context")
                                                .build()
                                )
                                .eventName("test-event-name")
                                .build(),
                        "test-org-id.test-domain-context.event.test-event-name"
                ),
                Arguments.of(
                        ErrorEventTopicNameParameters
                                .builder()
                                .topicNamePrefixParameters(
                                        TopicNamePrefixParameters
                                                .stepBuilder()
                                                .orgId("test-org-id")
                                                .domainContext("test-domain-context")
                                                .build()
                                )
                                .errorEventName("test-error-event-name")
                                .build(),
                        "test-org-id.test-domain-context.event.error.test-error-event-name"
                ),
                Arguments.of(
                        EntityTopicNameParameters
                                .builder()
                                .topicNamePrefixParameters(
                                        TopicNamePrefixParameters
                                                .stepBuilder()
                                                .orgId("test-org-id")
                                                .domainContext("test-domain-context")
                                                .build()
                                )
                                .resourceName("test-resource-name")
                                .build(),
                        "test-org-id.test-domain-context.entity.test-resource-name"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("testTopicNameParameters")
    void name(TopicNameParameters topicNameParameters, String expectedTopicName) throws InterruptedException {
        CountDownLatch entityCDL = new CountDownLatch(1);
        ArrayList<ConsumerRecord<String, TestObject>> consumedRecords = new ArrayList<>();

        ConcurrentMessageListenerContainer<String, TestObject> listenerContainer =
                parameterizedListenerContainerFactoryService
                        .createRecordListenerContainerFactory(
                                TestObject.class,
                                (consumerRecord) -> {
                                    consumedRecords.add(consumerRecord);
                                    entityCDL.countDown();
                                },
                                ListenerConfiguration
                                        .stepBuilder()
                                        .groupIdApplicationDefault()
                                        .maxPollRecordsKafkaDefault()
                                        .maxPollIntervalKafkaDefault()
                                        .continueFromPreviousOffsetOnAssignment()
                                        .build(),
                                null
                        )
                        .createContainer(topicNameParameters);

        ParameterizedTemplate<TestObject> parameterizedTemplate =
                parameterizedTemplateFactory.createTemplate(TestObject.class);
        parameterizedTemplate.send(
                ParameterizedProducerRecord
                        .<TestObject>builder()
                        .topicNameParameters(topicNameParameters)
                        .key("test-key")
                        .value(new TestObject(2, "testObjectString"))
                        .build()
        );

        listenerContainer.start();

        assertThat(entityCDL.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(consumedRecords).hasSize(1);
        ConsumerRecord<String, TestObject> consumedRecord = consumedRecords.getFirst();
        assertThat(consumedRecord.topic())
                .isEqualTo(expectedTopicName);
        assertThat(consumedRecord.key()).isEqualTo("test-key");
        assertThat(consumedRecord.value()).isEqualTo(new TestObject(2, "testObjectString"));
    }

    @Test
    void pattern() throws InterruptedException {
        CountDownLatch eventCDL = new CountDownLatch(2);
        ArrayList<ConsumerRecord<String, TestObject>> consumedRecords = new ArrayList<>();


        EventTopicNameParameters eventTopicNameParameters1 = EventTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id-3")
                                .domainContext("test-domain-context-2")
                                .build()
                )
                .eventName("test-event-name-2")
                .build();


        EventTopicNameParameters eventTopicNameParameters2 = EventTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id-2")
                                .domainContext(
                                        "test-domain-context-3")
                                .build()
                )
                .eventName("test-event-name-2")
                .build();

        EventTopicNameParameters eventTopicNameParameters3 = EventTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id-2")
                                .domainContext("test-domain-context-3")
                                .build()
                )
                .eventName("test-event-name-3")
                .build();

        EventTopicNameParameters eventTopicNameParameters4 = EventTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id-1")
                                .domainContext("test-domain-context-1")
                                .build()
                )
                .eventName("test-event-name-1")
                .build();
        EventTopicNameParameters eventTopicNameParameters5 = EventTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id-2")
                                .domainContext(
                                        "test-domain-context-2")
                                .build()
                )
                .eventName("test-event-name-2")
                .build();

        EventTopicNamePatternParameters eventTopicNamePatternParameters = EventTopicNamePatternParameters
                .builder()
                .topicNamePatternPrefixParameters(
                        TopicNamePatternPrefixParameters
                                .stepBuilder()
                                .orgId(TopicNamePatternParameterPattern.anyOf(
                                        "test-org-id-1",
                                        "test-org-id-2"
                                ))
                                .domainContext(TopicNamePatternParameterPattern.anyOf(
                                        "test-domain-context-1",
                                        "test-domain-context-2"
                                ))
                                .build()
                )
                .eventName(TopicNamePatternParameterPattern.anyOf(
                        "test-event-name-1",
                        "test-event-name-2"
                ))
                .build();

        ConcurrentMessageListenerContainer<String, TestObject> listenerContainer =
                parameterizedListenerContainerFactoryService
                        .createRecordListenerContainerFactory(
                                TestObject.class,
                                consumerRecord -> {
                                    consumedRecords.add(consumerRecord);
                                    eventCDL.countDown();
                                },
                                ListenerConfiguration
                                        .stepBuilder()
                                        .groupIdApplicationDefault()
                                        .maxPollRecordsKafkaDefault()
                                        .maxPollIntervalKafkaDefault()
                                        .continueFromPreviousOffsetOnAssignment()
                                        .build(),
                                errorHandlerFactory.createErrorHandler(
                                        ErrorHandlerConfiguration
                                                .stepBuilder()
                                                .noRetries()
                                                .skipFailedRecords()
                                                .build()
                                )
                        )
                        .createContainer(eventTopicNamePatternParameters);

        ParameterizedTemplate<TestObject> parameterizedTemplate =
                parameterizedTemplateFactory.createTemplate(TestObject.class);
        AtomicInteger messageCounter = new AtomicInteger(0);
        Stream
                .of(
                        eventTopicNameParameters1,
                        eventTopicNameParameters2,
                        eventTopicNameParameters3,
                        eventTopicNameParameters4,
                        eventTopicNameParameters5
                )
                .forEach(topicNameParameters -> {
                            int messageCount = messageCounter.incrementAndGet();
                            parameterizedTemplate.send(
                                    ParameterizedProducerRecord
                                            .<TestObject>builder()
                                            .topicNameParameters(topicNameParameters)
                                            .key("test-key-" + messageCount)
                                            .value(new TestObject(
                                                    messageCount,
                                                    "testObjectString" + messageCount
                                            ))
                                            .build()
                            );
                        }
                );

        listenerContainer.start();

        assertThat(eventCDL.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(consumedRecords).hasSize(2);

        List<TopicKeyValue> topicKeyValueList = consumedRecords
                .stream()
                .map(cr -> new TopicKeyValue(cr.topic(), cr.key(), cr.value()))
                .toList();

        assertThat(topicKeyValueList).containsExactlyInAnyOrder(
                new TopicKeyValue(
                        "test-org-id-1.test-domain-context-1.event.test-event-name-1",
                        "test-key-4",
                        new TestObject(4, "testObjectString4")
                ),
                new TopicKeyValue(
                        "test-org-id-2.test-domain-context-2.event.test-event-name-2",
                        "test-key-5",
                        new TestObject(5, "testObjectString5")
                )
        );
    }


    public record TopicKeyValue(String topic, String key, TestObject value) {
    }
}

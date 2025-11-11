package no.fintlabs.kafka;


import lombok.*;
import no.fintlabs.kafka.consuming.ErrorHandlerConfiguration;
import no.fintlabs.kafka.consuming.ErrorHandlerFactory;
import no.fintlabs.kafka.consuming.ListenerConfiguration;
import no.fintlabs.kafka.consuming.ParameterizedListenerContainerFactoryService;
import no.fintlabs.kafka.model.Error;
import no.fintlabs.kafka.model.ErrorCollection;
import no.fintlabs.kafka.model.ParameterizedProducerRecord;
import no.fintlabs.kafka.producing.ParameterizedTemplate;
import no.fintlabs.kafka.producing.ParameterizedTemplateFactory;
import no.fintlabs.kafka.topic.name.EntityTopicNameParameters;
import no.fintlabs.kafka.topic.name.ErrorEventTopicNameParameters;
import no.fintlabs.kafka.topic.name.EventTopicNameParameters;
import no.fintlabs.kafka.topic.name.TopicNamePrefixParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
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
        ConsumerRecord<String, TestObject> consumedRecord = consumedRecords.getFirst();
        assertThat(consumedRecord.topic()).isEqualTo("test-org-id.test-domain-context.event.test-event-name");
        assertThat(consumedRecord.key()).isEqualTo("test-key");
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
        ConsumerRecord<String, ErrorCollection> consumedRecord = consumedRecords.getFirst();
        assertThat(consumedRecord.topic()).isEqualTo("test-org-id.test-domain-context.event.error.test-error-event-name");
        assertThat(consumedRecord.key()).isEqualTo("test-key");
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
        ConsumerRecord<String, TestObject> consumedRecord = consumedRecords.getFirst();
        assertThat(consumedRecord.topic()).isEqualTo("test-org-id.test-domain-context.entity.test-resource-name");
        assertThat(consumedRecord.key()).isEqualTo("test-key");
        assertThat(consumedRecord.value()).isEqualTo(new TestObject(2, "testObjectString"));
    }

}

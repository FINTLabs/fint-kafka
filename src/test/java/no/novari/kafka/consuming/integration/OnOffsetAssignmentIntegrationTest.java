package no.novari.kafka.consuming.integration;

import no.novari.kafka.TopicNameGenerator;
import no.novari.kafka.consumertracking.ConsumerTrackingService;
import no.novari.kafka.consumertracking.ConsumerTrackingTools;
import no.novari.kafka.consumertracking.event.ListenerInvokedWithRecord;
import no.novari.kafka.consumertracking.event.ListenerSuccessfullyProcessedRecord;
import no.novari.kafka.consumertracking.event.OffsetsCommitted;
import no.novari.kafka.consumertracking.event.PartitionsAssigned;
import no.novari.kafka.consumertracking.event.RecordsPolled;
import no.novari.kafka.consumertracking.event.predicates.EventTypePredicate;
import no.novari.kafka.consumertracking.event.predicates.OffsetCommittedPredicate;
import no.novari.kafka.consumertracking.event.report.KeyValueReport;
import no.novari.kafka.consumertracking.event.report.TopicPartitionReport;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import no.novari.kafka.consuming.ErrorHandlerFactory;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.consuming.ListenerContainerFactoryService;
import no.novari.kafka.producing.TemplateFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class OnOffsetAssignmentIntegrationTest {
    private static final TopicNameGenerator topicNameGenerator = new TopicNameGenerator(42);
    private ListenerContainerFactoryService listenerContainerFactoryService;
    private ErrorHandlerFactory errorHandlerFactory;
    private ConsumerTrackingService consumerTrackingService;
    private KafkaTemplate<String, String> template;

    @BeforeEach
    public void setup(
            @Autowired ListenerContainerFactoryService listenerContainerFactoryService,
            @Autowired ErrorHandlerFactory errorHandlerFactory,
            @Autowired TemplateFactory templateFactory,
            @Autowired ConsumerTrackingService consumerTrackingService
    ) {
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.errorHandlerFactory = errorHandlerFactory;
        this.consumerTrackingService = consumerTrackingService;
        template = templateFactory.createTemplate(String.class);
    }

    @Test
    void continueFromPreviousOffsetOnAssignment() {
        final String topic = topicNameGenerator.generateRandomTopicName();
        final TopicPartitionReport topicPartition = new TopicPartitionReport(topic, 0);

        IntStream
                .rangeClosed(1, 2)
                .forEach(i -> template.send(topic, "key" + i, "value" + i));

        ConsumerTrackingTools<String> consumerTrackingTools1 = consumerTrackingService.createConsumerTrackingTools(
                new OffsetCommittedPredicate<>(topicPartition, 2L)
        );
        ConcurrentMessageListenerContainer<String, String> listenerContainer1 =
                listenerContainerFactoryService
                        .createRecordListenerContainerFactory(
                                String.class,
                                consumerRecord -> {},
                                consumerTrackingTools1.wrapListenerConfigurationWithAssignmentTracking(
                                        ListenerConfiguration
                                                .stepBuilder()
                                                .groupIdApplicationDefault()
                                                .maxPollRecordsKafkaDefault()
                                                .maxPollIntervalKafkaDefault()
                                                .continueFromPreviousOffsetOnAssignment()
                                                .build()
                                ),
                                errorHandlerFactory.createErrorHandler(
                                        consumerTrackingTools1.wrapErrorHandlerConfigWithCustomRecovererTracking(
                                                ErrorHandlerConfiguration
                                                        .<String>stepBuilder()
                                                        .noRetries()
                                                        .skipFailedRecords()
                                                        .build()
                                        )
                                ),
                                consumerTrackingTools1::registerContainerTracking
                        )
                        .createContainer(topic);
        listenerContainer1.start();

        assertThat(consumerTrackingTools1.waitForEventCondition(Duration.ofSeconds(10))).isTrue();
        assertThat(consumerTrackingTools1.getEvents())
                .isEqualTo(
                        List.of(
                                new PartitionsAssigned<>(Map.of(
                                        topicPartition, 0L
                                )),
                                new RecordsPolled<>(Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2")
                                        )
                                )),
                                new ListenerInvokedWithRecord<Object>(
                                        topicPartition,
                                        new KeyValueReport<>("key1", "value1")
                                ),
                                new ListenerSuccessfullyProcessedRecord<Object>(
                                        topicPartition,
                                        new KeyValueReport<>("key1", "value1")
                                ),
                                new ListenerInvokedWithRecord<Object>(
                                        topicPartition,
                                        new KeyValueReport<>("key2", "value2")
                                ),
                                new ListenerSuccessfullyProcessedRecord<Object>(
                                        topicPartition,
                                        new KeyValueReport<>("key2", "value2")
                                ),
                                new OffsetsCommitted<>(Map.of(topicPartition, 2L))
                        )
                );
        listenerContainer1.stop();

        IntStream
                .rangeClosed(3, 4)
                .forEach(i -> template.send(topic, "key" + i, "value" + i));

        ConsumerTrackingTools<String> consumerTrackingTools2 = consumerTrackingService.createConsumerTrackingTools(
                new EventTypePredicate<>(RecordsPolled.class)
        );
        ConcurrentMessageListenerContainer<String, String> listenerContainer2 =
                listenerContainerFactoryService
                        .createRecordListenerContainerFactory(
                                String.class,
                                consumerRecord -> {},
                                consumerTrackingTools2.wrapListenerConfigurationWithAssignmentTracking(
                                        ListenerConfiguration
                                                .stepBuilder()
                                                .groupIdApplicationDefault()
                                                .maxPollRecordsKafkaDefault()
                                                .maxPollIntervalKafkaDefault()
                                                .continueFromPreviousOffsetOnAssignment()
                                                .build()
                                ),
                                errorHandlerFactory.createErrorHandler(
                                        consumerTrackingTools2.wrapErrorHandlerConfigWithCustomRecovererTracking(
                                                ErrorHandlerConfiguration
                                                        .<String>stepBuilder()
                                                        .noRetries()
                                                        .skipFailedRecords()
                                                        .build()
                                        )
                                ),
                                consumerTrackingTools2::registerContainerTracking
                        )
                        .createContainer(topic);
        listenerContainer2.start();

        assertThat(consumerTrackingTools2.waitForEventCondition(Duration.ofSeconds(10))).isTrue();
        assertThat(consumerTrackingTools2.getEvents())
                .isEqualTo(
                        List.of(
                                new PartitionsAssigned<>(Map.of(
                                        topicPartition, 2L
                                )),
                                new RecordsPolled<>(Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ))
                        )
                );
        listenerContainer2.stop();
    }

    @Test
    void seekToBeginningOnAssignment() {
        final String topic = topicNameGenerator.generateRandomTopicName();
        final TopicPartitionReport topicPartition = new TopicPartitionReport(topic, 0);

        IntStream
                .rangeClosed(1, 2)
                .forEach(i -> template.send(topic, "key" + i, "value" + i));

        ConsumerTrackingTools<String> consumerTrackingTools1 = consumerTrackingService.createConsumerTrackingTools(
                new OffsetCommittedPredicate<>(topicPartition, 2L)
        );
        ConcurrentMessageListenerContainer<String, String> listenerContainer1 =
                listenerContainerFactoryService
                        .createRecordListenerContainerFactory(
                                String.class,
                                consumerRecord -> {},
                                consumerTrackingTools1.wrapListenerConfigurationWithAssignmentTracking(
                                        ListenerConfiguration
                                                .stepBuilder()
                                                .groupIdApplicationDefault()
                                                .maxPollRecordsKafkaDefault()
                                                .maxPollIntervalKafkaDefault()
                                                .seekToBeginningOnAssignment()
                                                .build()
                                ),
                                errorHandlerFactory.createErrorHandler(
                                        consumerTrackingTools1.wrapErrorHandlerConfigWithCustomRecovererTracking(
                                                ErrorHandlerConfiguration
                                                        .<String>stepBuilder()
                                                        .noRetries()
                                                        .skipFailedRecords()
                                                        .build()
                                        )
                                ),
                                consumerTrackingTools1::registerContainerTracking
                        )
                        .createContainer(topic);
        listenerContainer1.start();

        assertThat(consumerTrackingTools1.waitForEventCondition(Duration.ofSeconds(10))).isTrue();
        assertThat(consumerTrackingTools1.getEvents())
                .isEqualTo(
                        List.of(
                                new PartitionsAssigned<>(Map.of(
                                        topicPartition, 0L
                                )),
                                new RecordsPolled<>(Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2")
                                        )
                                )),
                                new ListenerInvokedWithRecord<Object>(
                                        topicPartition,
                                        new KeyValueReport<>("key1", "value1")
                                ),
                                new ListenerSuccessfullyProcessedRecord<Object>(
                                        topicPartition,
                                        new KeyValueReport<>("key1", "value1")
                                ),
                                new ListenerInvokedWithRecord<Object>(
                                        topicPartition,
                                        new KeyValueReport<>("key2", "value2")
                                ),
                                new ListenerSuccessfullyProcessedRecord<Object>(
                                        topicPartition,
                                        new KeyValueReport<>("key2", "value2")
                                ),
                                new OffsetsCommitted<>(Map.of(topicPartition, 2L))
                        )
                );
        listenerContainer1.stop();


        ConsumerTrackingTools<String> consumerTrackingTools2 = consumerTrackingService.createConsumerTrackingTools(
                new EventTypePredicate<>(RecordsPolled.class)
        );
        ConcurrentMessageListenerContainer<String, String> listenerContainer2 =
                listenerContainerFactoryService
                        .createRecordListenerContainerFactory(
                                String.class,
                                consumerRecord -> {},
                                consumerTrackingTools2.wrapListenerConfigurationWithAssignmentTracking(
                                        ListenerConfiguration
                                                .stepBuilder()
                                                .groupIdApplicationDefault()
                                                .maxPollRecordsKafkaDefault()
                                                .maxPollIntervalKafkaDefault()
                                                .seekToBeginningOnAssignment()
                                                .build()
                                ),
                                errorHandlerFactory.createErrorHandler(
                                        consumerTrackingTools2.wrapErrorHandlerConfigWithCustomRecovererTracking(
                                                ErrorHandlerConfiguration
                                                        .<String>stepBuilder()
                                                        .noRetries()
                                                        .skipFailedRecords()
                                                        .build()
                                        )
                                ),
                                consumerTrackingTools2::registerContainerTracking
                        )
                        .createContainer(topic);
        listenerContainer2.start();

        assertThat(consumerTrackingTools2.waitForEventCondition(Duration.ofSeconds(10))).isTrue();
        assertThat(consumerTrackingTools2.getEvents())
                .isEqualTo(
                        List.of(
                                new PartitionsAssigned<>(Map.of(
                                        topicPartition, 2L
                                )),
                                new RecordsPolled<>(Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2")
                                        )
                                ))
                        )
                );
        listenerContainer2.stop();
    }

    @Test
    void customSeekOnAssignment() {
        final String topic = topicNameGenerator.generateRandomTopicName();
        final TopicPartitionReport topicPartition = new TopicPartitionReport(topic, 0);

        IntStream
                .rangeClosed(1, 4)
                .forEach(i -> template.send(topic, "key" + i, "value" + i));

        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                new EventTypePredicate<>(RecordsPolled.class)
        );
        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService
                        .createRecordListenerContainerFactory(
                                String.class,
                                consumerRecord -> {},
                                consumerTrackingTools.wrapListenerConfigurationWithAssignmentTracking(
                                        ListenerConfiguration
                                                .stepBuilder()
                                                .groupIdApplicationDefault()
                                                .maxPollRecordsKafkaDefault()
                                                .maxPollIntervalKafkaDefault()
                                                .onAssignment(
                                                        (assignments, callbacks) ->
                                                                callbacks.seek(topic, 0, 2L)
                                                )
                                                .build()
                                ),
                                errorHandlerFactory.createErrorHandler(
                                        consumerTrackingTools.wrapErrorHandlerConfigWithCustomRecovererTracking(
                                                ErrorHandlerConfiguration
                                                        .<String>stepBuilder()
                                                        .noRetries()
                                                        .skipFailedRecords()
                                                        .build()
                                        )
                                ),
                                consumerTrackingTools::registerContainerTracking
                        )
                        .createContainer(topic);
        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForEventCondition(Duration.ofSeconds(10))).isTrue();
        assertThat(consumerTrackingTools.getEvents())
                .isEqualTo(
                        List.of(
                                new PartitionsAssigned<>(Map.of(
                                        topicPartition, 0L
                                )),
                                new RecordsPolled<>(Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ))
                        )
                );
        listenerContainer.stop();
    }

}

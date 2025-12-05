package no.novari.kafka.topic;

import lombok.extern.slf4j.Slf4j;
import no.novari.kafka.consumertracking.ConsumerTrackingService;
import no.novari.kafka.consumertracking.ConsumerTrackingTools;
import no.novari.kafka.consumertracking.event.PartitionsAssigned;
import no.novari.kafka.consumertracking.event.RecordsPolled;
import no.novari.kafka.consumertracking.event.predicates.EventTypePredicate;
import no.novari.kafka.consumertracking.event.reports.KeyValueReport;
import no.novari.kafka.consumertracking.event.reports.TopicPartitionReport;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import no.novari.kafka.consuming.ErrorHandlerFactory;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.consuming.ListenerContainerFactoryService;
import no.novari.kafka.producing.TemplateFactory;
import no.novari.kafka.topic.configuration.TopicCompactCleanupPolicyConfiguration;
import no.novari.kafka.topic.configuration.TopicConfiguration;
import no.novari.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.novari.kafka.topic.configuration.TopicSegmentConfiguration;
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

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(properties = {"logging.level.kafka.log.LogCleaner=DEBUG"})
@EmbeddedKafka(
        partitions = 1,
        kraft = true,
        brokerProperties = {
                "log.cleaner.backoff.ms=1000",
                "log.retention.check.interval.ms=1000",
                "log.roll.ms=1000",
                "log.retention.ms=1000",
                "retention.ms=1000",
                "log.initial.task.delay.ms=0"
                // Necessary for cleanup=delete to trigger quickly
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class TopicCleanupIntegrationTest {
    private TopicService topicService;
    private ListenerContainerFactoryService listenerContainerFactoryService;
    private ErrorHandlerFactory errorHandlerFactory;
    private ConsumerTrackingService consumerTrackingService;
    private KafkaTemplate<String, String> template;

    @BeforeEach
    public void setup(
            @Autowired TopicService topicService,
            @Autowired ListenerContainerFactoryService listenerContainerFactoryService,
            @Autowired ErrorHandlerFactory errorHandlerFactory,
            @Autowired TemplateFactory templateFactory,
            @Autowired ConsumerTrackingService consumerTrackingService
    ) {
        this.topicService = topicService;
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.errorHandlerFactory = errorHandlerFactory;
        this.consumerTrackingService = consumerTrackingService;
        template = templateFactory.createTemplate(String.class);
    }

    @Test
    public void delete() {
        final String topicName = "deleteTopic";
        TopicPartitionReport topicPartition = new TopicPartitionReport(topicName, 0);

        topicService.createOrModifyTopic(
                topicName,
                TopicConfiguration
                        .builder()
                        .partitions(1)
                        .segmentConfiguration(
                                TopicSegmentConfiguration
                                        .builder()
                                        .openSegmentDuration(Duration.ofSeconds(1))
                                        .build()
                        )
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(Duration.ofSeconds(1))
                                        .build()
                        )
                        .build()
        );
        template.send(topicName, "key1", "value1");
        template.send(topicName, "key2", "value2");
        template.send(topicName, "key3", "value3");
        template.send(topicName, "key4", "value4");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        template.send(topicName, "key5", "value5");
        template.send(topicName, "key6", "value6");

        ConsumerTrackingTools<String> trackingTools = consumerTrackingService.createConsumerTrackingTools(
                new EventTypePredicate<>(RecordsPolled.class)
        );

        ConcurrentMessageListenerContainer<String, String> container = listenerContainerFactoryService
                .createRecordListenerContainerFactory(
                        String.class,
                        consumerRecord -> {},
                        trackingTools.wrapListenerConfigurationWithAssignmentTracking(
                                ListenerConfiguration
                                        .stepBuilder()
                                        .groupIdApplicationDefault()
                                        .maxPollRecordsKafkaDefault()
                                        .maxPollIntervalKafkaDefault()
                                        .continueFromPreviousOffsetOnAssignment()
                                        .build()
                        ),
                        errorHandlerFactory.createErrorHandler(
                                trackingTools.wrapRecovererWithTracking(
                                        ErrorHandlerConfiguration
                                                .<String>stepBuilder()
                                                .noRetries()
                                                .skipFailedRecords()
                                                .build()
                                )
                        ),
                        trackingTools::registerContainerTracking
                )
                .createContainer(topicName);

        container.start();

        assertThat(trackingTools.waitForEventCondition(Duration.ofSeconds(10))).isTrue();
        assertThat(trackingTools.getEvents()).isEqualTo(
                List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 4L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition,
                                List.of(
                                        new KeyValueReport<>("key5", "value5"),
                                        new KeyValueReport<>("key6", "value6")
                                )
                        ))
                )
        );
        container.stop();
    }

    /**
     * Kafka rolls a segment for a compacted topic when a new record’s timestamp differs from the timestamp of the
     * first record in the segment by more than segment.ms. The last sent record in this test is sent to trigger
     * compaction, and is not included in the asserted events.
     */
    @Test
    public void compact() {
        final String topicName = "compactTopic";
        TopicPartitionReport topicPartition = new TopicPartitionReport(topicName, 0);
        topicService.createOrModifyTopic(
                topicName,
                TopicConfiguration
                        .builder()
                        .partitions(1)
                        .segmentConfiguration(
                                TopicSegmentConfiguration
                                        .builder()
                                        .openSegmentDuration(Duration.ofSeconds(1))
                                        .build()
                        )
                        .compactCleanupPolicy(
                                TopicCompactCleanupPolicyConfiguration
                                        .builder()
                                        .maxCompactionLag(Duration.ofSeconds(1))
                                        .nullValueRetentionTime(Duration.ofDays(1))
                                        .build()
                        )
                        .build()
        );
        template.send(topicName, "key1", "value1-1");
        template.send(topicName, "key2", "value2-1");
        template.send(topicName, "key3", "value3");
        template.send(topicName, "key4", "value4-1");
        template.send(topicName, "key4", "value4-2");
        template.send(topicName, "key2", "value2-2");
        template.send(topicName, "key5", "value5");
        template.send(topicName, "key1", "value1-2");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        template.send(topicName, "key5", "value5");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        ConsumerTrackingTools<String> trackingTools = consumerTrackingService.createConsumerTrackingTools(
                new EventTypePredicate<>(RecordsPolled.class)
        );

        ConcurrentMessageListenerContainer<String, String> container = listenerContainerFactoryService
                .createRecordListenerContainerFactory(
                        String.class,
                        consumerRecord -> {},
                        trackingTools.wrapListenerConfigurationWithAssignmentTracking(
                                ListenerConfiguration
                                        .stepBuilder()
                                        .groupIdApplicationDefault()
                                        .maxPollRecordsKafkaDefault()
                                        .maxPollIntervalKafkaDefault()
                                        .continueFromPreviousOffsetOnAssignment()
                                        .build()
                        ),
                        errorHandlerFactory.createErrorHandler(
                                trackingTools.wrapRecovererWithTracking(
                                        ErrorHandlerConfiguration
                                                .<String>stepBuilder()
                                                .noRetries()
                                                .skipFailedRecords()
                                                .build()
                                )
                        ),
                        trackingTools::registerContainerTracking
                )
                .createContainer(topicName);

        container.start();

        assertThat(trackingTools.waitForEventCondition(Duration.ofSeconds(10))).isTrue();
        // Even if the first messages are removed from the topic, initial assigned offset is expected to be 0.
        // Initial offset for compaction works differently than with delete cleanup.
        assertThat(trackingTools.getEvents()).isEqualTo(
                List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition,
                                List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4-2"),
                                        new KeyValueReport<>("key2", "value2-2"),
                                        new KeyValueReport<>("key5", "value5"),
                                        new KeyValueReport<>("key1", "value1-2")
                                )
                        ))
                )
        );
        container.stop();
    }

    /**
     * Kafka rolls a segment for a compacted topic when a new record’s timestamp differs from the timestamp of the
     * first record in the segment by more than segment.ms. This also triggers deletion cleanup when a topic has both
     * delete and compaction cleanup. The last sent record in this test is sent to trigger cleanup, and is not
     * included in the asserted events.
     */
    @Test
    public void compactAndDelete() {
        final String topicName = "compactAndDeleteTopic";
        TopicPartitionReport topicPartition = new TopicPartitionReport(topicName, 0);
        topicService.createOrModifyTopic(
                topicName,
                TopicConfiguration
                        .builder()
                        .partitions(1)
                        .segmentConfiguration(
                                TopicSegmentConfiguration
                                        .builder()
                                        .openSegmentDuration(Duration.ofSeconds(1))
                                        .build()
                        )
                        .compactCleanupPolicy(
                                TopicCompactCleanupPolicyConfiguration
                                        .builder()
                                        .maxCompactionLag(Duration.ofSeconds(1))
                                        .nullValueRetentionTime(Duration.ofDays(1))
                                        .build()
                        )
                        .deleteCleanupPolicy(
                                TopicDeleteCleanupPolicyConfiguration
                                        .builder()
                                        .retentionTime(Duration.ofMillis(5500))
                                        .build()
                        )
                        .build()
        );
        template.send(topicName, "key1", "value1");
        template.send(topicName, "key2", "value2");
        template.send(topicName, "key3", "value3-1");

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        template.send(topicName, "key5", "value5");
        template.send(topicName, "key3", "value3-2");
        template.send(topicName, "key4", "value4-1");
        template.send(topicName, "key4", "value4-2");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        template.send(topicName, "key4", "value4-3");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        ConsumerTrackingTools<String> trackingTools = consumerTrackingService.createConsumerTrackingTools(
                new EventTypePredicate<>(RecordsPolled.class)
        );

        ConcurrentMessageListenerContainer<String, String> container = listenerContainerFactoryService
                .createRecordListenerContainerFactory(
                        String.class,
                        consumerRecord -> {},
                        trackingTools.wrapListenerConfigurationWithAssignmentTracking(
                                ListenerConfiguration
                                        .stepBuilder()
                                        .groupIdApplicationDefault()
                                        .maxPollRecordsKafkaDefault()
                                        .maxPollIntervalKafkaDefault()
                                        .continueFromPreviousOffsetOnAssignment()
                                        .build()
                        ),
                        errorHandlerFactory.createErrorHandler(
                                trackingTools.wrapRecovererWithTracking(
                                        ErrorHandlerConfiguration
                                                .<String>stepBuilder()
                                                .noRetries()
                                                .skipFailedRecords()
                                                .build()
                                )
                        ),
                        trackingTools::registerContainerTracking
                )
                .createContainer(topicName);

        container.start();

        assertThat(trackingTools.waitForEventCondition(Duration.ofSeconds(10))).isTrue();
        assertThat(trackingTools.getEvents()).isEqualTo(
                List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 3L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition,
                                List.of(
                                        new KeyValueReport<>("key5", "value5"),
                                        new KeyValueReport<>("key3", "value3-2"),
                                        new KeyValueReport<>("key4", "value4-2")
                                )
                        ))
                )
        );
        container.stop();
    }

}

package no.fintlabs.kafka.topic;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.consumertracking.ConsumerTrackingService;
import no.fintlabs.kafka.consumertracking.ConsumerTrackingTools;
import no.fintlabs.kafka.consumertracking.events.Event;
import no.fintlabs.kafka.consumertracking.events.RecordReport;
import no.fintlabs.kafka.consumertracking.events.RecordsReport;
import no.fintlabs.kafka.consuming.*;
import no.fintlabs.kafka.producing.TemplateFactory;
import no.fintlabs.kafka.topic.configuration.TopicCompactCleanupPolicyConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicDeleteCleanupPolicyConfiguration;
import no.fintlabs.kafka.topic.configuration.TopicSegmentConfiguration;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static no.fintlabs.kafka.consumertracking.events.Event.Type.RECORDS_POLLED;
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
                "log.initial.task.delay.ms=0" // Necessary for cleanup=delete to trigger quickly
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD) // TODO 02/10/2025 eivindmorch: remove
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
        topicService.createOrModifyTopic(
                "deleteTopic",
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
        template.send("deleteTopic", "key1", "value1");
        template.send("deleteTopic", "key2", "value2");
        template.send("deleteTopic", "key3", "value3");
        template.send("deleteTopic", "key4", "value4");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        template.send("deleteTopic", "key5", "value5");
        template.send("deleteTopic", "key6", "value6");

        CountDownLatch hasBeenAssignedLatch = new CountDownLatch(1);
        AtomicLong assignedOffset = new AtomicLong(-1L);

        ConsumerTrackingTools<String> trackingTools = consumerTrackingService.createConsumerTrackingTools(
                "deleteTopic",
                6L
        );

        listenerContainerFactoryService.createListenerContainerFactory(
                        String.class,
                        ListenerConfiguration
                                .stepBuilder()
                                .groupIdApplicationDefault()
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .continueFromPreviousOffsetOnAssignment()
                                .build(),
                        errorHandlerFactory.createErrorHandler(
                                ErrorHandlerConfiguration
                                        .<String>stepBuilder()
                                        .noRetries()
                                        .skipFailedRecords()
                                        .build()
                        ),
                        container -> new TestOffsetSeekingListener(
                                Map.of(new TopicPartition("deleteTopic", 0),
                                        offset -> {
                                            assignedOffset.set(offset);
                                            hasBeenAssignedLatch.countDown();
                                        }
                                )),
                        trackingTools::registerTracking
                ).createContainer("deleteTopic")
                .start();

        try {
            assertThat(hasBeenAssignedLatch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertThat(assignedOffset.get()).isEqualTo(4L);

        assertThat(trackingTools.waitForFinalCommit(Duration.ofSeconds(10))).isTrue();
        assertThat(trackingTools.getFilteredEvents(RECORDS_POLLED)).isEqualTo(List.of(
                Event.recordsPolled(new RecordsReport<>(List.of(
                        new RecordReport<>("key5", "value5"),
                        new RecordReport<>("key6", "value6")
                )))
        ));
    }

    // TODO 29/09/2025 eivindmorch: Kafka rolls a segment when a new record’s timestamp differs from the timestamp of
    //  the first record in the segment by more than segment.ms
    @Test
    public void compact() {
        topicService.createOrModifyTopic(
                "compactTopic",
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
        template.send("compactTopic", "key1", "value1-1");
        template.send("compactTopic", "key2", "value2-1");
        template.send("compactTopic", "key3", "value3");
        template.send("compactTopic", "key4", "value4-1");
        template.send("compactTopic", "key4", "value4-2");
        template.send("compactTopic", "key2", "value2-2");
        template.send("compactTopic", "key5", "value5");
        template.send("compactTopic", "key1", "value1-2");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // TODO 02/10/2025 eivindmorch: Document why this is needed to trigger compaction. Maybe related to segment
        //  rotation being triggered by timestamp diff between first message in segment and new message > segment.ms?
        template.send("compactTopic", "key5", "value5");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        CountDownLatch hasBeenAssignedLatch = new CountDownLatch(1);
        AtomicLong assignedOffset = new AtomicLong(-1L);

        ConsumerTrackingTools<String> trackingTools = consumerTrackingService.createConsumerTrackingTools(
                "compactTopic",
                8L
        );

        listenerContainerFactoryService.createListenerContainerFactory(
                        String.class,
                        ListenerConfiguration
                                .stepBuilder()
                                .groupIdApplicationDefault()
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .continueFromPreviousOffsetOnAssignment()
                                .build(),
                        errorHandlerFactory.createErrorHandler(
                                ErrorHandlerConfiguration
                                        .<String>stepBuilder()
                                        .noRetries()
                                        .skipFailedRecords()
                                        .build()
                        ),
                        container -> new TestOffsetSeekingListener(
                                Map.of(new TopicPartition("compactTopic", 0),
                                        offset -> {
                                            assignedOffset.set(offset);
                                            hasBeenAssignedLatch.countDown();
                                        }
                                )),
                        trackingTools::registerTracking
                ).createContainer("compactTopic")
                .start();

        try {
            assertThat(hasBeenAssignedLatch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Even if the first messages are removed from the topic, initial assigned offset is expected to be 0. Initial
        // offset for compaction works differently than with delete cleanup.
        assertThat(assignedOffset.get()).isEqualTo(0L);

        assertThat(trackingTools.waitForFinalCommit(Duration.ofSeconds(10))).isTrue();
        assertThat(trackingTools.getFilteredEvents(RECORDS_POLLED)).isEqualTo(List.of(
                Event.recordsPolled(new RecordsReport<>(List.of(
                        new RecordReport<>("key3", "value3"),
                        new RecordReport<>("key4", "value4-2"),
                        new RecordReport<>("key2", "value2-2"),
                        new RecordReport<>("key5", "value5"),
                        new RecordReport<>("key1", "value1-2")
                )))
        ));
    }

    @Test
    public void compactAndDelete() {
        topicService.createOrModifyTopic(
                "compactAndDeleteTopic",
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
                                        .retentionTime(Duration.ofMillis(3500))
                                        .build()
                        )
                        .build()
        );
        template.send("compactAndDeleteTopic", "key1", "value1");
        template.send("compactAndDeleteTopic", "key2", "value2");
        template.send("compactAndDeleteTopic", "key3", "value3-1");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        template.send("compactAndDeleteTopic", "key5", "value5");
        template.send("compactAndDeleteTopic", "key3", "value3-2");
        template.send("compactAndDeleteTopic", "key4", "value4-1");
        template.send("compactAndDeleteTopic", "key4", "value4-2");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // TODO 03/10/2025 eivindmorch: Not included in data used in test. Comment same reason as with compact test
        template.send("compactAndDeleteTopic", "key4", "value4-3");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        CountDownLatch hasBeenAssignedLatch = new CountDownLatch(1);
        AtomicLong assignedOffset = new AtomicLong(-1L);

        ConsumerTrackingTools<String> trackingTools = consumerTrackingService.createConsumerTrackingTools(
                "compactAndDeleteTopic",
                7L
        );

        listenerContainerFactoryService.createListenerContainerFactory(
                        String.class,
                        ListenerConfiguration
                                .stepBuilder()
                                .groupIdApplicationDefault()
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .continueFromPreviousOffsetOnAssignment()
                                .build(),
                        errorHandlerFactory.createErrorHandler(
                                ErrorHandlerConfiguration
                                        .<String>stepBuilder()
                                        .noRetries()
                                        .skipFailedRecords()
                                        .build()
                        ),
                        container -> new TestOffsetSeekingListener(
                                Map.of(new TopicPartition("compactAndDeleteTopic", 0),
                                        offset -> {
                                            assignedOffset.set(offset);
                                            hasBeenAssignedLatch.countDown();
                                        }
                                )),
                        trackingTools::registerTracking
                ).createContainer("compactAndDeleteTopic")
                .start();

        try {
            assertThat(hasBeenAssignedLatch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertThat(assignedOffset.get()).isEqualTo(3L);

        assertThat(trackingTools.waitForFinalCommit(Duration.ofSeconds(10))).isTrue();
        assertThat(trackingTools.getFilteredEvents(RECORDS_POLLED)).isEqualTo(List.of(
                Event.recordsPolled(new RecordsReport<>(List.of(
                        new RecordReport<>("key5", "value5"),
                        new RecordReport<>("key3", "value3-2"),
                        new RecordReport<>("key4", "value4-2")
                )))
        ));
    }

}

package no.fintlabs.kafka.consuming;

import no.fintlabs.kafka.producing.TemplateFactory;
import no.fintlabs.kafka.utils.consumertracking.ConsumerTrackingService;
import no.fintlabs.kafka.utils.consumertracking.ConsumerTrackingTools;
import no.fintlabs.kafka.utils.consumertracking.events.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext
public class ConsumerPollAndProcessIntegrationTest {

    ListenerContainerFactoryService listenerContainerFactoryService;
    ConsumerTrackingService consumerTrackingService;
    KafkaTemplate<String, String> template;

    @BeforeEach
    public void setup(
            @Autowired ListenerContainerFactoryService listenerContainerFactoryService,
            @Autowired TemplateFactory templateFactory,
            @Autowired ConsumerTrackingService consumerTrackingService
    ) {
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.consumerTrackingService = consumerTrackingService;
        template = templateFactory.createTemplate(String.class);
    }

    @Test
    public void recordConsumerShouldPollMultipleMessagesAtOnceAndConsumeEachRecordIndividuallyAndCommitRecordsInBatch() {
        final String topic = "test-topic-1";
        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                        String.class,
                        consumerRecord -> {
                        },
                        ListenerConfiguration
                                .<String>builder()
                                .groupIdApplicationDefault()
                                .maxPollRecords(3)
                                .maxPollIntervalKafkaDefault()
                                .errorHandling(
                                        ErrorHandlerConfiguration.<String>builder()
                                                .noRetries()
                                                .skipFailedRecords()
                                                .build()
                                )
                                .seekToBeginningOnAssignment()
                                .build(),
                        consumerTrackingTools::registerTracking
                ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(10))).isTrue();
        assertThat(consumerTrackingTools.getEvents()).isEqualTo(List.of(
                Event.recordsPolled(
                        new RecordsReport<Object>(List.of(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ))
                ),
                Event.listenerInvokedWithRecord(
                        new RecordReport<>("key1", "value1")
                ),
                Event.listenerSuccessfullyProcessedRecord(
                        new RecordReport<>("key1", "value1")
                ),
                Event.listenerInvokedWithRecord(
                        new RecordReport<>("key2", "value2")
                ),
                Event.listenerSuccessfullyProcessedRecord(
                        new RecordReport<>("key2", "value2")
                ),
                Event.listenerInvokedWithRecord(
                        new RecordReport<>("key3", "value3")
                ),
                Event.listenerSuccessfullyProcessedRecord(
                        new RecordReport<>("key3", "value3")
                ),
                Event.offsetsCommited(
                        new OffsetReport<>(3L)
                )
        ));
    }


    @Test
    public void givenErrorDuringRecordProcessingRecordConsumerShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
        final String topic = "test-topic-2";

        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );

        AtomicBoolean alreadyFailed = new AtomicBoolean(false);

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                        String.class,
                        consumerRecord -> {
                            if (consumerRecord.key().equals("key2") && !alreadyFailed.get()) {
                                alreadyFailed.set(true);
                                throw new RuntimeException();
                            }
                        },
                        ListenerConfiguration
                                .<String>builder()
                                .groupIdApplicationDefaultWithUniqueSuffix()
                                .maxPollRecordsKafkaDefault()
                                .maxPollInterval(Duration.ofSeconds(5))
                                .errorHandling(
                                        ErrorHandlerConfiguration
                                                .<String>builder()
                                                .retryWithFixedInterval(Duration.ofSeconds(8), 2)
                                                .skipFailedRecords()
                                                .build()
                                )
                                .continueFromPreviousOffsetOnAssignment()
                                .build(),
                        consumerTrackingTools::registerTracking
                ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(10))).isTrue();
        assertThat(consumerTrackingTools.getEvents()).isEqualTo(List.of(
                Event.recordsPolled(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ))
                ),
                Event.listenerInvokedWithRecord(
                        new RecordReport<>("key1", "value1")
                ),
                Event.listenerSuccessfullyProcessedRecord(
                        new RecordReport<>("key1", "value1")
                ),
                Event.listenerInvokedWithRecord(
                        new RecordReport<>("key2", "value2")
                ),
                Event.listenerFailedToProcessedRecord(
                        new RecordExceptionReport<>(
                                new RecordReport<>("key2", "value2"),
                                new ExceptionReport<>(
                                        ListenerExecutionFailedException.class,
                                        "Listener failed"
                                )
                        )
                ),
                Event.offsetsCommited(
                        new OffsetReport<>(1L)
                ),
                Event.recordDeliveryFailed(
                        RecordDeliveryFailedReport
                                .builder()
                                .record(new RecordReport<>("key2", "value2"))
                                .cause(
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                                .attempt(1)
                                .build()
                ),
                Event.recordsPolled(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ))
                ),
                Event.listenerInvokedWithRecord(
                        new RecordReport<>("key2", "value2")
                ),
                Event.listenerSuccessfullyProcessedRecord(
                        new RecordReport<>("key2", "value2")
                ),
                Event.listenerInvokedWithRecord(
                        new RecordReport<>("key3", "value3")
                ),
                Event.listenerSuccessfullyProcessedRecord(
                        new RecordReport<>("key3", "value3")
                ),
                Event.offsetsCommited(
                        new OffsetReport<>(3L)
                )
        ));
    }

    @Test
    public void batchConsumerShouldPollMultipleMessagesAtOnceAndConsumeInBatchesAndCommitRecordsInBatch() {
        final String topic = "test-topic-3";
        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                4L
        );

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
                        String.class,
                        consumerRecords -> {
                        },
                        ListenerConfiguration
                                .<String>builder()
                                .groupIdApplicationDefault()
                                .maxPollRecords(3)
                                .maxPollIntervalKafkaDefault()
                                .errorHandling(
                                        ErrorHandlerConfiguration
                                                .<String>builder()
                                                .noRetries()
                                                .skipFailedRecords()
                                                .build()
                                )
                                .seekToBeginningOnAssignment()
                                .build(),
                        consumerTrackingTools::registerTracking
                ).createContainer(topic);

        IntStream.rangeClosed(1, 4).forEach(i -> template.send(topic, "key" + i, "value" + i));

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(10))).isTrue();
        assertThat(consumerTrackingTools.getEvents()).isEqualTo(List.of(
                Event.recordsPolled(
                        new RecordsReport<>(
                                List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                )
                        )
                ),
                Event.listenerInvokedWithBatch(
                        new RecordsReport<>(
                                List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                )
                        )
                ),
                Event.listenerSuccessfullyProcessedBatch(
                        new RecordsReport<>(
                                List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                )
                        )
                ),
                Event.offsetsCommited(new OffsetReport<>(3L)),
                Event.recordsPolled(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key4", "value4")
                        ))
                ),
                Event.listenerInvokedWithBatch(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key4", "value4")
                        ))
                ),
                Event.listenerSuccessfullyProcessedBatch(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key4", "value4")
                        ))
                ),
                Event.offsetsCommited(new OffsetReport<>(4L))
        ));
    }

    @Test
    public void givenBatchListenerFailedExceptionDuringBatchProcessingBatchConsumerShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
        final String topic = "test-topic-4";

        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                4L
        );

        CountDownLatch latch = new CountDownLatch(2);
        //AtomicBoolean hasAlreadyFailed = new AtomicBoolean(false);

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
                        String.class,
                        consumerRecords -> {
                            OptionalInt failingRecordIndex = IntStream.range(0, consumerRecords.size())
                                    .filter(i -> "key2".equals(consumerRecords.get(i).key()))
                                    .findFirst();
                            if (failingRecordIndex.isPresent() && latch.getCount() > 0) {
                                latch.countDown();
                                throw new BatchListenerFailedException("test message", failingRecordIndex.getAsInt());
                            }
                        },
                        ListenerConfiguration
                                .<String>builder()
                                .groupIdApplicationDefault()
                                .maxPollRecords(3)
                                .maxPollIntervalKafkaDefault()
                                .errorHandling(
                                        ErrorHandlerConfiguration
                                                .<String>builder()
                                                .retryWithFixedInterval(Duration.ofSeconds(1), 3)
                                                .skipFailedRecords()
                                                .build()
                                )
                                .seekToBeginningOnAssignment()
                                .build(),
                        consumerTrackingTools::registerTracking
                ).createContainer(topic);

        IntStream.rangeClosed(1, 4).forEach(i -> template.send(topic, "key" + i, "value" + i));

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(10))).isTrue();
        assertThat(consumerTrackingTools.getEvents()).isEqualTo(List.of(
                Event.recordsPolled(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ))
                ),
                Event.listenerInvokedWithBatch(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ))
                ),
                Event.listenerFailedToProcessedBatch(
                        new RecordsExceptionReport<>(
                                List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                ),
                                new ExceptionReport<>(
                                        ListenerExecutionFailedException.class,
                                        "Listener failed"
                                )
                        )
                )
              /*  Event.errorHandlerHandleBatchCalled(
                        new RecordsExceptionReport<>(
                                List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                ),
                                new ExceptionReport<>(
                                        ListenerExecutionFailedException.class,
                                        "Listener failed"
                                )
                        )
                ),
                Event.retryListenerBatchFailedDeliveryCalled(
                        new RecordsExceptionReport<>(
                                List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                ),
                                new ExceptionReport<>(
                                        ListenerExecutionFailedException.class,
                                        "Listener failed"
                                )
                        )
                ),
                Event.offsetsCommited(new OffsetReport<>(1L)),
                Event.retryListenerRecordFailedDeliveryCalled(
                        new RecordExceptionReport<>(
                                new RecordReport<>("key2", "value2"),
                                new ExceptionReport<>(
                                        ListenerExecutionFailedException.class,
                                        "Listener failed"
                                )
                        )
                ),
                Event.recordsPolled(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3"),
                                new RecordReport<>("key4", "value4")
                        ))
                ),
                Event.listenerInvokedWithBatch(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3"),
                                new RecordReport<>("key4", "value4")
                        ))
                ),
                Event.listenerSuccessfullyProcessedBatch(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3"),
                                new RecordReport<>("key4", "value4")
                        ))
                ),
                Event.offsetsCommited(new OffsetReport<>(4L)) */
        ));
    }
/*
    @Test
    public void givenRuntimeExceptionOtherThanBatchListenerFailedExceptionDuringBatchProcessingBatchConsumerShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
        final String topic = "test-topic-5";

        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                4L
        );

        AtomicBoolean hasAlreadyFailed = new AtomicBoolean(false);

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
                        String.class,
                        consumerRecords -> {
                            log.info("Consuming {}", consumerRecords);
                            OptionalInt failingRecordIndex = IntStream.range(0, consumerRecords.size())
                                    .filter(i -> "key2".equals(consumerRecords.get(i).key()))
                                    .findFirst();
                            if (failingRecordIndex.isPresent() && !hasAlreadyFailed.get()) {
                                hasAlreadyFailed.set(true);
                                throw new RuntimeException("test message");
                            }
                        },
                        ListenerConfiguration
                                .<String>builder()
                                .maxPollRecords(3)
                                .errorHandlerConfiguration(null)
                                .build(),
                        consumerTrackingTools::registerInterceptors
                ).createContainer(topic);

        IntStream.rangeClosed(1, 4).forEach(i -> template.send(topic, "key" + i, "value" + i));

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(10, TimeUnit.SECONDS)).isTrue();
        assertThat(consumerTrackingTools.getEvents()).isEqualTo(List.of(
                Event.recordsPolled(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ))
                ),
                Event.listenerInvokedWithBatch(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ))
                ),
                Event.listenerFailedToProcessedBatch(
                        new RecordsExceptionReport<>(
                                List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                ),
                                new ExceptionReport<>(
                                        ListenerExecutionFailedException.class,
                                        "Listener failed"
                                )
                        )
                ),
                Event.errorHandlerHandleBatchCalled(
                        new RecordsExceptionReport<>(
                                List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                ),
                                new ExceptionReport<>(
                                        ListenerExecutionFailedException.class,
                                        "Listener failed"
                                )
                        )
                ),
                Event.retryListenerBatchFailedDeliveryCalled(
                        new RecordsExceptionReport<>(
                                List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                ),
                                new ExceptionReport<>(
                                        ListenerExecutionFailedException.class,
                                        "Listener failed"
                                )
                        )
                ),
                Event.listenerInvokedWithBatch(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ))
                ),
                Event.offsetsCommited(new OffsetReport<>(3L)),
                Event.recordsPolled(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key4", "value4")
                        ))
                ),
                Event.listenerInvokedWithBatch(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key4", "value4")
                        ))
                ),
                Event.listenerSuccessfullyProcessedBatch(
                        new RecordsReport<>(List.of(
                                new RecordReport<>("key4", "value4")
                        ))
                ),
                Event.offsetsCommited(new OffsetReport<>(4L))
        ));
    }*/

}

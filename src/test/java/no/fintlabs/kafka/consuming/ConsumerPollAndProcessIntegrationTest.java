package no.fintlabs.kafka.consuming;

import no.fintlabs.kafka.consumertracking.ConsumerTrackingService;
import no.fintlabs.kafka.consumertracking.ConsumerTrackingTools;
import no.fintlabs.kafka.consumertracking.events.*;
import no.fintlabs.kafka.producing.TemplateFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext
public class ConsumerPollAndProcessIntegrationTest {
    private static final Random random = new Random(42);
    private ListenerContainerFactoryService listenerContainerFactoryService;
    private ConsumerTrackingService consumerTrackingService;
    private KafkaTemplate<String, String> template;

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

    private String generateRandomTopicName() {
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        return UUID.nameUUIDFromBytes(bytes).toString();
    }

    // TODO 26/09/2025 eivindmorch: Test custom recoverer?
    //  Use same formulare for tests as batch (without need for handling two different types of exceptions)
    @Nested
    public class RecordConsumer {

        @Test
        public void givenMaxPollRecordsOverOneShouldPollMultipleMessagesAtOnceAndConsumeEachRecordIndividuallyAndCommitRecordsInBatch() {
            final String topic = generateRandomTopicName();
            ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                    topic,
                    3L
            );

            ConcurrentMessageListenerContainer<String, String> listenerContainer =
                    listenerContainerFactoryService.createRecordListenerContainerFactory(
                            consumerRecord -> {
                            },
                            ListenerConfiguration
                                    .stepBuilder(String.class)
                                    .groupIdApplicationDefault()
                                    .maxPollRecords(3)
                                    .maxPollIntervalKafkaDefault()
                                    .errorHandler(
                                            ErrorHandlerConfiguration.stepBuilder(String.class)
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
        public void givenSuccessfulRetryShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
            final String topic = generateRandomTopicName();

            ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                    topic,
                    3L
            );

            CountDownLatch latch = new CountDownLatch(1);

            ConcurrentMessageListenerContainer<String, String> listenerContainer =
                    listenerContainerFactoryService.createRecordListenerContainerFactory(
                            consumerRecord -> {
                                if (Objects.equals(consumerRecord.key(), "key2") && latch.getCount() > 0) {
                                    latch.countDown();
                                    throw new RuntimeException();
                                }
                            },
                            ListenerConfiguration
                                    .stepBuilder(String.class)
                                    .groupIdApplicationDefault()
                                    .maxPollRecordsKafkaDefault()
                                    .maxPollIntervalKafkaDefault()
                                    .errorHandler(
                                            ErrorHandlerConfiguration
                                                    .stepBuilder(String.class)
                                                    .retryWithFixedInterval(Duration.ofMillis(100), 1)
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
            consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(30));
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
        public void givenSuccessfulRecoveryShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
            final String topic = generateRandomTopicName();

            ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                    topic,
                    3L
            );

            ConcurrentMessageListenerContainer<String, String> listenerContainer =
                    listenerContainerFactoryService.createRecordListenerContainerFactory(
                            consumerRecord -> {
                                if (Objects.equals(consumerRecord.key(), "key2")) {
                                    throw new RuntimeException();
                                }
                            },
                            ListenerConfiguration
                                    .stepBuilder(String.class)
                                    .groupIdApplicationDefault()
                                    .maxPollRecordsKafkaDefault()
                                    .maxPollIntervalKafkaDefault()
                                    .errorHandler(
                                            ErrorHandlerConfiguration
                                                    .stepBuilder(String.class)
                                                    .noRetries()
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
            consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(30));
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
                    Event.recordRecovered(
                            new RecordReport<>("key2", "value2")
                    ),
                    Event.offsetsCommited(
                            new OffsetReport<>(2L)
                    ),
                    Event.recordsPolled(
                            new RecordsReport<>(List.of(
                                    new RecordReport<>("key3", "value3")
                            ))
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
    }

    @Nested
    public class BatchConsumer {

        @Test
        public void givenMaxPollRecordsMoreThanOneShouldPollMultipleMessagesAtOnceAndConsumeInBatchesAndCommitRecordsInBatch() {
            final String topic = generateRandomTopicName();

            ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                    topic,
                    4L
            );

            ConcurrentMessageListenerContainer<String, String> listenerContainer =
                    listenerContainerFactoryService.createBatchListenerContainerFactory(
                            consumerRecords -> {
                            },
                            ListenerConfiguration
                                    .stepBuilder(String.class)
                                    .groupIdApplicationDefault()
                                    .maxPollRecords(3)
                                    .maxPollIntervalKafkaDefault()
                                    .errorHandler(
                                            ErrorHandlerConfiguration
                                                    .stepBuilder(String.class)
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

            assertThat(consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(30))).isTrue();
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
        public void givenBatchListenerFailedExceptionAndNoRetriesShould() {

        }

        @Test
        public void givenBatchListenerFailedExceptionAndSuccessfulRetryShould() {

        }

        @Test
        public void givenBatchListenerFailedExceptionAndSuccessfulRecoveryShould() {

        }

        // TODO 26/09/2025 eivindmorch: RECORD
        //      happy case
        //      retries -> success
        //      no retries -> skip recovery,
        //      no retries -> custom recovery (check recoverer called)
        //      failing retry -> skip recovery

        // TODO 28/08/2025 eivindmorch: BATCH
        //      happy case
        //      For batchListenerFailedException and other:
        //          retries -> success,
        //          no retries -> skip recovery,
        //          no retries -> custom recovery (check recoverer called)
        //          failing retry -> skip recovery

        @Test
        public void givenBatchListenerFailedExceptionShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
            final String topic = generateRandomTopicName();

            ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                    topic,
                    4L
            );

            CountDownLatch latch = new CountDownLatch(1);

            ConcurrentMessageListenerContainer<String, String> listenerContainer =
                    listenerContainerFactoryService.createBatchListenerContainerFactory(
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
                                    .stepBuilder(String.class)
                                    .groupIdApplicationDefault()
                                    .maxPollRecords(3)
                                    .maxPollIntervalKafkaDefault()
                                    .errorHandler(
                                            ErrorHandlerConfiguration
                                                    .stepBuilder(String.class)
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

            assertThat(consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(30))).isTrue();
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
                    Event.batchDeliveryFailed(
                            BatchDeliveryFailedReport.builder()
                                    .records(new RecordsReport<>(List.of(
                                                    new RecordReport<>("key1", "value1"),
                                                    new RecordReport<>("key2", "value2"),
                                                    new RecordReport<>("key3", "value3")
                                            ))
                                    )
                                    .cause(new ExceptionReport<>(
                                            ListenerExecutionFailedException.class,
                                            "Listener failed"
                                    ))
                                    .attempt(1)
                                    .build()
                    ),
                    Event.offsetsCommited(new OffsetReport<>(1L)),
                    Event.recordDeliveryFailed(
                            RecordDeliveryFailedReport
                                    .builder()
                                    .record(new RecordReport<>("key2", "value2"))
                                    .cause(new ExceptionReport<>(
                                            ListenerExecutionFailedException.class,
                                            "Listener failed"
                                    ))
                                    .attempt(1)
                                    .build()
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
                    Event.offsetsCommited(new OffsetReport<>(4L))
            ));
        }

        @Test
        public void givenRuntimeExceptionOtherThanBatchListenerFailedExceptionShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
            final String topic = generateRandomTopicName();

            ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                    topic,
                    4L
            );

            CountDownLatch latch = new CountDownLatch(1);
            ConcurrentMessageListenerContainer<String, String> listenerContainer =
                    listenerContainerFactoryService.createBatchListenerContainerFactory(
                            consumerRecords -> {
                                OptionalInt failingRecordIndex = IntStream.range(0, consumerRecords.size())
                                        .filter(i -> "key2".equals(consumerRecords.get(i).key()))
                                        .findFirst();
                                if (failingRecordIndex.isPresent() && latch.getCount() > 0) {
                                    latch.countDown();
                                    throw new RuntimeException();
                                }
                            },
                            ListenerConfiguration
                                    .stepBuilder(String.class)
                                    .groupIdApplicationDefault()
                                    .maxPollRecords(3)
                                    .maxPollIntervalKafkaDefault()
                                    .errorHandler(
                                            ErrorHandlerConfiguration
                                                    .stepBuilder(String.class)
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
                    ),
                    Event.batchDeliveryFailed(
                            BatchDeliveryFailedReport
                                    .builder()
                                    .records(new RecordsReport<>(
                                            List.of(
                                                    new RecordReport<>("key1", "value1"),
                                                    new RecordReport<>("key2", "value2"),
                                                    new RecordReport<>("key3", "value3")
                                            )
                                    ))
                                    .cause(new ExceptionReport<>(
                                            ListenerExecutionFailedException.class,
                                            "Listener failed"
                                    ))
                                    .attempt(1)
                                    .build()
                    ),
                    Event.listenerInvokedWithBatch(
                            new RecordsReport<>(List.of(
                                    new RecordReport<>("key1", "value1"),
                                    new RecordReport<>("key2", "value2"),
                                    new RecordReport<>("key3", "value3")
                            ))
                    ),
                    Event.listenerSuccessfullyProcessedBatch(
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
        }
    }

}

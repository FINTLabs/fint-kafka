package no.novari.kafka.consuming.integration;

import no.novari.kafka.TopicNameGenerator;
import no.novari.kafka.consumertracking.ConsumerTrackingService;
import no.novari.kafka.consumertracking.ConsumerTrackingTools;
import no.novari.kafka.consumertracking.event.BatchDeliveryFailed;
import no.novari.kafka.consumertracking.event.BatchRecovered;
import no.novari.kafka.consumertracking.event.BatchRecoveryFailed;
import no.novari.kafka.consumertracking.event.CustomRecovererInvoked;
import no.novari.kafka.consumertracking.event.ListenerFailedToProcessBatch;
import no.novari.kafka.consumertracking.event.ListenerInvokedWithBatch;
import no.novari.kafka.consumertracking.event.ListenerSuccessfullyProcessedBatch;
import no.novari.kafka.consumertracking.event.OffsetsCommitted;
import no.novari.kafka.consumertracking.event.PartitionsAssigned;
import no.novari.kafka.consumertracking.event.RecordDeliveryFailed;
import no.novari.kafka.consumertracking.event.RecordRecovered;
import no.novari.kafka.consumertracking.event.RecordRecoveryFailed;
import no.novari.kafka.consumertracking.event.RecordsPolled;
import no.novari.kafka.consumertracking.event.predicates.OffsetCommittedPredicate;
import no.novari.kafka.consumertracking.event.report.ExceptionReport;
import no.novari.kafka.consumertracking.event.report.KeyValueReport;
import no.novari.kafka.consumertracking.event.report.TopicPartitionReport;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import no.novari.kafka.consuming.ErrorHandlerFactory;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.consuming.ListenerContainerFactoryService;
import no.novari.kafka.producing.TemplateFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class BatchConsumerPollAndProcessIntegrationTest {
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

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters1() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("maxPollRecords>1")
                .should("poll multiple records at once")
                .andShould("consume records in batch")
                .andShould("commit in batch")
                .numberOfMessages(3)
                .commitToWaitFor(3)
                .maxPollRecords(3)
                .noMessageProcessor()
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .noRetries()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters2() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("maxPollRecords=1")
                .should("poll records individually")
                .andShould("consume single record batches")
                .andShould("commit single record batches")
                .numberOfMessages(3)
                .commitToWaitFor(3)
                .maxPollRecords(1)
                .noMessageProcessor()
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .noRetries()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters3() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails once")
                .andGiven("throws BatchListenerFailedException with index of failed record")
                .andGiven("is configured for retry")
                .should("commit already processed records")
                .andShould("retry rest of batch")
                .numberOfMessages(4)
                .commitToWaitFor(3)
                .maxPollRecords(3)
                .failAtMessageOnce(
                        "key3",
                        () -> new BatchListenerFailedException("testMessage", 2)
                )
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .useDefaultRetryClassification()
                                .restartRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-2"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-2"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-2"),
                                topicPartition,
                                new KeyValueReport<>("key3", "value3"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters4() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails once")
                .andGiven("throws other exception than BatchListenerFailedException")
                .andGiven("is configured for retry")
                .should("retry whole batch")
                .numberOfMessages(3)
                .commitToWaitFor(3)
                .maxPollRecords(3)
                .failAtMessageOnce("key2")
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .useDefaultRetryClassification()
                                .restartRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters5() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails multiple times")
                .andGiven("throws BatchListenerFailedException with index of failed record")
                .andGiven("is configured for more attempts than fails")
                .should("commit already processed records")
                .andShould("retry batch starting with failed record until successfully processed")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce(
                        "key3",
                        () -> new BatchListenerFailedException("testMessage", 2)
                )
                .failAtMessageNTimes(
                        "key3",
                        2,
                        () -> new BatchListenerFailedException("testMessage", 0)
                )
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 3)
                                .useDefaultRetryClassification()
                                .restartRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-2"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-2"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-2"),
                                topicPartition,
                                new KeyValueReport<>("key3", "value3"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ),
                                1
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                topicPartition,
                                new KeyValueReport<>("key3", "value3"),
                                2
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ),
                                1
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                topicPartition,
                                new KeyValueReport<>("key3", "value3"),
                                3
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters6() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails multiple times")
                .andGiven("throws other exception than BatchListenerFailedException")
                .andGiven("is configured for more attempts than fails")
                .should("retry batch until successfully processed")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce("key3")
                .failAtMessageNTimes("key3", 2)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 3)
                                .useDefaultRetryClassification()
                                .restartRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                2
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                3
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L)),

                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters7() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails multiple times")
                .andGiven("throws BatchListenerFailedException with index of failed record")
                .andGiven("is configured for less attempts than fails")
                .should("commit already processed records")
                .andShould("retry batch starting with failed record until retries exhausted")
                .andShould("recover record")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce(
                        "key3",
                        () -> new BatchListenerFailedException("testMessage", 2)
                )
                .failAtMessageNTimes(
                        "key3",
                        2,
                        () -> new BatchListenerFailedException("testMessage", 0)
                )
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 2)
                                .useDefaultRetryClassification()
                                .restartRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-2"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-2"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-2"),
                                topicPartition,
                                new KeyValueReport<>("key3", "value3"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ),
                                1
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                topicPartition,
                                new KeyValueReport<>("key3", "value3"),
                                2
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ),
                                1
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                topicPartition,
                                new KeyValueReport<>("key3", "value3"),
                                3
                        ),
                        new RecordRecovered<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters8() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails multiple times")
                .andGiven("throws other exception than BatchListenerFailedException")
                .andGiven("is configured for more attempts than fails")
                .should("retry batch until retries exhausted")
                .andShould("recover batch")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce("key3")
                .failAtMessageNTimes("key3", 2)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 2)
                                .useDefaultRetryClassification()
                                .restartRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                2
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                3
                        ),
                        new BatchRecovered<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters9() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails once")
                .andGiven("throws BatchListenerFailedException with index of failed record")
                .andGiven("is configured for no retries")
                .should("commit already processed records")
                .andShould("recover record")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce(
                        "key2",
                        () -> new BatchListenerFailedException("testMessage", 1)
                )
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .noRetries()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new RecordRecovered<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters10() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails once")
                .andGiven("throws other exception than BatchListenerFailedException")
                .andGiven("is configured for no retries")
                .should("recover batch")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce("key2")
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .noRetries()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new BatchRecovered<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters11() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails once")
                .andGiven("throws BatchListenerFailedException with index of failed record")
                .andGiven("is configured for no retries and  custom recoverer")
                .should("commit already processed records")
                .andShould("recover record")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce(
                        "key2",
                        () -> new BatchListenerFailedException("testMessage", 1)
                )
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .noRetries()
                                .recoverFailedRecords(
                                        (consumerRecord, e) -> {}
                                )
                                .skipRecordOnRecoveryFailure()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordRecovered<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters12() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails once")
                .andGiven("throws other exception than BatchListenerFailedException")
                .andGiven("is configured for no retries and  custom recoverer")
                .should("recover batch")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce("key2")
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .noRetries()
                                .recoverFailedRecords((consumerRecord, e) -> {
                                })
                                .skipRecordOnRecoveryFailure()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new BatchRecovered<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters13() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails")
                .andGiven("throws BatchListenerFailedException with index of failed record")
                .andGiven("is configured for retries")
                .andGiven("with failing custom recoverer")
                .andGiven("is configured to reprocess and retry on recovery failure")
                .should("reprocess and retry batch")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce(
                        "key2",
                        () -> new BatchListenerFailedException("testMessage", 1)
                )
                .failAtMessageNTimes(
                        "key2",
                        2,
                        () -> new BatchListenerFailedException("testMessage", 0)
                )
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .useDefaultRetryClassification()
                                .continueRetryOnExceptionChange()
                                .recoverFailedRecords((consumerRecord, e) -> {
                                    throw new RuntimeException();
                                })
                                .reprocessAndRetryRecordOnRecoveryFailure()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ),
                                1
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                2
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordRecoveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ),
                                1
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters14() {
        AtomicBoolean alreadyFailed = new AtomicBoolean(false);
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails")
                .andGiven("throws BatchListenerFailedException with index of failed record")
                .andGiven("is configured for retries")
                .andGiven("with failing custom recoverer")
                .andGiven("is configured to reprocess on recovery failure")
                .should("reprocess and skip retry")
                .numberOfMessages(4)
                .commitToWaitFor(4)
                .maxPollRecords(3)
                .failAtMessageOnce(
                        "key2",
                        () -> new BatchListenerFailedException("testMessage", 1)
                )
                .failAtMessageNTimes(
                        "key2",
                        2,
                        () -> new BatchListenerFailedException("testMessage", 0)
                )
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .useDefaultRetryClassification()
                                .continueRetryOnExceptionChange()
                                .recoverFailedRecords((consumerRecord, e) -> {
                                    if (!alreadyFailed.get()) {
                                        alreadyFailed.set(true);
                                        throw new RuntimeException();
                                    }
                                })
                                .reprocessRecordOnRecoveryFailure()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-1"),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ),
                                1
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                2
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordRecoveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3"),
                                                new KeyValueReport<>("key4", "value4")
                                        )
                                ),
                                1
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(BatchListenerFailedException.class, "testMessage @-0"),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                3
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordRecovered<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3"),
                                        new KeyValueReport<>("key4", "value4")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 4L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters15() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails")
                .andGiven("throws other exception than BatchListenerFailedException")
                .andGiven("is configured for retries")
                .andGiven("with failing custom recoverer")
                .andGiven("is configured to reprocess and retry on recovery failure")
                .should("reprocess and retry batch")
                .numberOfMessages(3)
                .commitToWaitFor(3)
                .maxPollRecords(3)
                .failAtMessageNTimes(
                        "key2",
                        3
                )
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .useDefaultRetryClassification()
                                .continueRetryOnExceptionChange()
                                .recoverFailedRecords((consumerRecord, e) -> {
                                    throw new RuntimeException();
                                })
                                .reprocessAndRetryRecordOnRecoveryFailure()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                2
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new BatchRecoveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerSuccessfullyProcessedBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters16() {
        AtomicBoolean alreadyFailed = new AtomicBoolean(false);
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("record processing that fails")
                .andGiven("throws other exception than BatchListenerFailedException")
                .andGiven("is configured for retries")
                .andGiven("with failing custom recoverer")
                .andGiven("is configured to reprocess on recovery failure")
                .should("reprocess and retry, as batch retry is not affected by retry skipping")
                .numberOfMessages(3)
                .commitToWaitFor(3)
                .maxPollRecords(3)
                .failAtMessageNTimes(
                        "key2",
                        4
                )
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .useDefaultRetryClassification()
                                .continueRetryOnExceptionChange()
                                .recoverFailedRecords((consumerRecord, e) -> {
                                    if (!alreadyFailed.get()) {
                                        alreadyFailed.set(true);
                                        throw new RuntimeException();
                                    }
                                })
                                .reprocessRecordOnRecoveryFailure()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                2
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new BatchRecoveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                1
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1"),
                                                new KeyValueReport<>("key2", "value2"),
                                                new KeyValueReport<>("key3", "value3")
                                        )
                                ),
                                2
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new BatchRecovered<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1"),
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters17() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("message processor throws exception that is excluded from retry")
                .should("not retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(1)
                .failAtMessageNTimes("key1", 5, IllegalArgumentException::new)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 5)
                                .excludeExceptionsFromRetry(List.of(
                                        IllegalArgumentException.class
                                ))
                                .continueRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                ),
                                1
                        ),
                        new BatchRecovered<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters18() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("message processor throws exception that is not excluded from retry")
                .should("retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(1)
                .failAtMessageNTimes("key1", 2, RuntimeException::new)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .excludeExceptionsFromRetry(List.of(
                                        IllegalArgumentException.class
                                ))
                                .continueRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                ),
                                1
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                ),
                                2
                        ),
                        new BatchRecovered<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters19() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("message processor throws exception that is classified as only retry Exception")
                .should("should retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(1)
                .failAtMessageNTimes("key1", 2, IllegalArgumentException::new)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .retryOnly(List.of(
                                        IllegalArgumentException.class
                                ))
                                .continueRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                ),
                                1
                        ),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                ),
                                2
                        ),
                        new BatchRecovered<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters20() {
        return ConsumingIntegrationTestParameters
                .batchStepBuilder()
                .given("message processor throws exception that is not included in only retry Exception")
                .should("not retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(1)
                .failAtMessageNTimes("key1", 5, RuntimeException::new)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 5)
                                .retryOnly(List.of(
                                        IllegalArgumentException.class
                                ))
                                .continueRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithBatch<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerFailedToProcessBatch<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                )
                        ),
                        new BatchDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                ),
                                1
                        ),
                        new BatchRecovered<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static Stream<ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String>> testParameters() {
        return Stream.of(
                testParameters1(),
                testParameters2(),
                testParameters3(),
                testParameters4(),
                testParameters5(),
                testParameters6(),
                testParameters7(),
                testParameters8(),
                testParameters9(),
                testParameters10(),
                testParameters11(),
                testParameters12(),
                testParameters13(),
                testParameters14(),
                testParameters15(),
                testParameters16(),
                testParameters17(),
                testParameters18(),
                testParameters19(),
                testParameters20()
        );
    }

    @MethodSource("testParameters")
    @ParameterizedTest
    void performTest(ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>, String> testParameters) {
        final String topic = topicNameGenerator.generateRandomTopicName();
        TopicPartitionReport topicPartition = new TopicPartitionReport(topic, 0);
        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                new OffsetCommittedPredicate<>(
                        topicPartition,
                        testParameters.getCommitToWaitFor()
                )
        );

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService
                        .createBatchListenerContainerFactory(
                                String.class,
                                testParameters.getMessageProcessor(),
                                consumerTrackingTools.wrapListenerConfigurationWithAssignmentTracking(
                                        ListenerConfiguration
                                                .stepBuilder()
                                                .groupIdApplicationDefault()
                                                .maxPollRecords(testParameters.getMaxPollRecords())
                                                .maxPollIntervalKafkaDefault()
                                                .seekToBeginningOnAssignment()
                                                .build()
                                ),
                                errorHandlerFactory.createErrorHandler(
                                        consumerTrackingTools.wrapRecovererWithTracking(
                                                testParameters.getErrorHandlerConfiguration()
                                        )
                                ),
                                consumerTrackingTools::registerContainerTracking
                        )
                        .createContainer(topic);

        IntStream
                .rangeClosed(1, testParameters.getNumberOfMessages())
                .forEach(i -> template.send(topic, "key" + i, "value" + i));

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForEventCondition(Duration.ofSeconds(30))).isTrue();
        assertThat(consumerTrackingTools.getEvents())
                .isEqualTo(
                        testParameters
                                .getExpectedEvents()
                                .apply(topicPartition)
                );
        listenerContainer.stop();
    }

}

package no.novari.kafka.consuming.integration;

import no.novari.kafka.TopicNameGenerator;
import no.novari.kafka.consumertracking.ConsumerTrackingService;
import no.novari.kafka.consumertracking.ConsumerTrackingTools;
import no.novari.kafka.consumertracking.event.CustomRecovererInvoked;
import no.novari.kafka.consumertracking.event.ListenerFailedToProcessRecord;
import no.novari.kafka.consumertracking.event.ListenerInvokedWithRecord;
import no.novari.kafka.consumertracking.event.ListenerSuccessfullyProcessedRecord;
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
public class RecordConsumerPollAndProcessIntegrationTest {
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

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters1() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("maxPollRecords>1")
                .should("poll multiple records at once")
                .andShould("consume records individually")
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters2() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("maxPollRecords=1")
                .should("poll records individually")
                .andShould("consume records individually")
                .andShould("commit records individually")
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters3() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails once")
                .andGiven("is configured for retry")
                .should("commit already processed records")
                .andShould("retry failed record successfully")
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters4() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails multiple times")
                .andGiven("is configured for more attempts than fails")
                .should("commit already processed records")
                .andShould("retry record until successfully processed")
                .numberOfMessages(3)
                .commitToWaitFor(3)
                .maxPollRecords(3)
                .failAtMessageNTimes("key2", 3)
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                2
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                3
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters5() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails multiple times")
                .andGiven("is configured for less retry attempts than fails and skip failed record")
                .should("retry until attempts are exhausted")
                .andShould("skip record")
                .numberOfMessages(3)
                .commitToWaitFor(3)
                .maxPollRecords(3)
                .failAtMessageNTimes("key2", 3)
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                2
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key2", "value2"),
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2"),
                                3
                        ),
                        new RecordRecovered<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters6() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails")
                .andGiven("is configured for no retries and skip failed records")
                .should("skip failed record")
                .numberOfMessages(3)
                .commitToWaitFor(3)
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L)),
                        new RecordRecovered<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 2L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters7() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails with different exceptions")
                .andGiven("is configured for retry that continue on exception change")
                .should("attempt retry")
                .andShould("continue retry on different exception")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(3)
                .failAtMessageNTimes("key1", 2, RuntimeException::new)
                .failAtMessageNTimes("key1", 1, IllegalArgumentException::new)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 3)
                                .useDefaultRetryClassification()
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                2
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                3
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters8() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails with different exceptions")
                .andGiven("is configured for retry that continue on exception change")
                .should("attempt retry")
                .andShould("restart retry on different exception")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(3)
                .failAtMessageNTimes("key1", 2, RuntimeException::new)
                .failAtMessageNTimes("key1", 1, IllegalArgumentException::new)
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
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                2
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters9() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails")
                .andGiven("is configured for no retries and custom recoverer")
                .should("call custom recoverer")
                .numberOfMessages(3)
                .commitToWaitFor(3)
                .maxPollRecords(3)
                .failAtMessageOnce("key2")
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
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
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters10() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails")
                .andGiven("is configured for no retries and failing custom recoverer with skip on recovery error")
                .should("attempt to recover record")
                .andShould("skip record")
                .numberOfMessages(3)
                .commitToWaitFor(3)
                .maxPollRecords(3)
                .failAtMessageNTimes("key2", 10)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .noRetries()
                                .recoverFailedRecords(
                                        ((consumerRecord, e) -> {
                                            throw new RuntimeException();
                                        })
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key2", "value2")
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
                                        new KeyValueReport<>("key3", "value3")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 3L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters11() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails")
                .andGiven("is configured for retries")
                .andGiven("with failing custom recoverer")
                .andGiven("is configured to reprocess and retry on recovery failure")
                .should("reprocess and retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(3)
                .failAtMessageNTimes("key1", 3)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .useDefaultRetryClassification()
                                .continueRetryOnExceptionChange()
                                .recoverFailedRecords(
                                        ((consumerRecord, e) -> {
                                            throw new RuntimeException();
                                        })
                                )
                                .reprocessAndRetryRecordOnRecoveryFailure()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                2
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordRecoveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters12() {
        AtomicBoolean alreadyFailed = new AtomicBoolean(false);
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("record processing that fails")
                .andGiven("is configured for retries")
                .andGiven("with failing custom recoverer")
                .andGiven("is configured to reprocess on recovery failure")
                .should("reprocess and skip retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(3)
                .failAtMessageNTimes("key1", 3)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .useDefaultRetryClassification()
                                .continueRetryOnExceptionChange()
                                .recoverFailedRecords(
                                        ((consumerRecord, e) -> {
                                            if (!alreadyFailed.get()) {
                                                alreadyFailed.set(true);
                                                throw new RuntimeException();
                                            }
                                        })
                                )
                                .reprocessRecordOnRecoveryFailure()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                        new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                2
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordRecoveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                3
                        ),
                        new CustomRecovererInvoked<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordRecovered<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters13() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("message processor throws exception that is excluded from retry")
                .should("not retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(3)
                .failAtMessageNTimes("key1", 5, IllegalArgumentException::new)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .excludeExceptionsFromRetry(List.of(
                                        IllegalArgumentException.class
                                ))
                                .restartRetryOnExceptionChange()
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(IllegalArgumentException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                1
                        ),
                        new RecordRecovered<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters14() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("message processor throws exception that is not excluded from retry")
                .should("retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(3)
                .failAtMessageNTimes("key1", 1, RuntimeException::new)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .excludeExceptionsFromRetry(List.of(
                                        IllegalArgumentException.class
                                ))
                                .restartRetryOnExceptionChange()
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                1
                        ),
                        new RecordsPolled<>(Map.of(
                                topicPartition, List.of(
                                        new KeyValueReport<>("key1", "value1")
                                )
                        )),
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters15() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("message processor throws exception that is classified as only retry Exception")
                .should("should retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(3)
                .failAtMessageNTimes("key1", 1, IllegalArgumentException::new)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .retryOnly(List.of(
                                        IllegalArgumentException.class
                                ))
                                .restartRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(topicPartition -> List.of(
                                new PartitionsAssigned<>(Map.of(topicPartition, 0L)),
                                new RecordsPolled<>(
                                        Map.of(topicPartition, List.of(new KeyValueReport<>("key1", "value1")))
                                ),
                                new ListenerInvokedWithRecord<>(
                                        topicPartition,
                                        new KeyValueReport<>("key1", "value1")
                                ),
                                new ListenerFailedToProcessRecord<>(
                                        new ExceptionReport(IllegalArgumentException.class, null),
                                        topicPartition,
                                        new KeyValueReport<>("key1", "value1")
                                ),
                                new RecordDeliveryFailed<>(
                                        new ExceptionReport(IllegalArgumentException.class, null),
                                        topicPartition,
                                        new KeyValueReport<>("key1", "value1"),
                                        1
                                ),
                                new RecordsPolled<>(Map.of(
                                        topicPartition, List.of(
                                                new KeyValueReport<>("key1", "value1")
                                        )
                                )),
                                new ListenerInvokedWithRecord<>(
                                        topicPartition,
                                        new KeyValueReport<>("key1", "value1")
                                ),
                                new ListenerSuccessfullyProcessedRecord<>(
                                        topicPartition,
                                        new KeyValueReport<>("key1", "value1")
                                ),
                                new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                        )
                )
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters16() {
        return ConsumingIntegrationTestParameters
                .recordStepBuilder()
                .given("message processor throws exception that is not included in only retry Exception")
                .should("not retry")
                .numberOfMessages(1)
                .commitToWaitFor(1)
                .maxPollRecords(3)
                .failAtMessageNTimes("key1", 5, RuntimeException::new)
                .errorHandlerConfiguration(
                        ErrorHandlerConfiguration
                                .<String>stepBuilder()
                                .retryWithFixedInterval(Duration.ofMillis(10), 1)
                                .retryOnly(List.of(
                                        IllegalArgumentException.class
                                ))
                                .restartRetryOnExceptionChange()
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
                        new ListenerInvokedWithRecord<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                new ExceptionReport(RuntimeException.class, null),
                                topicPartition,
                                new KeyValueReport<>("key1", "value1"),
                                1
                        ),
                        new RecordRecovered<>(
                                topicPartition,
                                new KeyValueReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(Map.of(topicPartition, 1L))
                ))
                .build();
    }

    static Stream<ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String>> testParameters() {
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
                testParameters16()
        );
    }

    @MethodSource("testParameters")
    @ParameterizedTest
    void performTest(ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, String> testParameters) {
        final String topic = topicNameGenerator.generateRandomTopicName();
        final TopicPartitionReport topicPartition = new TopicPartitionReport(topic, 0);
        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                new OffsetCommittedPredicate<>(topicPartition, testParameters.getCommitToWaitFor())
        );

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService
                        .createRecordListenerContainerFactory(
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
                                        consumerTrackingTools.wrapErrorHandlerConfigWithCustomRecovererTracking(
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

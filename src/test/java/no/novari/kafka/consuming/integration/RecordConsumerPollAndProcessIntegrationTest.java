package no.novari.kafka.consuming.integration;

import no.novari.kafka.TopicNameGenerator;
import no.novari.kafka.consumertracking.ConsumerTrackingService;
import no.novari.kafka.consumertracking.ConsumerTrackingTools;
import no.novari.kafka.consumertracking.RecordReport;
import no.novari.kafka.consumertracking.events.CustomRecovererInvoked;
import no.novari.kafka.consumertracking.events.ListenerFailedToProcessRecord;
import no.novari.kafka.consumertracking.events.ListenerInvokedWithRecord;
import no.novari.kafka.consumertracking.events.ListenerSuccessfullyProcessedRecord;
import no.novari.kafka.consumertracking.events.OffsetsCommitted;
import no.novari.kafka.consumertracking.events.RecordDeliveryFailed;
import no.novari.kafka.consumertracking.events.RecordRecovered;
import no.novari.kafka.consumertracking.events.RecordRecoveryFailed;
import no.novari.kafka.consumertracking.events.RecordsPolled;
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

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters1() {
        return
                ConsumingIntegrationTestParameters
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
                        .expectedEvents(
                                List.of(
                                        new RecordsPolled<>(
                                                new RecordReport<>("key1", "value1"),
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3")
                                        ),
                                        new ListenerInvokedWithRecord<>(
                                                new RecordReport<>("key1", "value1")
                                        ),
                                        new ListenerSuccessfullyProcessedRecord<>(
                                                new RecordReport<>("key1", "value1")
                                        ),
                                        new ListenerInvokedWithRecord<>(
                                                new RecordReport<>("key2", "value2")
                                        ),
                                        new ListenerSuccessfullyProcessedRecord<>(
                                                new RecordReport<>("key2", "value2")
                                        ),
                                        new ListenerInvokedWithRecord<>(
                                                new RecordReport<>("key3", "value3")
                                        ),
                                        new ListenerSuccessfullyProcessedRecord<>(
                                                new RecordReport<>("key3", "value3")
                                        ),
                                        new OffsetsCommitted<>(3L)
                                ))
                        .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters2() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(1L),
                        new RecordsPolled<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(2L),
                        new RecordsPolled<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(3L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters3() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(1L),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(3L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters4() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(1L),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                2,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                3,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(3L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters5() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")

                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(1L),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")

                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                2,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")

                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                3,
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordRecovered<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(2L),
                        new RecordsPolled<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(3L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters6() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")

                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(1L),
                        new RecordRecovered<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(2L),
                        new RecordsPolled<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(3L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters7() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                2,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                IllegalArgumentException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                IllegalArgumentException.class,
                                null,
                                3,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(1L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters8() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                2,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                IllegalArgumentException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                IllegalArgumentException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(1L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters9() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")

                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(1L),
                        new CustomRecovererInvoked<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordRecovered<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(2L),
                        new RecordsPolled<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(3L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters10() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1"),
                                new RecordReport<>("key2", "value2"),
                                new RecordReport<>("key3", "value3")

                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(1L),
                        new CustomRecovererInvoked<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new RecordRecovered<>(
                                new RecordReport<>("key2", "value2")
                        ),
                        new OffsetsCommitted<>(2L),
                        new RecordsPolled<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key3", "value3")
                        ),
                        new OffsetsCommitted<>(3L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters11() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                2,
                                new RecordReport<>("key1", "value1")
                        ),
                        new CustomRecovererInvoked<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordRecoveryFailed<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(1L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters12() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                2,
                                new RecordReport<>("key1", "value1")
                        ),
                        new CustomRecovererInvoked<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordRecoveryFailed<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                3,
                                new RecordReport<>("key1", "value1")
                        ),
                        new CustomRecovererInvoked<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordRecovered<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(1L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters13() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                IllegalArgumentException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                IllegalArgumentException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordRecovered<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(1L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters14() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(1L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters15() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                IllegalArgumentException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                IllegalArgumentException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerSuccessfullyProcessedRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(1L)
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>
    testParameters16() {
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
                .expectedEvents(List.of(
                        new RecordsPolled<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerInvokedWithRecord<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new ListenerFailedToProcessRecord<>(
                                RuntimeException.class,
                                null,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordDeliveryFailed<>(
                                RuntimeException.class,
                                null,
                                1,
                                new RecordReport<>("key1", "value1")
                        ),
                        new RecordRecovered<>(
                                new RecordReport<>("key1", "value1")
                        ),
                        new OffsetsCommitted<>(1L)
                ))
                .build();
    }

    static Stream<ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String, String>>>
    testParameters() {
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
    void performTest(
            ConsumingIntegrationTestParameters<ConsumerRecord<String, String>, ConsumerRecord<String,
                    String>> testParameters
    ) {
        final String topic = topicNameGenerator.generateRandomTopicName();
        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                testParameters.getCommitToWaitFor()
        );

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService
                        .createRecordListenerContainerFactory(
                                String.class,
                                testParameters.getMessageProcessor(),
                                ListenerConfiguration
                                        .stepBuilder()
                                        .groupIdApplicationDefault()
                                        .maxPollRecords(testParameters.getMaxPollRecords())
                                        .maxPollIntervalKafkaDefault()
                                        .seekToBeginningOnAssignment()
                                        .build(),
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

        assertThat(consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(30))).isTrue();
        assertThat(consumerTrackingTools.getEvents()).isEqualTo(testParameters.getExpectedEvents());
    }

}

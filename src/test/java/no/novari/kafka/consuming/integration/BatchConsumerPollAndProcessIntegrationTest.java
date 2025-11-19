package no.novari.kafka.consuming.integration;

import no.novari.kafka.consumertracking.ConsumerTrackingService;
import no.novari.kafka.consumertracking.ConsumerTrackingTools;
import no.novari.kafka.consumertracking.events.BatchDeliveryFailedReport;
import no.novari.kafka.consumertracking.events.Event;
import no.novari.kafka.consumertracking.events.ExceptionReport;
import no.novari.kafka.consumertracking.events.OffsetReport;
import no.novari.kafka.consumertracking.events.RecordDeliveryFailedReport;
import no.novari.kafka.consumertracking.events.RecordReport;
import no.novari.kafka.consumertracking.events.RecordsExceptionReport;
import no.novari.kafka.consumertracking.events.RecordsReport;
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
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext
public class BatchConsumerPollAndProcessIntegrationTest {
    private static final Random random = new Random(42);
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

    static Stream<TestParameters<List<ConsumerRecord<String, String>>>> testParameters() {
        return Stream.of(
                TestParameters
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
                        .expectedEvents(List.of(
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
                                Event.listenerSuccessfullyProcessedBatch(
                                        new RecordsReport<>(List.of(
                                                new RecordReport<>("key1", "value1"),
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3")
                                        ))
                                ),
                                Event.offsetsCommited(
                                        new OffsetReport<>(3L)
                                )
                        ))
                        .build(),

                TestParameters
                        .batchStepBuilder()
                        .given("maxPollRecords=1")
                        .should("poll single record")
                        .andShould("consume single record batch")
                        .andShould("commit single record batch")
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
                                Event.recordsPolled(
                                        new RecordsReport<>(List.of(new RecordReport<>("key1", "value1")))
                                ),
                                Event.listenerInvokedWithBatch(
                                        new RecordsReport<>(List.of(new RecordReport<>("key1", "value1")))
                                ),
                                Event.listenerSuccessfullyProcessedBatch(
                                        new RecordsReport<>(List.of(new RecordReport<>("key1", "value1")))
                                ),
                                Event.offsetsCommited(
                                        new OffsetReport<>(1L)
                                ),
                                Event.recordsPolled(
                                        new RecordsReport<>(List.of(new RecordReport<>("key2", "value2")))
                                ),
                                Event.listenerInvokedWithBatch(
                                        new RecordsReport<>(List.of(new RecordReport<>("key2", "value2")))
                                ),
                                Event.listenerSuccessfullyProcessedBatch(
                                        new RecordsReport<>(List.of(new RecordReport<>("key2", "value2")))
                                ),
                                Event.offsetsCommited(
                                        new OffsetReport<>(2L)
                                ), Event.recordsPolled(
                                        new RecordsReport<>(List.of(new RecordReport<>("key3", "value3")))
                                ),
                                Event.listenerInvokedWithBatch(
                                        new RecordsReport<>(List.of(new RecordReport<>("key3", "value3")))
                                ),
                                Event.listenerSuccessfullyProcessedBatch(
                                        new RecordsReport<>(List.of(new RecordReport<>("key3", "value3")))
                                ),
                                Event.offsetsCommited(
                                        new OffsetReport<>(3L)
                                )
                        ))
                        .build(),

                TestParameters
                        .batchStepBuilder()
                        .given("record processing that fails once")
                        .andGiven("throws BatchListenerFailedException with index of failed record")
                        .andGiven("is configured for retry")
                        .should("commit already processed records")
                        .andShould("retry rest of batch")
                        .numberOfMessages(4)
                        .commitToWaitFor(3)
                        .maxPollRecords(3)
                        .failAtMessageOnce(3)
                        .customException(() -> new BatchListenerFailedException("testMessage", 2))
                        .errorHandlerConfiguration(
                                ErrorHandlerConfiguration
                                        .<String>stepBuilder()
                                        .retryWithFixedInterval(Duration.ofMillis(100), 1)
                                        .skipFailedRecords()
                                        .build()
                        )
                        .expectedEvents(List.of(
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
                                        new BatchDeliveryFailedReport<>(
                                                new RecordsReport<>(List.of(
                                                        new RecordReport<>("key1", "value1"),
                                                        new RecordReport<>("key2", "value2"),
                                                        new RecordReport<>("key3", "value3")
                                                )),
                                                new ExceptionReport<>(
                                                        ListenerExecutionFailedException.class,
                                                        "Listener failed"
                                                ),
                                                1
                                        )
                                ),
                                Event.offsetsCommited(
                                        new OffsetReport<>(2L)
                                ),
                                Event.recordDeliveryFailed(
                                        new RecordDeliveryFailedReport<>(
                                                new RecordReport<>("key3", "value3"),
                                                new ExceptionReport<>(
                                                        ListenerExecutionFailedException.class,
                                                        "Listener failed"
                                                ),
                                                1
                                        )
                                ),
                                Event.recordsPolled(
                                        new RecordsReport<>(List.of(
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        ))
                                ),
                                Event.listenerInvokedWithBatch(
                                        new RecordsReport<>(List.of(
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        ))
                                ),
                                Event.listenerSuccessfullyProcessedBatch(
                                        new RecordsReport<>(List.of(
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        ))
                                ),
                                Event.offsetsCommited(
                                        new OffsetReport<>(4L)
                                )
                        ))
                        .build(),

                TestParameters
                        .batchStepBuilder()
                        .given("record processing that fails once")
                        .andGiven("throws other exception than BatchListenerFailedException")
                        .andGiven("is configured for retry")
                        .should("retry whole batch")
                        .numberOfMessages(3)
                        .commitToWaitFor(3)
                        .maxPollRecords(3)
                        .failAtMessageOnce(2)
                        .errorHandlerConfiguration(
                                ErrorHandlerConfiguration
                                        .<String>stepBuilder()
                                        .retryWithFixedInterval(Duration.ofMillis(100), 1)
                                        .skipFailedRecords()
                                        .build()
                        )
                        .expectedEvents(List.of(
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
                                        new BatchDeliveryFailedReport<>(
                                                new RecordsReport<>(List.of(
                                                        new RecordReport<>("key1", "value1"),
                                                        new RecordReport<>("key2", "value2"),
                                                        new RecordReport<>("key3", "value3")
                                                )),
                                                new ExceptionReport<>(
                                                        ListenerExecutionFailedException.class,
                                                        "Listener failed"
                                                ),
                                                1
                                        )
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
                                Event.offsetsCommited(
                                        new OffsetReport<>(3L)
                                )
                        ))
                        .build()

                // TODO 18/11/2025 eivindmorch: Fix final test cases

//                TestParameters
//                        .batchStepBuilder()
//                        .given("record processing that fails multiple times")
//                        .andGiven("is configured for more retry attempts than fails")
//                        .should("commit already processed records")
//                        .andShould("retry record until successfully processed")
//                        .numberOfMessages(3)
//                        .commitToWaitFor(3)
//                        .maxPollRecords(3)
//                        .failAtMessageNTimes(2, 3)
//                        .errorHandlerConfiguration(
//                                ErrorHandlerConfiguration
//                                        .<String>stepBuilder()
//                                        .retryWithFixedInterval(Duration.ofMillis(100), 3)
//                                        .skipFailedRecords()
//                                        .build()
//                        )
//                        .expectedEvents(List.of(
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key1", "value1"),
//                                                new RecordReport<>("key2", "value2"),
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key1", "value1")
//                                ),
//                                Event.listenerSuccessfullyProcessedRecord(
//                                        new RecordReport<>("key1", "value1")
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerFailedToProcessedRecord(
//                                        new RecordExceptionReport<>(
//                                                new RecordReport<>("key2", "value2"),
//                                                new ExceptionReport<>(
//                                                        ListenerExecutionFailedException.class,
//                                                        "Listener failed"
//                                                )
//                                        )
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(1L)
//                                ),
//                                Event.recordDeliveryFailed(
//                                        RecordDeliveryFailedReport
//                                                .<String>builder()
//                                                .record(new RecordReport<>("key2", "value2"))
//                                                .cause(
//                                                        new ExceptionReport<>(
//                                                                ListenerExecutionFailedException.class,
//                                                                "Listener failed"
//                                                        )
//                                                )
//                                                .attempt(1)
//                                                .build()
//                                ),
//
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key2", "value2"),
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerFailedToProcessedRecord(
//                                        new RecordExceptionReport<>(
//                                                new RecordReport<>("key2", "value2"),
//                                                new ExceptionReport<>(
//                                                        ListenerExecutionFailedException.class,
//                                                        "Listener failed"
//                                                )
//                                        )
//                                ),
//                                Event.recordDeliveryFailed(
//                                        RecordDeliveryFailedReport
//                                                .<String>builder()
//                                                .record(new RecordReport<>("key2", "value2"))
//                                                .cause(
//                                                        new ExceptionReport<>(
//                                                                ListenerExecutionFailedException.class,
//                                                                "Listener failed"
//                                                        )
//                                                )
//                                                .attempt(2)
//                                                .build()
//                                ),
//
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key2", "value2"),
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerFailedToProcessedRecord(
//                                        new RecordExceptionReport<>(
//                                                new RecordReport<>("key2", "value2"),
//                                                new ExceptionReport<>(
//                                                        ListenerExecutionFailedException.class,
//                                                        "Listener failed"
//                                                )
//                                        )
//                                ),
//                                Event.recordDeliveryFailed(
//                                        RecordDeliveryFailedReport
//                                                .<String>builder()
//                                                .record(new RecordReport<>("key2", "value2"))
//                                                .cause(
//                                                        new ExceptionReport<>(
//                                                                ListenerExecutionFailedException.class,
//                                                                "Listener failed"
//                                                        )
//                                                )
//                                                .attempt(3)
//                                                .build()
//                                ),
//
//
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key2", "value2"),
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerSuccessfullyProcessedRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key3", "value3")
//                                ),
//                                Event.listenerSuccessfullyProcessedRecord(
//                                        new RecordReport<>("key3", "value3")
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(3L)
//                                )
//                        ))
//                        .build(),
//
//                TestParameters
//                        .batchStepBuilder()
//                        .given("record processing that fails multiple times")
//                        .andGiven("is configured for less retry attempts than fails")
//                        .should("retry until attempts are exhausted")
//                        .andShould("recover record")
//                        .numberOfMessages(3)
//                        .commitToWaitFor(3)
//                        .maxPollRecords(3)
//                        .failAtMessageNTimes(2, 3)
//                        .errorHandlerConfiguration(
//                                ErrorHandlerConfiguration
//                                        .<String>stepBuilder()
//                                        .retryWithFixedInterval(Duration.ofMillis(100), 2)
//                                        .skipFailedRecords()
//                                        .build()
//                        )
//                        .expectedEvents(List.of(
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key1", "value1"),
//                                                new RecordReport<>("key2", "value2"),
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key1", "value1")
//                                ),
//                                Event.listenerSuccessfullyProcessedRecord(
//                                        new RecordReport<>("key1", "value1")
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerFailedToProcessedRecord(
//                                        new RecordExceptionReport<>(
//                                                new RecordReport<>("key2", "value2"),
//                                                new ExceptionReport<>(
//                                                        ListenerExecutionFailedException.class,
//                                                        "Listener failed"
//                                                )
//                                        )
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(1L)
//                                ),
//                                Event.recordDeliveryFailed(
//                                        RecordDeliveryFailedReport
//                                                .<String>builder()
//                                                .record(new RecordReport<>("key2", "value2"))
//                                                .cause(
//                                                        new ExceptionReport<>(
//                                                                ListenerExecutionFailedException.class,
//                                                                "Listener failed"
//                                                        )
//                                                )
//                                                .attempt(1)
//                                                .build()
//                                ),
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key2", "value2"),
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerFailedToProcessedRecord(
//                                        new RecordExceptionReport<>(
//                                                new RecordReport<>("key2", "value2"),
//                                                new ExceptionReport<>(
//                                                        ListenerExecutionFailedException.class,
//                                                        "Listener failed"
//                                                )
//                                        )
//                                ),
//                                Event.recordDeliveryFailed(
//                                        RecordDeliveryFailedReport
//                                                .<String>builder()
//                                                .record(new RecordReport<>("key2", "value2"))
//                                                .cause(
//                                                        new ExceptionReport<>(
//                                                                ListenerExecutionFailedException.class,
//                                                                "Listener failed"
//                                                        )
//                                                )
//                                                .attempt(2)
//                                                .build()
//                                ),
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key2", "value2"),
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerFailedToProcessedRecord(
//                                        new RecordExceptionReport<>(
//                                                new RecordReport<>("key2", "value2"),
//                                                new ExceptionReport<>(
//                                                        ListenerExecutionFailedException.class,
//                                                        "Listener failed"
//                                                )
//                                        )
//                                ),
//                                Event.recordDeliveryFailed(
//                                        RecordDeliveryFailedReport
//                                                .<String>builder()
//                                                .record(new RecordReport<>("key2", "value2"))
//                                                .cause(
//                                                        new ExceptionReport<>(
//                                                                ListenerExecutionFailedException.class,
//                                                                "Listener failed"
//                                                        )
//                                                )
//                                                .attempt(3)
//                                                .build()
//                                ),
//                                Event.recordRecovered(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.offsetsCommited(new OffsetReport<>(2L)),
//
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key3", "value3")
//                                ),
//                                Event.listenerSuccessfullyProcessedRecord(
//                                        new RecordReport<>("key3", "value3")
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(3L)
//                                )
//                        ))
//                        .build(),
//
//                TestParameters
//                        .batchStepBuilder()
//                        .given("record processing that fails")
//                        .andGiven("is configured for no retries")
//                        .should("recover record")
//                        .numberOfMessages(3)
//                        .commitToWaitFor(3)
//                        .maxPollRecords(3)
//                        .failAtMessageOnce(2)
//                        .errorHandlerConfiguration(
//                                ErrorHandlerConfiguration
//                                        .<String>stepBuilder()
//                                        .noRetries()
//                                        .skipFailedRecords()
//                                        .build()
//                        )
//                        .expectedEvents(List.of(
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key1", "value1"),
//                                                new RecordReport<>("key2", "value2"),
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key1", "value1")
//                                ),
//                                Event.listenerSuccessfullyProcessedRecord(
//                                        new RecordReport<>("key1", "value1")
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerFailedToProcessedRecord(
//                                        new RecordExceptionReport<>(
//                                                new RecordReport<>("key2", "value2"),
//                                                new ExceptionReport<>(
//                                                        ListenerExecutionFailedException.class,
//                                                        "Listener failed"
//                                                )
//                                        )
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(1L)
//                                ),
//                                Event.recordRecovered(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(2L)
//                                ),
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key3", "value3")
//                                ),
//                                Event.listenerSuccessfullyProcessedRecord(
//                                        new RecordReport<>("key3", "value3")
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(3L)
//                                )
//                        ))
//                        .build(),
//
//                TestParameters
//                        .batchStepBuilder()
//                        .given("record processing that fails")
//                        .andGiven("is configured for no retries and custom recoverer")
//                        .should("recover record")
//                        .numberOfMessages(3)
//                        .commitToWaitFor(3)
//                        .maxPollRecords(3)
//                        .failAtMessageOnce(2)
//                        .errorHandlerConfiguration(
//                                ErrorHandlerConfiguration
//                                        .<String>stepBuilder()
//                                        .noRetries()
//                                        .handleFailedRecords(((consumerRecord, e) -> {
//                                        }))
//                                        .build()
//                        )
//                        .expectedEvents(List.of(
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key1", "value1"),
//                                                new RecordReport<>("key2", "value2"),
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key1", "value1")
//                                ),
//                                Event.listenerSuccessfullyProcessedRecord(
//                                        new RecordReport<>("key1", "value1")
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.listenerFailedToProcessedRecord(
//                                        new RecordExceptionReport<>(
//                                                new RecordReport<>("key2", "value2"),
//                                                new ExceptionReport<>(
//                                                        ListenerExecutionFailedException.class,
//                                                        "Listener failed"
//                                                )
//                                        )
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(1L)
//                                ),
//                                Event.recordRecovered(
//                                        new RecordReport<>("key2", "value2")
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(2L)
//                                ),
//                                Event.recordsPolled(
//                                        new RecordsReport<>(List.of(
//                                                new RecordReport<>("key3", "value3")
//                                        ))
//                                ),
//                                Event.listenerInvokedWithRecord(
//                                        new RecordReport<>("key3", "value3")
//                                ),
//                                Event.listenerSuccessfullyProcessedRecord(
//                                        new RecordReport<>("key3", "value3")
//                                ),
//                                Event.offsetsCommited(
//                                        new OffsetReport<>(3L)
//                                )
//                        ))
//                        .build()
        );
    }

    @MethodSource("testParameters")
    @ParameterizedTest
    void performTest(TestParameters<List<ConsumerRecord<String, String>>> testParameters) {
        final String topic = generateRandomTopicName();
        ConsumerTrackingTools<String> consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                testParameters.getCommitToWaitFor()
        );

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService
                        .createBatchListenerContainerFactory(
                                String.class,
                                testParameters.getMessageProcessor(),
                                ListenerConfiguration
                                        .stepBuilder()
                                        .groupIdApplicationDefault()
                                        .maxPollRecords(testParameters.getMaxPollRecords())
                                        .maxPollIntervalKafkaDefault()
                                        .seekToBeginningOnAssignment()
                                        .build(),
                                errorHandlerFactory.createErrorHandler(testParameters.getErrorHandlerConfiguration()),
                                consumerTrackingTools::registerTracking
                        )
                        .createContainer(topic);

        IntStream
                .rangeClosed(1, testParameters.getNumberOfMessages())
                .forEach(i -> template.send(topic, "key" + i, "value" + i));

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(Duration.ofSeconds(30))).isTrue();
        assertThat(consumerTrackingTools.getEvents()).isEqualTo(testParameters.getExpectedEvents());
    }

    private String generateRandomTopicName() {
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        return UUID
                .nameUUIDFromBytes(bytes)
                .toString();
    }

}

package no.novari.kafka.consuming.integration;

import no.novari.kafka.TopicNameGenerator;
import no.novari.kafka.consumertracking.ConsumerTrackingService;
import no.novari.kafka.consumertracking.ConsumerTrackingTools;
import no.novari.kafka.consumertracking.events.Event;
import no.novari.kafka.consumertracking.events.ExceptionReport;
import no.novari.kafka.consumertracking.events.OffsetReport;
import no.novari.kafka.consumertracking.events.RecordDeliveryFailedReport;
import no.novari.kafka.consumertracking.events.RecordExceptionReport;
import no.novari.kafka.consumertracking.events.RecordReport;
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
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
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

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters1() {
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
                        .expectedEvents(List.of(
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
                        ))
                        .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters2() {
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
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(new RecordReport<>("key1", "value1")))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerSuccessfullyProcessedRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(1L)
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(new RecordReport<>("key2", "value2")))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key2", "value2")
                        ),
                        Event.listenerSuccessfullyProcessedRecord(
                                new RecordReport<>("key2", "value2")
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(2L)
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(new RecordReport<>("key3", "value3")))
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
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters3() {
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
                                .restartRetryOnExceptionChange()
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
                                        .<String>builder()
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
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters4() {
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
                                .restartRetryOnExceptionChange()
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
                                        .<String>builder()
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
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                RecordDeliveryFailedReport
                                        .<String>builder()
                                        .record(new RecordReport<>("key2", "value2"))
                                        .cause(
                                                new ExceptionReport<>(
                                                        ListenerExecutionFailedException.class,
                                                        "Listener failed"
                                                )
                                        )
                                        .attempt(2)
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
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                RecordDeliveryFailedReport
                                        .<String>builder()
                                        .record(new RecordReport<>("key2", "value2"))
                                        .cause(
                                                new ExceptionReport<>(
                                                        ListenerExecutionFailedException.class,
                                                        "Listener failed"
                                                )
                                        )
                                        .attempt(3)
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
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters5() {
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
                                .restartRetryOnExceptionChange()
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
                                        .<String>builder()
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
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                RecordDeliveryFailedReport
                                        .<String>builder()
                                        .record(new RecordReport<>("key2", "value2"))
                                        .cause(
                                                new ExceptionReport<>(
                                                        ListenerExecutionFailedException.class,
                                                        "Listener failed"
                                                )
                                        )
                                        .attempt(2)
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
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                RecordDeliveryFailedReport
                                        .<String>builder()
                                        .record(new RecordReport<>("key2", "value2"))
                                        .cause(
                                                new ExceptionReport<>(
                                                        ListenerExecutionFailedException.class,
                                                        "Listener failed"
                                                )
                                        )
                                        .attempt(3)
                                        .build()
                        ),
                        Event.recordRecovered(
                                new RecordReport<>("key2", "value2")
                        ),
                        Event.offsetsCommited(new OffsetReport<>(2L)),

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
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters6() {
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
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters7() {
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
                                .continueRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(List.of(
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        2
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        3
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerSuccessfullyProcessedRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(1L)
                        )
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters8() {
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
                                .restartRetryOnExceptionChange()
                                .skipFailedRecords()
                                .build()
                )
                .expectedEvents(List.of(
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        2
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerSuccessfullyProcessedRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(1L)
                        )
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters9() {
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
                        Event.customRecovererInvoked(
                                new RecordReport<>("key2", "value2")
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
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters10() {
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
                        Event.customRecovererInvoked(
                                new RecordReport<>("key2", "value2")
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
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters11() {
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
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        2
                                )
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.recordRecoveryFailed(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                RuntimeException.class,
                                                null
                                        )
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerSuccessfullyProcessedRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(1L)
                        )
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters12() {
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
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        2
                                )
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.recordRecoveryFailed(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                RuntimeException.class,
                                                null
                                        )
                                )
                        ),
                        Event.recordsPolled(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1")
                                ))
                        ),
                        Event.listenerInvokedWithRecord(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.listenerFailedToProcessedRecord(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key1", "value1"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        3
                                )
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.recordRecovered(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(1L)
                        )
                ))
                .build();
    }

    static Stream<ConsumingIntegrationTestParameters<ConsumerRecord<String, String>>> testParameters() {
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
                testParameters12()
        );
    }

    @MethodSource("testParameters")
    @ParameterizedTest
    void performTest(ConsumingIntegrationTestParameters<ConsumerRecord<String, String>> testParameters) {
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

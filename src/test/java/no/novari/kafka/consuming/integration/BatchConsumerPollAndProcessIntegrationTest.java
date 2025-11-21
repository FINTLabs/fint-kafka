package no.novari.kafka.consuming.integration;

import no.novari.kafka.TopicNameGenerator;
import no.novari.kafka.consumertracking.ConsumerTrackingService;
import no.novari.kafka.consumertracking.ConsumerTrackingTools;
import no.novari.kafka.consumertracking.events.BatchDeliveryFailedReport;
import no.novari.kafka.consumertracking.events.Event;
import no.novari.kafka.consumertracking.events.ExceptionReport;
import no.novari.kafka.consumertracking.events.OffsetReport;
import no.novari.kafka.consumertracking.events.RecordDeliveryFailedReport;
import no.novari.kafka.consumertracking.events.RecordExceptionReport;
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

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters1() {
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
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters2() {
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
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters3() {
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
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters4() {
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
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters5() {
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
                        Event.listenerFailedToProcessedBatch(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
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
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        )),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key3", "value3"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        2
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
                        Event.listenerFailedToProcessedBatch(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
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
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        )),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key3", "value3"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        3
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
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters6() {
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
                                        2
                                )
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
                                        3
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
                        ),

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
                        Event.offsetsCommited(
                                new OffsetReport<>(4L)
                        )
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters7() {
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
                        Event.listenerFailedToProcessedBatch(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
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
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        )),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key3", "value3"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        2
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
                        Event.listenerFailedToProcessedBatch(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
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
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        )),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key3", "value3"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        3
                                )
                        ),
                        Event.recordRecovered(
                                new RecordReport<>("key3", "value3")
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(3L)
                        ),
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
                        Event.offsetsCommited(
                                new OffsetReport<>(4L)
                        )
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters8() {
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
                                        2
                                )
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
                                        3
                                )
                        ),
                        Event.batchRecovered(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                ))
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(3L)
                        ),
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
                        Event.offsetsCommited(
                                new OffsetReport<>(4L)
                        )
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters9() {
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
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters10() {
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
                        Event.batchRecovered(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                ))
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(3L)
                        ),
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
                        Event.offsetsCommited(
                                new OffsetReport<>(4L)
                        )
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters11() {
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
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters12() {
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
                        Event.customRecovererInvoked(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key2", "value2")
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key3", "value3")
                        ),
                        Event.batchRecovered(
                                new RecordsReport<>(List.of(
                                        new RecordReport<>("key1", "value1"),
                                        new RecordReport<>("key2", "value2"),
                                        new RecordReport<>("key3", "value3")
                                ))
                        ),
                        Event.offsetsCommited(
                                new OffsetReport<>(3L)
                        ),
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
                        Event.offsetsCommited(
                                new OffsetReport<>(4L)
                        )
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters13() {
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
                                .continueRetryOnExceptionChange()
                                .recoverFailedRecords((consumerRecord, e) -> {
                                    throw new RuntimeException();
                                })
                                .reprocessAndRetryRecordOnRecoveryFailure()
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
                                new OffsetReport<>(1L)
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
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
                        Event.listenerFailedToProcessedBatch(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
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
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        )),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        2
                                )
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key2", "value2")
                        ),
                        Event.recordRecoveryFailed(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                RuntimeException.class,
                                                null
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
                        Event.listenerFailedToProcessedBatch(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
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
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        )),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
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
                        Event.offsetsCommited(
                                new OffsetReport<>(4L)
                        )
                ))
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters14() {
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
                                new OffsetReport<>(1L)
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
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
                        Event.listenerFailedToProcessedBatch(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
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
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        )),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        2
                                )
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key2", "value2")
                        ),
                        Event.recordRecoveryFailed(
                                new RecordExceptionReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                RuntimeException.class,
                                                null
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
                        Event.listenerFailedToProcessedBatch(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
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
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3"),
                                                new RecordReport<>("key4", "value4")
                                        )),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        1
                                )
                        ),
                        Event.recordDeliveryFailed(
                                new RecordDeliveryFailedReport<>(
                                        new RecordReport<>("key2", "value2"),
                                        new ExceptionReport<>(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        ),
                                        3
                                )
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
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters15() {
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
                                .continueRetryOnExceptionChange()
                                .recoverFailedRecords((consumerRecord, e) -> {
                                    throw new RuntimeException();
                                })
                                .reprocessAndRetryRecordOnRecoveryFailure()
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
                                        2
                                )
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.batchRecoveryFailed(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key1", "value1"),
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3")
                                        ),
                                        new ExceptionReport<>(
                                                RuntimeException.class,
                                                null
                                        )
                                )
                        ),
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
                .build();
    }

    static ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters16() {
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
                                        2
                                )
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.batchRecoveryFailed(
                                new RecordsExceptionReport<>(
                                        List.of(
                                                new RecordReport<>("key1", "value1"),
                                                new RecordReport<>("key2", "value2"),
                                                new RecordReport<>("key3", "value3")
                                        ),
                                        new ExceptionReport<>(
                                                RuntimeException.class,
                                                null
                                        )
                                )
                        ),
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
                                        2
                                )
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key1", "value1")
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key2", "value2")
                        ),
                        Event.customRecovererInvoked(
                                new RecordReport<>("key3", "value3")
                        ),
                        Event.batchRecovered(
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
                .build();
    }

    static Stream<ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>>> testParameters() {
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
    void performTest(ConsumingIntegrationTestParameters<List<ConsumerRecord<String, String>>> testParameters) {
        final String topic = topicNameGenerator.generateRandomTopicName();
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

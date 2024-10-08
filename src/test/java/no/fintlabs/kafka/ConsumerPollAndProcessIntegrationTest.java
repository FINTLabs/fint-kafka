package no.fintlabs.kafka;

import no.fintlabs.kafka.consuming.ListenerConfiguration;
import no.fintlabs.kafka.consuming.ListenerContainerFactoryService;
import no.fintlabs.kafka.producing.TemplateFactory;
import no.fintlabs.kafka.utils.consumertracking.ConsumerTrackingReport;
import no.fintlabs.kafka.utils.consumertracking.ConsumerTrackingService;
import no.fintlabs.kafka.utils.consumertracking.ConsumerTrackingTools;
import no.fintlabs.kafka.utils.consumertracking.reports.ExceptionReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordExceptionReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordsExceptionReport;
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

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@EmbeddedKafka(partitions = 1)
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

    private void sleep(long timeout, TimeUnit timeUnit) {
        try {
            timeUnit.sleep(timeout);
        } catch (InterruptedException e) {
            fail("Sleep interrupted", e);
        }
    }

    @Test
    public void recordConsumerShouldPollMultipleMessagesAtOnceAndConsumeEachRecordIndividuallyAndCommitRecordsInBatch() {
        final String topic = "test-topic-1";
        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                        String.class,
                        consumerRecord -> {
                        },
                        ListenerConfiguration
                                .builder()
                                .maxPollRecords(3)
                                .errorHandler(consumerTrackingTools.getErrorHandler())
                                .build(),
                        consumerTrackingTools::registerInterceptors
                ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(10, TimeUnit.SECONDS)).isTrue();
        assertThat(consumerTrackingTools.getContinuouslyUpdatedConsumeReportsOrderedChronologically()).isEqualTo(List.of(
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .listenerSuccessRecords(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .committedOffsets(List.of(3L))
                        .build()
        ));
    }

    @Test
    public void givenErrorDuringRecordProcessingRecordConsumerShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
        final String topic = "test-topic-2";

        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );

        AtomicBoolean hasAlreadyFailed = new AtomicBoolean(false);

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                        String.class,
                        consumerRecord -> {
                            if (consumerRecord.key().equals("key2") && !hasAlreadyFailed.get()) {
                                hasAlreadyFailed.set(true);
                                throw new RuntimeException();
                            }
                        },
                        ListenerConfiguration
                                .builder()
                                .errorHandler(consumerTrackingTools.getErrorHandler())
                                .build(),
                        consumerTrackingTools::registerInterceptors
                ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(10, TimeUnit.SECONDS)).isTrue();
        assertThat(consumerTrackingTools.getContinuouslyUpdatedConsumeReportsOrderedChronologically()).isEqualTo(List.of(
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .listenerSuccessRecords(List.of(new RecordReport("key1", "value1")))
                        .listenerFailureRecords(List.of(
                                new RecordExceptionReport(
                                        new RecordReport("key2", "value2"),
                                        new ExceptionReport(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ))
                        .errorHandlerHandleRemainingCalls(List.of(
                                new RecordsExceptionReport(
                                        List.of(
                                                new RecordReport("key2", "value2"),
                                                new RecordReport("key3", "value3")
                                        ),
                                        new ExceptionReport(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ))
                        .committedOffsets(List.of(1L))
                        .build(),
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .listenerSuccessRecords(List.of(
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .committedOffsets(List.of(3L))
                        .build()
        ));
    }

    @Test
    public void batchConsumerShouldPollMultipleMessagesAtOnceAndConsumeInBatchesAndCommitRecordsInBatch() {
        final String topic = "test-topic-3";
        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                7L
        );

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
                        String.class,
                        consumerRecords -> {
                        },
                        ListenerConfiguration
                                .builder()
                                .maxPollRecords(3)
                                .errorHandler(consumerTrackingTools.getErrorHandler())
                                .build(),
                        consumerTrackingTools::registerInterceptors
                ).createContainer(topic);

        IntStream.rangeClosed(1, 7).forEach(i -> template.send(topic, "key" + i, "value" + i));

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(10, TimeUnit.SECONDS)).isTrue();
        assertThat(consumerTrackingTools.getContinuouslyUpdatedConsumeReportsOrderedChronologically()).isEqualTo(List.of(
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .listenerSuccessBatches(List.of(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        )))
                        .committedOffsets(List.of(3L))
                        .build(),
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key4", "value4"),
                                new RecordReport("key5", "value5"),
                                new RecordReport("key6", "value6")
                        ))
                        .listenerSuccessBatches(List.of(List.of(
                                new RecordReport("key4", "value4"),
                                new RecordReport("key5", "value5"),
                                new RecordReport("key6", "value6")
                        )))
                        .committedOffsets(List.of(6L))
                        .build(),
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key7", "value7")
                        ))
                        .listenerSuccessBatches(List.of(List.of(
                                new RecordReport("key7", "value7")
                        )))
                        .committedOffsets(List.of(7L))
                        .build()
        ));
    }

    @Test
    public void givenBatchListenerFailedExceptionDuringBatchProcessingBatchConsumerShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
        final String topic = "test-topic-4";

        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                7L
        );

        AtomicBoolean hasAlreadyFailed = new AtomicBoolean(false);

        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
                        String.class,
                        consumerRecords -> {
                            OptionalInt message5Index = IntStream.range(0, consumerRecords.size())
                                    .filter(i -> "key5".equals(consumerRecords.get(i).key()))
                                    .findFirst();
                            if (message5Index.isPresent() && !hasAlreadyFailed.get()) {
                                hasAlreadyFailed.set(true);
                                throw new BatchListenerFailedException("test message", message5Index.getAsInt());
                            }
                        },
                        ListenerConfiguration
                                .builder()
                                .maxPollRecords(3)
                                .errorHandler(consumerTrackingTools.getErrorHandler())
                                .build(),
                        consumerTrackingTools::registerInterceptors
                ).createContainer(topic);

        IntStream.rangeClosed(1, 7).forEach(i -> template.send(topic, "key" + i, "value" + i));

        listenerContainer.start();

        assertThat(consumerTrackingTools.waitForFinalCommit(10, TimeUnit.SECONDS)).isTrue();
        assertThat(consumerTrackingTools.getContinuouslyUpdatedConsumeReportsOrderedChronologically()).isEqualTo(List.of(
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .listenerSuccessBatches(List.of(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        )))
                        .committedOffsets(List.of(3L))
                        .build(),
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key4", "value4"),
                                new RecordReport("key5", "value5"),
                                new RecordReport("key6", "value6")
                        ))
                        .listenerFailureBatches(List.of(new RecordsExceptionReport(
                                List.of(
                                        new RecordReport("key4", "value4"),
                                        new RecordReport("key5", "value5"),
                                        new RecordReport("key6", "value6")
                                ),
                                new ExceptionReport(
                                        ListenerExecutionFailedException.class,
                                        "Listener failed"
                                )
                        )))
                        .errorHandlerHandleBatchCalls(List.of(
                                new RecordsExceptionReport(
                                        List.of(
                                                new RecordReport("key4", "value4"),
                                                new RecordReport("key5", "value5"),
                                                new RecordReport("key6", "value6")
                                        ),
                                        new ExceptionReport(
                                                ListenerExecutionFailedException.class,
                                                "Listener failed"
                                        )
                                )
                        ))
                        .committedOffsets(List.of(4L))
                        .build(),
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key5", "value5"),
                                new RecordReport("key6", "value6"),
                                new RecordReport("key7", "value7")
                        ))
                        .listenerSuccessBatches(List.of(List.of(
                                new RecordReport("key5", "value5"),
                                new RecordReport("key6", "value6"),
                                new RecordReport("key7", "value7")
                        )))
                        .committedOffsets(List.of(7L))
                        .build()
        ));
    }

}

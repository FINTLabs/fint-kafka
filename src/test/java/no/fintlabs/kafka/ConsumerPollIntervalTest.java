package no.fintlabs.kafka;

import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.common.ListenerContainerFactoryService;
import no.fintlabs.kafka.event.EventConsumerConfiguration;
import no.fintlabs.kafka.utils.consumertracking.ConsumerTrackingReport;
import no.fintlabs.kafka.utils.consumertracking.ConsumerTrackingService;
import no.fintlabs.kafka.utils.consumertracking.ConsumerTrackingTools;
import no.fintlabs.kafka.utils.consumertracking.reports.ExceptionReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordExceptionReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordReport;
import no.fintlabs.kafka.utils.consumertracking.reports.RecordsExceptionReport;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@EmbeddedKafka(partitions = 1)
@DirtiesContext
public class ConsumerPollIntervalTest {

    AdminClient adminClient;
    ListenerContainerFactoryService listenerContainerFactoryService;
    ConsumerTrackingService consumerTrackingService;
    KafkaTemplate<String, String> template;

    // TODO eivindmorch 02/08/2024 : !!! Dersom man trenger exactly once må man ha en shared store med offsets som er
    //  prosessert, slik atm an kan skippe prosesseringen og commite dem. Er aktuelt feks etter CommitFailedException
    //  Trenger man også reverting logikk for de operasjonene som kan revertes?


    // TODO eivindmorch 28/05/2024 : Replace integration test with mocked consumer to see behaviour?
    //  Replace DefaultKafkaConsumerFactory with MockConsumerFactoryService in FintConsumerFactory for tests

    // TODO eivindmorch 27/05/2024 : Fix unique topic generation
    // TODO eivindmorch 28/05/2024 : Create consumer that only handles one record at a time? Calls ErrorHandler.handleOne instead of handleRemaining

    @BeforeEach
    public void setup(
            @Autowired AdminClient adminClient,
            @Autowired ListenerContainerFactoryService listenerContainerFactoryService,
            @Autowired FintTemplateFactory fintTemplateFactory,
            @Autowired ConsumerTrackingService consumerTrackingService
    ) {
        this.adminClient = adminClient;
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.consumerTrackingService = consumerTrackingService;
        template = fintTemplateFactory.createTemplate(String.class);
    }

    private void sleep(long timeout, TimeUnit timeUnit) {
        try {
            timeUnit.sleep(timeout);
        } catch (InterruptedException e) {
            fail("Sleep interrupted", e);
        }
    }

    @Test
    public void recordConsumerWithRecordCommitShouldPollMultipleMessagesAtOnceAndConsumeAndCommitEachRecordIndividually() {
        final String topic = "test-topic-1";
        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );

        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                String.class,
                consumerRecord -> {
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(5000)
                        .maxPollRecords(3)
                        .ackMode(ContainerProperties.AckMode.RECORD)
                        .errorHandler(consumerTrackingTools.getErrorHandler())
                        .build(),
                consumerTrackingTools::registerInterceptors
        ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

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
                        .committedOffsets(List.of(1L, 2L, 3L))
                        .build()
        ));
    }

    @Test
    public void recordConsumerWithBatchCommitShouldPollMultipleMessagesAtOnceAndConsumeEachRecordIndividuallyAndCommitRecordsInBatch() {
        final String topic = "test-topic-1";
        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );

        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                String.class,
                consumerRecord -> {
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(5000)
                        .maxPollRecords(3)
                        .ackMode(ContainerProperties.AckMode.BATCH)
                        .errorHandler(consumerTrackingTools.getErrorHandler())
                        .build(),
                consumerTrackingTools::registerInterceptors
        ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

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


    // TODO eivindmorch 28/05/2024 : Do we ever get errorHandler handleOne calls? With poll size 1?
    @Test
    public void givenErrorDuringRecordProcessingRecordConsumerWithRecordCommitShouldInvokeHandleRemainingAndCommitEachRecordIndividually() {
        final String topic = "test-topic-2";

        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );

        AtomicBoolean alreadyFailed = new AtomicBoolean(false);

        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                String.class,
                consumerRecord -> {
                    if (consumerRecord.key().equals("key2") && !alreadyFailed.get()) {
                        alreadyFailed.set(true);
                        throw new RuntimeException();
                    }
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(5000)
                        .maxPollRecords(3)
                        .ackMode(ContainerProperties.AckMode.RECORD)
                        .errorHandler(consumerTrackingTools.getErrorHandler())
                        .build(),
                consumerTrackingTools::registerInterceptors
        ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

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
                                        new ExceptionReport(ListenerExecutionFailedException.class, "Listener failed")
                                )
                        ))
                        .errorHandlerHandleRemainingCalls(List.of(
                                new RecordsExceptionReport(
                                        List.of(
                                                new RecordReport("key2", "value2"),
                                                new RecordReport("key3", "value3")
                                        ),
                                        new ExceptionReport(ListenerExecutionFailedException.class, "Listener failed")
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
                        .committedOffsets(List.of(2L, 3L))
                        .build()
        ));
    }

    @Test
    public void givenErrorDuringRecordProcessingRecordConsumerWithBatchCommitShouldInvokeHandleRemainingAndCommitRecordsInBatch() {
        final String topic = "test-topic-3";

        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );

        AtomicBoolean hasAlreadyFailed = new AtomicBoolean(false);

        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                String.class,
                consumerRecord -> {
                    if (consumerRecord.key().equals("key2") && !hasAlreadyFailed.get()) {
                        hasAlreadyFailed.set(true);
                        throw new RuntimeException();
                    }
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(5000)
                        .ackMode(ContainerProperties.AckMode.BATCH)
                        .errorHandler(consumerTrackingTools.getErrorHandler())
                        .build(),
                consumerTrackingTools::registerInterceptors
        ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

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
                                        new ExceptionReport(ListenerExecutionFailedException.class, "Listener failed")
                                )
                        ))
                        .errorHandlerHandleRemainingCalls(List.of(
                                new RecordsExceptionReport(
                                        List.of(
                                                new RecordReport("key2", "value2"),
                                                new RecordReport("key3", "value3")
                                        ),
                                        new ExceptionReport(ListenerExecutionFailedException.class, "Listener failed")
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

    // TODO eivindmorch 24/05/2024 : Fix title
    @Test
    public void givenRecordProcessingTimeLessThanMaxPollInterval_TODO_shouldNotFailBatchCommitGivenProcessingTimeLessThanMaxPollInterval() {
        final String topic = "test-topic-4";
        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );

        listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
                String.class,
                consumerRecord -> sleep(200, TimeUnit.MILLISECONDS),
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(5000)
                        .ackMode(ContainerProperties.AckMode.BATCH)
                        .errorHandler(consumerTrackingTools.getErrorHandler())
                        .build(),
                consumerTrackingTools::registerInterceptors
        ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

        assertThat(consumerTrackingTools.waitForFinalCommit(10, TimeUnit.SECONDS)).isTrue();

        assertThat(consumerTrackingTools.getContinuouslyUpdatedConsumeReportsOrderedChronologically()).isEqualTo(List.of(
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .listenerSuccessBatches(List.of(
                                List.of(
                                        new RecordReport("key1", "value1"),
                                        new RecordReport("key2", "value2"),
                                        new RecordReport("key3", "value3")
                                )
                        ))
                        .committedOffsets(List.of(3L))
                        .build()
        ));
    }

    // TODO eivindmorch 24/05/2024 : Fix title
    // TODO eivindmorch 31/05/2024 : Hvorfor er det alltid handleRemaining når record consumer feiler?
    @Test
    public void givenRecordProcessingTimeMoreThanMaxPollInterval_TODO() {
        final String topic = "test-topic-5";
        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );
        Iterator<Long> processingTimeMillis = List.of(1500L, 1500L, 1500L).iterator();
        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                String.class,
                consumerRecord -> {
                    if (processingTimeMillis.hasNext()) {
                        sleep(processingTimeMillis.next(), TimeUnit.MILLISECONDS);
                    }
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(1000)
                        .ackMode(ContainerProperties.AckMode.RECORD)
                        .errorHandler(consumerTrackingTools.getErrorHandler())
                        .maxPollRecords(1)
                        .build(),
                consumer -> {
                    consumerTrackingTools.registerInterceptors(consumer);
                    consumer.getContainerProperties().setSyncCommits(true);
                }

        ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

        assertThat(consumerTrackingTools.waitForFinalCommit(40, TimeUnit.SECONDS)).isTrue();
        assertThat(consumerTrackingTools.getContinuouslyUpdatedConsumeReportsOrderedChronologically()).isEqualTo(List.of(
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .committedOffsets(List.of(3L))
                        .build()
        ));
    }

    @Test
    public void givenBatchProcessingTimeMoreThanMaxPollInterval_TODO() {
        final String topic = "test-topic-5";
        ConsumerTrackingTools consumerTrackingTools = consumerTrackingService.createConsumerTrackingTools(
                topic,
                3L
        );
        Iterator<Long> processingTimeMillis = List.of(1500L).iterator();
        listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
                String.class,
                consumerRecords -> {
                    if (processingTimeMillis.hasNext()) {
                        sleep(processingTimeMillis.next(), TimeUnit.MILLISECONDS);
                    }
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(1000)
                        .ackMode(ContainerProperties.AckMode.BATCH)
                        .errorHandler(consumerTrackingTools.getErrorHandler())
                        .maxPollRecords(1)
                        .build(),
                consumerTrackingTools::registerInterceptors
        ).createContainer(topic);

        template.send(topic, "key1", "value1");
        template.send(topic, "key2", "value2");
        template.send(topic, "key3", "value3");

        // assertThat(consumerTrackingTools.waitForFinalCommit(40, TimeUnit.SECONDS)).isTrue();
        consumerTrackingTools.waitForFinalCommit(60, TimeUnit.SECONDS);
        assertThat(consumerTrackingTools.getContinuouslyUpdatedConsumeReportsOrderedChronologically()).isEqualTo(List.of(
                ConsumerTrackingReport
                        .builder()
                        .consumedRecords(List.of(
                                new RecordReport("key1", "value1"),
                                new RecordReport("key2", "value2"),
                                new RecordReport("key3", "value3")
                        ))
                        .committedOffsets(List.of(3L))
                        .build()
        ));
    }


//
//    @Test
//    public void batchConsumerShouldCommitProcessedRecordsAndRetryRemainingInBatchWhenBatchListenerFailedExceptionIsThrown() throws InterruptedException {
//        final String topic = "test-topic-2";
//        TestBatchInterceptor interceptor = new TestBatchInterceptor(2, new TopicPartition(topic, 0));
//        TestErrorHandler testErrorHandler = new TestErrorHandler(1);
//        CountDownLatch commitCountDownLatch = TestConsumerInterceptor.registerCommitsCountDownLatch(topic, 2);
//        AtomicInteger a = new AtomicInteger();
//
//        listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
//                String.class,
//                consumerRecords -> {
//                    if (a.getAndIncrement() == 0) {
//                        throw new BatchListenerFailedException("message", 1);
//                    }
//                },
//                EventConsumerConfiguration
//                        .builder()
//                        .maxPollIntervalMs(5000)
//                        .ackMode(ContainerProperties.AckMode.BATCH)
//                        .errorHandler(testErrorHandler)
//                        .build(),
//                container -> {
//                    container.setBatchInterceptor(interceptor);
//                    container.getContainerProperties().getKafkaConsumerProperties().setProperty(
//                            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TestConsumerInterceptor.class.getName()
//                    );
//                }
//        ).createContainer(topic);
//
//        template.send(topic, "test-value-1");
//        template.send(topic, "test-value-2");
//        template.send(topic, "test-value-3");
//
//        assertThat(interceptor.getBatchesProcessedCountDownLatch().await(10000, TimeUnit.MILLISECONDS)).isTrue();
//        assertThat(commitCountDownLatch.await(10000, TimeUnit.MILLISECONDS)).isTrue();
//
//        assertThat(interceptor.getSuccessBatches().size()).isEqualTo(1);
//
//        assertThat(interceptor.getSuccessBatches().get(0).get(0).topic()).isEqualTo(topic);
//        assertThat(interceptor.getSuccessBatches().get(0).get(0).value()).isEqualTo("test-value-2");
//
//        assertThat(interceptor.getSuccessBatches().get(0).get(1).topic()).isEqualTo(topic);
//        assertThat(interceptor.getSuccessBatches().get(0).get(1).value()).isEqualTo("test-value-3");
//
//
//        assertThat(interceptor.getFailureBatches().size()).isEqualTo(1);
//
//        assertThat(interceptor.getFailureBatches().get(0).get(0).topic()).isEqualTo(topic);
//        assertThat(interceptor.getFailureBatches().get(0).get(0).value()).isEqualTo("test-value-1");
//
//        assertThat(interceptor.getFailureBatches().get(0).get(1).topic()).isEqualTo(topic);
//        assertThat(interceptor.getFailureBatches().get(0).get(1).value()).isEqualTo("test-value-2");
//
//        assertThat(interceptor.getFailureBatches().get(0).get(2).topic()).isEqualTo(topic);
//        assertThat(interceptor.getFailureBatches().get(0).get(2).value()).isEqualTo("test-value-3");
//
//        assertThat(testErrorHandler.getCommitFailedExceptions().size()).isZero();
//
//        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic).size()).isEqualTo(2);
//        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic)).isEqualTo(List.of(1L, 3L));
//    }
//
//    @Test
//    public void recordConsumerRecordCommit() throws InterruptedException {
//        final String topic = "test-topic-3";
//        TestListenerRecordInterceptor interceptor = new TestListenerRecordInterceptor(5);
//        TestErrorHandler testErrorHandler = new TestErrorHandler(2);
//        CountDownLatch commitCountDownLatch = TestConsumerInterceptor.registerCommitsCountDownLatch(topic, 3);
//
//        Iterator<Long> processingTime = List.of(200L, 10000L, 10000L, 200L, 200L).iterator();
//        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
//                String.class,
//                consumerRecord -> {
//                    try {
//                        Thread.sleep(processingTime.next());
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                },
//                EventConsumerConfiguration
//                        .builder()
//                        .maxPollIntervalMs(5000)
//                        .ackMode(ContainerProperties.AckMode.RECORD)
//                        .errorHandler(testErrorHandler)
//                        .build(),
//                container -> {
//                    container.setRecordInterceptor(interceptor);
//                    container.getContainerProperties().getKafkaConsumerProperties().setProperty(
//                            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TestConsumerInterceptor.class.getName()
//                    );
//                }
//        ).createContainer(topic);
//
//        template.send(topic, "test-value-1");
//        template.send(topic, "test-value-2");
//        template.send(topic, "test-value-3");
//
//        assertThat(interceptor.getRecordsProcessedCountDownLatch().await(60000, TimeUnit.MILLISECONDS)).isTrue();
//        assertThat(testErrorHandler.getCommitFailedCountDownLatch().await(60000, TimeUnit.MILLISECONDS)).isTrue();
//        assertThat(commitCountDownLatch.await(60000, TimeUnit.MILLISECONDS)).isTrue();
//
//        assertThat(interceptor.getSuccessRecords().size()).isEqualTo(3);
//        assertThat(interceptor.getFailureRecords().size()).isEqualTo(2);
//
//        assertThat(areFromTestTopicAndPartition(interceptor.getSuccessRecords(), topic)).isTrue();
//        assertThat(areFromTestTopicAndPartition(interceptor.getFailureRecords(), topic)).isTrue();
//
//        assertThat(interceptor.getSuccessRecords().get(0).offset()).isEqualTo(0);
//        assertThat(interceptor.getSuccessRecords().get(0).value()).isEqualTo("test-value-1");
//
//        assertThat(interceptor.getSuccessRecords().get(1).offset()).isEqualTo(1);
//        assertThat(interceptor.getSuccessRecords().get(1).value()).isEqualTo("test-value-2");
//
//        assertThat(interceptor.getSuccessRecords().get(2).offset()).isEqualTo(2);
//        assertThat(interceptor.getSuccessRecords().get(2).value()).isEqualTo("test-value-3");
//
//        assertThat(interceptor.getFailureRecordsWithExceptions().get(0).getT1().offset()).isEqualTo(1);
//        assertThat(interceptor.getFailureRecordsWithExceptions().get(0).getT1().value()).isEqualTo("test-value-2");
//        assertThat(interceptor.getFailureRecordsWithExceptions().get(0).getT2()).isInstanceOf(CommitFailedException.class);
//        assertThat(interceptor.getFailureRecordsWithExceptions().get(0).getT2().getMessage()).isEqualTo(
//                "Offset commit cannot be completed since the consumer is not part of an active group" +
//                        " for auto partition assignment; it is likely that the consumer was kicked out of the group."
//        );
//
//        assertThat(interceptor.getFailureRecordsWithExceptions().get(1).getT1().offset()).isEqualTo(1);
//        assertThat(interceptor.getFailureRecordsWithExceptions().get(1).getT1().value()).isEqualTo("test-value-2");
//        assertThat(interceptor.getFailureRecordsWithExceptions().get(1).getT2()).isInstanceOf(CommitFailedException.class);
//        assertThat(interceptor.getFailureRecordsWithExceptions().get(1).getT2().getMessage()).isEqualTo(
//                "Offset commit cannot be completed since the consumer is not part of an active group" +
//                        " for auto partition assignment; it is likely that the consumer was kicked out of the group."
//        );
//
//        assertThat(testErrorHandler.getCommitFailedExceptions().size()).isEqualTo(2);
//
//        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic).size()).isEqualTo(3);
//        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic)).isEqualTo(List.of(1L, 2L, 3L));
//    }
//
//    @Test
//    public void recordConsumerBatchCommit() throws InterruptedException {
//        final String topic = "test-topic-4";
//        TestListenerRecordInterceptor interceptor = new TestListenerRecordInterceptor(6);
//        TestErrorHandler testErrorHandler = new TestErrorHandler(1);
//        CountDownLatch commitCountDownLatch = TestConsumerInterceptor.registerCommitsCountDownLatch(topic, 1);
//
//        Iterator<Long> processingTime = List.of(
//                200L, 4000L, 4000L, // Batch attempt 1
//                200L, 200L, 200L    // Batch attempt 2
//        ).iterator();
//        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
//                String.class,
//                consumerRecord -> {
//                    try {
//                        Thread.sleep(processingTime.next());
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                },
//                EventConsumerConfiguration
//                        .builder()
//                        .maxPollIntervalMs(5000)
//                        .ackMode(ContainerProperties.AckMode.BATCH)
//                        .errorHandler(testErrorHandler)
//                        .build(),
//                container -> {
//                    container.setRecordInterceptor(interceptor);
//                    container.getContainerProperties().getKafkaConsumerProperties().setProperty(
//                            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TestConsumerInterceptor.class.getName()
//                    );
//                }
//        ).createContainer(topic);
//
//        template.send(topic, "test-value-1");
//        template.send(topic, "test-value-2");
//        template.send(topic, "test-value-3");
//
//        assertThat(interceptor.getRecordsProcessedCountDownLatch().await(60000, TimeUnit.MILLISECONDS)).isTrue();
//        assertThat(testErrorHandler.getCommitFailedCountDownLatch().await(60000, TimeUnit.MILLISECONDS)).isTrue();
//        assertThat(commitCountDownLatch.await(60000, TimeUnit.MILLISECONDS)).isTrue();
//
//        assertThat(interceptor.getSuccessRecords().size()).isEqualTo(6);
//        assertThat(interceptor.getFailureRecords().size()).isEqualTo(0);
//
//        assertThat(areFromTestTopicAndPartition(interceptor.getSuccessRecords(), topic)).isTrue();
//        assertThat(areFromTestTopicAndPartition(interceptor.getFailureRecords(), topic)).isTrue();
//
//        assertThat(interceptor.getSuccessRecords().get(0).offset()).isEqualTo(0);
//        assertThat(interceptor.getSuccessRecords().get(0).value()).isEqualTo("test-value-1");
//
//        assertThat(interceptor.getSuccessRecords().get(1).offset()).isEqualTo(1);
//        assertThat(interceptor.getSuccessRecords().get(1).value()).isEqualTo("test-value-2");
//
//        assertThat(interceptor.getSuccessRecords().get(2).offset()).isEqualTo(2);
//        assertThat(interceptor.getSuccessRecords().get(2).value()).isEqualTo("test-value-3");
//
//        assertThat(interceptor.getSuccessRecords().get(3).offset()).isEqualTo(0);
//        assertThat(interceptor.getSuccessRecords().get(3).value()).isEqualTo("test-value-1");
//
//        assertThat(interceptor.getSuccessRecords().get(4).offset()).isEqualTo(1);
//        assertThat(interceptor.getSuccessRecords().get(4).value()).isEqualTo("test-value-2");
//
//        assertThat(interceptor.getSuccessRecords().get(5).offset()).isEqualTo(2);
//        assertThat(interceptor.getSuccessRecords().get(5).value()).isEqualTo("test-value-3");
//
//        assertThat(testErrorHandler.getCommitFailedExceptions().size()).isOne();
//
//        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic).size()).isEqualTo(1);
//        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic)).isEqualTo(List.of(3L));
//    }
//
//    @Test
//    public void batchConsumerBatchCommit() throws InterruptedException {
//        final String topic = "test-topic-5";
//        TestBatchInterceptor interceptor = new TestBatchInterceptor(2, new TopicPartition(topic, 0));
//        TestErrorHandler testErrorHandler = new TestErrorHandler(1);
//        CountDownLatch commitCountDownLatch = TestConsumerInterceptor.registerCommitsCountDownLatch(topic, 1);
//
//        Iterator<Long> processingTime = List.of(
//                10000L, // Batch attempt 1
//                200L    // Batch attempt 2
//        ).iterator();
//        listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
//                String.class,
//                consumerRecords -> {
//                    try {
//                        try {
//                            Thread.sleep(processingTime.next());
//                        } catch (NoSuchElementException e) {
//                            Thread.sleep(200L);
//                        }
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                },
//                EventConsumerConfiguration
//                        .builder()
//                        .maxPollIntervalMs(5000)
//                        .ackMode(ContainerProperties.AckMode.BATCH)
//                        .errorHandler(testErrorHandler)
//                        .build(),
//                container -> {
//                    container.setBatchInterceptor(interceptor);
//                    container.getContainerProperties().getKafkaConsumerProperties().setProperty(
//                            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TestConsumerInterceptor.class.getName()
//                    );
//                }
//        ).createContainer(topic);
//
//        template.send(topic, "test-value-1");
//        template.send(topic, "test-value-2");
//        template.send(topic, "test-value-3");
//
//        interceptor.getBatchesProcessedCountDownLatch().await(60000, TimeUnit.MILLISECONDS);
//        testErrorHandler.getCommitFailedCountDownLatch().await(60000, TimeUnit.MILLISECONDS);
//        commitCountDownLatch.await(60000, TimeUnit.MILLISECONDS);
//
//        assertThat(interceptor.getSuccessBatches().size()).isEqualTo(1);
//        assertThat(interceptor.getFailureBatches().size()).isEqualTo(0);
//
//        assertThat(areFromTestTopicAndPartition(
//                interceptor.getSuccessBatches().stream().flatMap(List::stream).toList(),
//                topic
//        )).isTrue();
//        assertThat(areFromTestTopicAndPartition(
//                interceptor.getFailureBatches().stream().flatMap(List::stream).toList(),
//                topic
//        )).isTrue();
//
//        assertThat(interceptor.getSuccessBatches().get(0).get(0).offset()).isEqualTo(0);
//        assertThat(interceptor.getSuccessBatches().get(0).get(0).value()).isEqualTo("test-value-1");
//
//        assertThat(interceptor.getSuccessBatches().get(0).get(1).offset()).isEqualTo(1);
//        assertThat(interceptor.getSuccessBatches().get(0).get(1).value()).isEqualTo("test-value-2");
//
//        assertThat(interceptor.getSuccessBatches().get(0).get(2).offset()).isEqualTo(2);
//        assertThat(interceptor.getSuccessBatches().get(0).get(2).value()).isEqualTo("test-value-3");
//
//        assertThat(interceptor.getSuccessBatches().get(1).get(0).offset()).isEqualTo(0);
//        assertThat(interceptor.getSuccessBatches().get(1).get(0).value()).isEqualTo("test-value-1");
//
//        assertThat(interceptor.getSuccessBatches().get(1).get(1).offset()).isEqualTo(1);
//        assertThat(interceptor.getSuccessBatches().get(1).get(1).value()).isEqualTo("test-value-2");
//
//        assertThat(interceptor.getSuccessBatches().get(1).get(2).offset()).isEqualTo(2);
//        assertThat(interceptor.getSuccessBatches().get(1).get(2).value()).isEqualTo("test-value-3");
//
//        assertThat(testErrorHandler.getCommitFailedExceptions().size()).isOne();
//
//        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic).size()).isEqualTo(1);
//        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic)).isEqualTo(List.of(3L));
//    }

}

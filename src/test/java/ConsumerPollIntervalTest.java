import no.fintlabs.kafka.TestApplication;
import no.fintlabs.kafka.common.FintTemplateFactory;
import no.fintlabs.kafka.common.ListenerContainerFactoryService;
import no.fintlabs.kafka.event.EventConsumerConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import utils.TestBatchInterceptor;
import utils.TestConsumerInterceptor;
import utils.TestErrorHandler;
import utils.TestRecordInterceptor;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest(classes = {TestApplication.class})
@EmbeddedKafka(topics = {"test-topic-1", "test-topic-2", "test-topic-3", "test-topic-4"}, partitions = 1)
@DirtiesContext
public class ConsumerPollIntervalTest {

    AdminClient adminClient;
    ListenerContainerFactoryService listenerContainerFactoryService;
    KafkaTemplate<String, String> template;

    @BeforeEach
    public void setup(
            @Autowired AdminClient adminClient,
            @Autowired ListenerContainerFactoryService listenerContainerFactoryService,
            @Autowired FintTemplateFactory fintTemplateFactory
    ) {
        this.adminClient = adminClient;
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        template = fintTemplateFactory.createTemplate(String.class);
    }

    private boolean areFromTestTopicAndPartition(List<ConsumerRecord<String, String>> records, String topic) {
        return records
                .stream()
                .allMatch(record ->
                        Objects.equals(record.topic(), topic)
                                && record.partition() == 0
                );
    }

    @Test
    public void shouldNotFailCommitGivenProcessingTimeLessThanMaxPollInterval() throws InterruptedException {
        final String topic = "test-topic-1";
        TestRecordInterceptor interceptor = new TestRecordInterceptor(3);
        TestErrorHandler testErrorHandler = new TestErrorHandler(0);
        CountDownLatch commitCountDownLatch = TestConsumerInterceptor.registerCommitsCountDownLatch(topic, 3);

        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                String.class,
                consumerRecord -> {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(5000)
                        .ackMode(ContainerProperties.AckMode.RECORD)
                        .errorHandler(testErrorHandler)
                        .build(),
                container -> {
                    container.setRecordInterceptor(interceptor);
                    container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TestConsumerInterceptor.class.getName()
                    );
                }
        ).createContainer(topic);

        template.send(topic, "test-value");
        template.send(topic, "test-value");
        template.send(topic, "test-value");

        assertThat(interceptor.getRecordsProcessedCountDownLatch().await(10000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(commitCountDownLatch.await(10000, TimeUnit.MILLISECONDS)).isTrue();

        assertThat(interceptor.getSuccessRecords().size()).isEqualTo(3);
        assertThat(interceptor.getSuccessRecords().get(0).topic()).isEqualTo(topic);
        assertThat(interceptor.getSuccessRecords().get(0).value()).isEqualTo("test-value");
        assertThat(interceptor.getFailureRecords().size()).isZero();

        assertThat(testErrorHandler.getCommitFailedExceptions().size()).isZero();

        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic).size()).isEqualTo(3);
        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic)).isEqualTo(List.of(1L, 2L, 3L));
    }

    @Test
    public void recordConsumerRecordCommit() throws InterruptedException {
        final String topic = "test-topic-2";
        TestRecordInterceptor interceptor = new TestRecordInterceptor(5);
        TestErrorHandler testErrorHandler = new TestErrorHandler(2);
        CountDownLatch commitCountDownLatch = TestConsumerInterceptor.registerCommitsCountDownLatch(topic, 3);

        Iterator<Long> processingTime = List.of(200L, 10000L, 10000L, 200L, 200L).iterator();
        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                String.class,
                consumerRecord -> {
                    try {
                        Thread.sleep(processingTime.next());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(5000)
                        .ackMode(ContainerProperties.AckMode.RECORD)
                        .errorHandler(testErrorHandler)
                        .build(),
                container -> {
                    container.setRecordInterceptor(interceptor);
                    container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TestConsumerInterceptor.class.getName()
                    );
                }
        ).createContainer(topic);

        template.send(topic, "test-value-1");
        template.send(topic, "test-value-2");
        template.send(topic, "test-value-3");

        assertThat(interceptor.getRecordsProcessedCountDownLatch().await(60000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(testErrorHandler.getCommitFailedCountDownLatch().await(60000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(commitCountDownLatch.await(60000, TimeUnit.MILLISECONDS)).isTrue();

        assertThat(interceptor.getSuccessRecords().size()).isEqualTo(3);
        assertThat(interceptor.getFailureRecords().size()).isEqualTo(2);

        assertThat(areFromTestTopicAndPartition(interceptor.getSuccessRecords(), topic)).isTrue();
        assertThat(areFromTestTopicAndPartition(interceptor.getFailureRecords(), topic)).isTrue();

        assertThat(interceptor.getSuccessRecords().get(0).offset()).isEqualTo(0);
        assertThat(interceptor.getSuccessRecords().get(0).value()).isEqualTo("test-value-1");

        assertThat(interceptor.getSuccessRecords().get(1).offset()).isEqualTo(1);
        assertThat(interceptor.getSuccessRecords().get(1).value()).isEqualTo("test-value-2");

        assertThat(interceptor.getSuccessRecords().get(2).offset()).isEqualTo(2);
        assertThat(interceptor.getSuccessRecords().get(2).value()).isEqualTo("test-value-3");

        assertThat(interceptor.getFailureRecordsWithExceptions().get(0).getT1().offset()).isEqualTo(1);
        assertThat(interceptor.getFailureRecordsWithExceptions().get(0).getT1().value()).isEqualTo("test-value-2");
        assertThat(interceptor.getFailureRecordsWithExceptions().get(0).getT2()).isInstanceOf(CommitFailedException.class);
        assertThat(interceptor.getFailureRecordsWithExceptions().get(0).getT2().getMessage()).isEqualTo(
                "Offset commit cannot be completed since the consumer is not part of an active group" +
                        " for auto partition assignment; it is likely that the consumer was kicked out of the group."
        );

        assertThat(interceptor.getFailureRecordsWithExceptions().get(1).getT1().offset()).isEqualTo(1);
        assertThat(interceptor.getFailureRecordsWithExceptions().get(1).getT1().value()).isEqualTo("test-value-2");
        assertThat(interceptor.getFailureRecordsWithExceptions().get(1).getT2()).isInstanceOf(CommitFailedException.class);
        assertThat(interceptor.getFailureRecordsWithExceptions().get(1).getT2().getMessage()).isEqualTo(
                "Offset commit cannot be completed since the consumer is not part of an active group" +
                        " for auto partition assignment; it is likely that the consumer was kicked out of the group."
        );

        assertThat(testErrorHandler.getCommitFailedExceptions().size()).isEqualTo(2);

        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic).size()).isEqualTo(3);
        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic)).isEqualTo(List.of(1L, 2L, 3L));
    }

    @Test
    public void recordConsumerBatchCommit() throws InterruptedException {
        final String topic = "test-topic-3";
        TestRecordInterceptor interceptor = new TestRecordInterceptor(6);
        TestErrorHandler testErrorHandler = new TestErrorHandler(1);
        CountDownLatch commitCountDownLatch = TestConsumerInterceptor.registerCommitsCountDownLatch(topic, 1);

        Iterator<Long> processingTime = List.of(
                200L, 4000L, 4000L, // Batch attempt 1
                200L, 200L, 200L    // Batch attempt 2
        ).iterator();
        listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                String.class,
                consumerRecord -> {
                    try {
                        Thread.sleep(processingTime.next());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(5000)
                        .ackMode(ContainerProperties.AckMode.BATCH)
                        .errorHandler(testErrorHandler)
                        .build(),
                container -> {
                    container.setRecordInterceptor(interceptor);
                    container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TestConsumerInterceptor.class.getName()
                    );
                }
        ).createContainer(topic);

        template.send(topic, "test-value-1");
        template.send(topic, "test-value-2");
        template.send(topic, "test-value-3");

        assertThat(interceptor.getRecordsProcessedCountDownLatch().await(60000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(testErrorHandler.getCommitFailedCountDownLatch().await(60000, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(commitCountDownLatch.await(60000, TimeUnit.MILLISECONDS)).isTrue();

        assertThat(interceptor.getSuccessRecords().size()).isEqualTo(6);
        assertThat(interceptor.getFailureRecords().size()).isEqualTo(0);

        assertThat(areFromTestTopicAndPartition(interceptor.getSuccessRecords(), topic)).isTrue();
        assertThat(areFromTestTopicAndPartition(interceptor.getFailureRecords(), topic)).isTrue();

        assertThat(interceptor.getSuccessRecords().get(0).offset()).isEqualTo(0);
        assertThat(interceptor.getSuccessRecords().get(0).value()).isEqualTo("test-value-1");

        assertThat(interceptor.getSuccessRecords().get(1).offset()).isEqualTo(1);
        assertThat(interceptor.getSuccessRecords().get(1).value()).isEqualTo("test-value-2");

        assertThat(interceptor.getSuccessRecords().get(2).offset()).isEqualTo(2);
        assertThat(interceptor.getSuccessRecords().get(2).value()).isEqualTo("test-value-3");

        assertThat(interceptor.getSuccessRecords().get(3).offset()).isEqualTo(0);
        assertThat(interceptor.getSuccessRecords().get(3).value()).isEqualTo("test-value-1");

        assertThat(interceptor.getSuccessRecords().get(4).offset()).isEqualTo(1);
        assertThat(interceptor.getSuccessRecords().get(4).value()).isEqualTo("test-value-2");

        assertThat(interceptor.getSuccessRecords().get(5).offset()).isEqualTo(2);
        assertThat(interceptor.getSuccessRecords().get(5).value()).isEqualTo("test-value-3");

        assertThat(testErrorHandler.getCommitFailedExceptions().size()).isOne();

        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic).size()).isEqualTo(1);
        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic)).isEqualTo(List.of(3L));
    }

    @Test
    public void batchConsumerBatchCommit() throws InterruptedException {
        final String topic = "test-topic-4";
        TestBatchInterceptor interceptor = new TestBatchInterceptor(2, new TopicPartition(topic, 0));
        TestErrorHandler testErrorHandler = new TestErrorHandler(1);
        CountDownLatch commitCountDownLatch = TestConsumerInterceptor.registerCommitsCountDownLatch(topic, 1);

        Iterator<Long> processingTime = List.of(
                10000L, // Batch attempt 1
                200L    // Batch attempt 2
        ).iterator();
        listenerContainerFactoryService.createBatchKafkaListenerContainerFactory(
                String.class,
                consumerRecords -> {
                    try {
                        Thread.sleep(processingTime.next());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                },
                EventConsumerConfiguration
                        .builder()
                        .maxPollIntervalMs(5000)
                        .ackMode(ContainerProperties.AckMode.BATCH)
                        .errorHandler(testErrorHandler)
                        .build(),
                container -> {
                    container.setBatchInterceptor(interceptor);
                    container.getContainerProperties().getKafkaConsumerProperties().setProperty(
                            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TestConsumerInterceptor.class.getName()
                    );
                }
        ).createContainer(topic);

        template.send(topic, "test-value-1");
        template.send(topic, "test-value-2");
        template.send(topic, "test-value-3");

        interceptor.getBatchesProcessedCountDownLatch().await(60000, TimeUnit.MILLISECONDS);
        testErrorHandler.getCommitFailedCountDownLatch().await(60000, TimeUnit.MILLISECONDS);
        commitCountDownLatch.await(60000, TimeUnit.MILLISECONDS);

        assertThat(interceptor.getSuccessBatches().size()).isEqualTo(2);
        assertThat(interceptor.getFailureBatches().size()).isEqualTo(0);

        assertThat(areFromTestTopicAndPartition(
                interceptor.getSuccessBatches().stream().flatMap(List::stream).toList(),
                topic
        )).isTrue();
        assertThat(areFromTestTopicAndPartition(
                interceptor.getFailureBatches().stream().flatMap(List::stream).toList(),
                topic
        )).isTrue();

        assertThat(interceptor.getSuccessBatches().get(0).get(0).offset()).isEqualTo(0);
        assertThat(interceptor.getSuccessBatches().get(0).get(0).value()).isEqualTo("test-value-1");

        assertThat(interceptor.getSuccessBatches().get(0).get(1).offset()).isEqualTo(1);
        assertThat(interceptor.getSuccessBatches().get(0).get(1).value()).isEqualTo("test-value-2");

        assertThat(interceptor.getSuccessBatches().get(0).get(2).offset()).isEqualTo(2);
        assertThat(interceptor.getSuccessBatches().get(0).get(2).value()).isEqualTo("test-value-3");

        assertThat(interceptor.getSuccessBatches().get(1).get(0).offset()).isEqualTo(0);
        assertThat(interceptor.getSuccessBatches().get(1).get(0).value()).isEqualTo("test-value-1");

        assertThat(interceptor.getSuccessBatches().get(1).get(1).offset()).isEqualTo(1);
        assertThat(interceptor.getSuccessBatches().get(1).get(1).value()).isEqualTo("test-value-2");

        assertThat(interceptor.getSuccessBatches().get(1).get(2).offset()).isEqualTo(2);
        assertThat(interceptor.getSuccessBatches().get(1).get(2).value()).isEqualTo("test-value-3");

        assertThat(testErrorHandler.getCommitFailedExceptions().size()).isOne();

        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic).size()).isEqualTo(1);
        assertThat(TestConsumerInterceptor.getCommitOffsetsForTopic(topic)).isEqualTo(List.of(3L));
    }

}

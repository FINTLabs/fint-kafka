package no.novari.kafka.requestreply;


import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import no.novari.kafka.consuming.ErrorHandlerFactory;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.requestreply.topic.name.ReplyTopicNameParameters;
import no.novari.kafka.requestreply.topic.name.RequestTopicNameParameters;
import no.novari.kafka.topic.name.TopicNamePrefixParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.KafkaReplyTimeoutException;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RequestReplyIntegrationTest {

    RequestTemplateFactory requestTemplateFactory;

    RequestListenerContainerFactory requestListenerContainerFactory;
    ErrorHandlerFactory errorHandlerFactory;

    public RequestReplyIntegrationTest(
            @Autowired RequestTemplateFactory requestTemplateFactory,
            @Autowired RequestListenerContainerFactory requestListenerContainerFactory,
            @Autowired ErrorHandlerFactory errorHandlerFactory
    ) {
        this.requestTemplateFactory = requestTemplateFactory;
        this.requestListenerContainerFactory = requestListenerContainerFactory;
        this.errorHandlerFactory = errorHandlerFactory;
    }

    @Setter
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    private static class TestObject {
        private Integer integer;
        private String string;
    }

    @Test
    void requestReply() {
        RequestTopicNameParameters requestTopicNameParameters = RequestTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .resourceName("test-resource-name")
                .parameterName("test-parameter-name")
                .build();

        ReplyTopicNameParameters replyTopicNameParameters = ReplyTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .applicationId("test-application-id")
                .resourceName("test-resource-name")
                .build();

        RequestTemplate<Integer, TestObject> requestTemplate = requestTemplateFactory.createTemplate(
                replyTopicNameParameters,
                Integer.class,
                TestObject.class,
                Duration.ofSeconds(30),
                ListenerConfiguration
                        .stepBuilder()
                        .groupIdApplicationDefault()
                        .maxPollRecordsKafkaDefault()
                        .maxPollIntervalKafkaDefault()
                        .continueFromPreviousOffsetOnAssignment()
                        .build()
        );

        List<ConsumerRecord<String, Integer>> consumedRequests = new ArrayList<>();

        ConcurrentMessageListenerContainer<String, Integer> requestListenerContainer =
                requestListenerContainerFactory.createRecordConsumerFactory(
                        Integer.class,
                        TestObject.class,
                        consumerRecord -> {
                            consumedRequests.add(consumerRecord);
                            return new ReplyProducerRecord<>(new TestObject(2, "testObjectString"));
                        },
                        RequestListenerConfiguration
                                .stepBuilder(Integer.class)
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .build(),
                        errorHandlerFactory.createErrorHandler(
                                ErrorHandlerConfiguration
                                        .<Integer>stepBuilder()
                                        .noRetries()
                                        .skipFailedRecords()
                                        .build()
                        )
                ).createContainer(requestTopicNameParameters);

        requestListenerContainer.start();

        ConsumerRecord<String, TestObject> reply = requestTemplate.requestAndReceive(
                new RequestProducerRecord<>(
                        requestTopicNameParameters,
                        "testKey",
                        4
                )
        );

        assertThat(consumedRequests).hasSize(1);
        ConsumerRecord<String, Integer> consumedRequest = consumedRequests.getFirst();
        assertThat(consumedRequest.topic()).isEqualTo(
                "test-org-id.test-domain-context.request.test-resource-name.by.test-parameter-name"
        );
        assertThat(consumedRequest.key()).isEqualTo("testKey");
        assertThat(consumedRequest.value()).isEqualTo(4);

        assertThat(reply.topic()).isEqualTo(
                "test-org-id.test-domain-context.reply.test-application-id.test-resource-name"
        );
        assertThat(reply.key()).isEqualTo("testKey");
        assertThat(reply.value()).isEqualTo(new TestObject(2, "testObjectString"));
    }

    @Test
    void requestReplyAsync() throws InterruptedException {
        RequestTopicNameParameters requestTopicNameParameters = RequestTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .resourceName("test-resource-name")
                .parameterName("test-parameter-name")
                .build();

        ReplyTopicNameParameters replyTopicNameParameters = ReplyTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .applicationId("test-application-id")
                .resourceName("test-resource-name")
                .build();

        RequestTemplate<Integer, TestObject> requestTemplate = requestTemplateFactory.createTemplate(
                replyTopicNameParameters,
                Integer.class,
                TestObject.class,
                Duration.ofSeconds(30),
                ListenerConfiguration
                        .stepBuilder()
                        .groupIdApplicationDefault()
                        .maxPollRecordsKafkaDefault()
                        .maxPollIntervalKafkaDefault()
                        .continueFromPreviousOffsetOnAssignment()
                        .build()
        );

        List<ConsumerRecord<String, Integer>> consumedRequests = new ArrayList<>();

        ConcurrentMessageListenerContainer<String, Integer> requestListenerContainer =
                requestListenerContainerFactory.createRecordConsumerFactory(
                        Integer.class,
                        TestObject.class,
                        consumerRecord -> {
                            consumedRequests.add(consumerRecord);
                            return new ReplyProducerRecord<>(new TestObject(2, "testObjectString"));
                        },
                        RequestListenerConfiguration
                                .stepBuilder(Integer.class)
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .build(),
                        errorHandlerFactory.createErrorHandler(
                                ErrorHandlerConfiguration
                                        .<Integer>stepBuilder()
                                        .noRetries()
                                        .skipFailedRecords()
                                        .build()
                        )
                ).createContainer(requestTopicNameParameters);

        requestListenerContainer.start();

        CountDownLatch asyncConsumeLatch = new CountDownLatch(1);
        AtomicReference<ConsumerRecord<String, TestObject>> atomicReply = new AtomicReference<>(null);
        requestTemplate.requestWithAsyncReplyConsumer(
                new RequestProducerRecord<>(
                        requestTopicNameParameters,
                        "testKey",
                        4
                ),
                record -> {
                    atomicReply.set(record);
                    asyncConsumeLatch.countDown();
                },
                e -> {}
        );

        assertThat(asyncConsumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(consumedRequests).hasSize(1);
        ConsumerRecord<String, Integer> consumedRequest = consumedRequests.getFirst();
        assertThat(consumedRequest.topic()).isEqualTo(
                "test-org-id.test-domain-context.request.test-resource-name.by.test-parameter-name"
        );
        assertThat(consumedRequest.key()).isEqualTo("testKey");
        assertThat(consumedRequest.value()).isEqualTo(4);

        ConsumerRecord<String, TestObject> reply = atomicReply.get();
        assertThat(reply).isNotNull();
        assertThat(reply.topic()).isEqualTo(
                "test-org-id.test-domain-context.reply.test-application-id.test-resource-name"
        );
        assertThat(reply.key()).isEqualTo("testKey");
        assertThat(reply.value()).isEqualTo(new TestObject(2, "testObjectString"));
    }

    @Test
    void givenReplyThatTakesLongerThanRequestTimeoutWhenRequestIsReceivedShouldThrowKafkaReplyTimeoutException() throws InterruptedException {
        RequestTopicNameParameters requestTopicNameParameters = RequestTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .resourceName("test-resource-name")
                .parameterName("test-parameter-name")
                .build();

        ReplyTopicNameParameters replyTopicNameParameters = ReplyTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .applicationId("test-application-id")
                .resourceName("test-resource-name")
                .build();

        RequestTemplate<Integer, TestObject> requestTemplate = requestTemplateFactory.createTemplate(
                replyTopicNameParameters,
                Integer.class,
                TestObject.class,
                Duration.ofMillis(100),
                ListenerConfiguration
                        .stepBuilder()
                        .groupIdApplicationDefault()
                        .maxPollRecordsKafkaDefault()
                        .maxPollIntervalKafkaDefault()
                        .continueFromPreviousOffsetOnAssignment()
                        .build()
        );

        List<ConsumerRecord<String, Integer>> consumedRequests = new ArrayList<>();
        CountDownLatch replyLatch = new CountDownLatch(1);

        ConcurrentMessageListenerContainer<String, Integer> requestListenerContainer =
                requestListenerContainerFactory.createRecordConsumerFactory(
                        Integer.class,
                        TestObject.class,
                        consumerRecord -> {
                            consumedRequests.add(consumerRecord);
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            replyLatch.countDown();
                            return new ReplyProducerRecord<>(new TestObject(2, "testObjectString"));
                        },
                        RequestListenerConfiguration
                                .stepBuilder(Integer.class)
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .build(),
                        errorHandlerFactory.createErrorHandler(
                                ErrorHandlerConfiguration
                                        .<Integer>stepBuilder()
                                        .noRetries()
                                        .skipFailedRecords()
                                        .build()
                        )
                ).createContainer(requestTopicNameParameters);

        assertThatThrownBy(() -> requestTemplate.requestAndReceive(
                new RequestProducerRecord<>(
                        requestTopicNameParameters,
                        "testKey",
                        4
                )
        )).hasCauseInstanceOf(KafkaReplyTimeoutException.class);

        requestListenerContainer.start();

        assertThat(replyLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(consumedRequests).hasSize(1);
        ConsumerRecord<String, Integer> consumedRequest = consumedRequests.getFirst();
        assertThat(consumedRequest.topic()).isEqualTo(
                "test-org-id.test-domain-context.request.test-resource-name.by.test-parameter-name"
        );
        assertThat(consumedRequest.key()).isEqualTo("testKey");
        assertThat(consumedRequest.value()).isEqualTo(4);
    }

    @Test
    void givenReplyThatTakesLongerThanRequestTimeoutWhenRequestWithAsyncReplyConsumerShouldInvokeFailureConsumerWithKafkaReplyTimeoutException() throws InterruptedException {
        RequestTopicNameParameters requestTopicNameParameters = RequestTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .resourceName("test-resource-name")
                .parameterName("test-parameter-name")
                .build();

        ReplyTopicNameParameters replyTopicNameParameters = ReplyTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("test-org-id")
                                .domainContext("test-domain-context")
                                .build()
                )
                .applicationId("test-application-id")
                .resourceName("test-resource-name")
                .build();

        RequestTemplate<Integer, TestObject> requestTemplate = requestTemplateFactory.createTemplate(
                replyTopicNameParameters,
                Integer.class,
                TestObject.class,
                Duration.ofMillis(100),
                ListenerConfiguration
                        .stepBuilder()
                        .groupIdApplicationDefault()
                        .maxPollRecordsKafkaDefault()
                        .maxPollIntervalKafkaDefault()
                        .continueFromPreviousOffsetOnAssignment()
                        .build()
        );

        List<ConsumerRecord<String, Integer>> consumedRequests = new ArrayList<>();
        CountDownLatch replyLatch = new CountDownLatch(1);

        ConcurrentMessageListenerContainer<String, Integer> requestListenerContainer =
                requestListenerContainerFactory.createRecordConsumerFactory(
                        Integer.class,
                        TestObject.class,
                        consumerRecord -> {
                            consumedRequests.add(consumerRecord);
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            replyLatch.countDown();
                            return new ReplyProducerRecord<>(new TestObject(2, "testObjectString"));
                        },
                        RequestListenerConfiguration
                                .stepBuilder(Integer.class)
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .build(),
                        errorHandlerFactory.createErrorHandler(
                                ErrorHandlerConfiguration
                                        .<Integer>stepBuilder()
                                        .noRetries()
                                        .skipFailedRecords()
                                        .build()
                        )
                ).createContainer(requestTopicNameParameters);

        CountDownLatch asyncFailureHandleLatch = new CountDownLatch(1);
        AtomicReference<Throwable> failureCause = new AtomicReference<>();
        requestTemplate.requestWithAsyncReplyConsumer(
                new RequestProducerRecord<>(
                        requestTopicNameParameters,
                        "testKey",
                        4
                ),
                record -> {},
                e -> {
                    asyncFailureHandleLatch.countDown();
                    failureCause.set(e);
                }
        );

        requestListenerContainer.start();

        assertThat(replyLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(asyncFailureHandleLatch.await(10, TimeUnit.SECONDS)).isTrue();


        assertThat(consumedRequests).hasSize(1);
        ConsumerRecord<String, Integer> consumedRequest = consumedRequests.getFirst();
        assertThat(consumedRequest.topic()).isEqualTo(
                "test-org-id.test-domain-context.request.test-resource-name.by.test-parameter-name"
        );
        assertThat(consumedRequest.key()).isEqualTo("testKey");
        assertThat(consumedRequest.value()).isEqualTo(4);

        assertThat(failureCause.get()).isNotNull();
        assertThat(failureCause.get()).isInstanceOf(KafkaReplyTimeoutException.class);
    }

}

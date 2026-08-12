package no.novari.kafka.tracing;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService;
import no.novari.kafka.producing.ParameterizedProducerRecord;
import no.novari.kafka.producing.ParameterizedTemplate;
import no.novari.kafka.producing.ParameterizedTemplateFactory;
import no.novari.kafka.requestreply.ReplyProducerRecord;
import no.novari.kafka.requestreply.RequestListenerConfiguration;
import no.novari.kafka.requestreply.RequestListenerContainerFactory;
import no.novari.kafka.requestreply.RequestProducerRecord;
import no.novari.kafka.requestreply.RequestTemplate;
import no.novari.kafka.requestreply.RequestTemplateFactory;
import no.novari.kafka.requestreply.topic.name.ReplyTopicNameParameters;
import no.novari.kafka.requestreply.topic.name.RequestTopicNameParameters;
import no.novari.kafka.topic.name.EventTopicNameParameters;
import no.novari.kafka.topic.name.TopicNamePrefixParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Beviser at et {@code ObservationRegistry}-bean alene ikke er nok til å slå på Kafka-sporing -
 * {@code fint.kafka.tracing.enabled=true} må også settes eksplisitt. Uten denne testen ville
 * en eksisterende bruker av biblioteket (som allerede har actuator + micrometer-tracing på
 * classpath) fått endret oppførsel ved en versjonsoppgradering.
 */
@SpringBootTest(properties = {
    "management.tracing.sampling.probability=1.0",
    "spring.kafka.consumer.auto-offset-reset=earliest"
})
@EmbeddedKafka(partitions = 1, kraft = true)
@AutoConfigureObservability
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TracingDisabledIntegrationTest {

    private static final String TRACEPARENT = "traceparent";

    private final ParameterizedTemplateFactory parameterizedTemplateFactory;
    private final ParameterizedListenerContainerFactoryService listenerContainerFactoryService;
    private final RequestTemplateFactory requestTemplateFactory;
    private final RequestListenerContainerFactory requestListenerContainerFactory;
    private final InMemorySpanExporter spanExporter;

    TracingDisabledIntegrationTest(
        @Autowired ParameterizedTemplateFactory parameterizedTemplateFactory,
        @Autowired ParameterizedListenerContainerFactoryService listenerContainerFactoryService,
        @Autowired RequestTemplateFactory requestTemplateFactory,
        @Autowired RequestListenerContainerFactory requestListenerContainerFactory,
        @Autowired InMemorySpanExporter spanExporter
    ) {
        this.parameterizedTemplateFactory = parameterizedTemplateFactory;
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.requestTemplateFactory = requestTemplateFactory;
        this.requestListenerContainerFactory = requestListenerContainerFactory;
        this.spanExporter = spanExporter;
    }

    @TestConfiguration
    static class SpanCaptureConfiguration {

        @Bean
        InMemorySpanExporter inMemorySpanExporter() {
            return InMemorySpanExporter.create();
        }

        @Bean
        SpanProcessor inMemorySpanProcessor(InMemorySpanExporter exporter) {
            return SimpleSpanProcessor.create(exporter);
        }
    }

    @Setter
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    static class TestObject {
        private Integer integer;
        private String string;
    }

    @BeforeEach
    void resetSpans() {
        spanExporter.reset();
    }

    private EventTopicNameParameters topic(String eventName) {
        return EventTopicNameParameters
            .builder()
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgId("test-org-id")
                    .domainContext("test-domain-context")
                    .build()
            )
            .eventName(eventName)
            .build();
    }

    private ConsumerRecord<String, TestObject> produceAndConsume(String eventName) throws InterruptedException {
        EventTopicNameParameters topicNameParameters = topic(eventName);
        CountDownLatch latch = new CountDownLatch(1);
        List<ConsumerRecord<String, TestObject>> consumed = new ArrayList<>();

        ConcurrentMessageListenerContainer<String, TestObject> container =
            listenerContainerFactoryService
                .createRecordListenerContainerFactory(
                    TestObject.class,
                    consumerRecord -> {
                        consumed.add(consumerRecord);
                        latch.countDown();
                    },
                    ListenerConfiguration
                        .stepBuilder()
                        .groupIdApplicationDefault()
                        .maxPollRecordsKafkaDefault()
                        .maxPollIntervalKafkaDefault()
                        .continueFromPreviousOffsetOnAssignment()
                        .build(),
                    null
                )
                .createContainer(topicNameParameters);
        container.start();

        ParameterizedTemplate<TestObject> template =
            parameterizedTemplateFactory.createTemplate(TestObject.class);
        template.send(
            ParameterizedProducerRecord
                .<TestObject>builder()
                .topicNameParameters(topicNameParameters)
                .key("test-key")
                .value(new TestObject(1, "disabled"))
                .build()
        );

        assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
        container.stop();
        return consumed.getFirst();
    }

    @Test
    void producerDoesNotAddTraceparentHeaderByDefault() throws InterruptedException {
        ConsumerRecord<String, TestObject> record = produceAndConsume("no-traceparent-header");

        assertThat(record.headers().lastHeader(TRACEPARENT))
            .as("uten fint.kafka.tracing.enabled skal produsenten ikke legge på traceparent")
            .isNull();
    }

    @Test
    void noKafkaSpansAreProducedByDefaultEvenWithObservationRegistryBeanPresent() throws InterruptedException {
        produceAndConsume("no-kafka-spans");
        Thread.sleep(500);

        boolean hasKafkaSpan = spanExporter.getFinishedSpanItems().stream()
            .anyMatch(span -> span.getKind().name().equals("PRODUCER") || span.getKind().name().equals("CONSUMER"));

        assertThat(hasKafkaSpan)
            .as("tilstedeværelsen av et ObservationRegistry-bean alene skal ikke være nok til å slå på sporing")
            .isFalse();
    }

    @Test
    void requestReplyStillWorksFunctionallyWhenTracingIsDisabled() {
        var prefix = TopicNamePrefixParameters
            .stepBuilder()
            .orgId("test-org-id")
            .domainContext("test-domain-context")
            .build();

        RequestTopicNameParameters requestTopic = RequestTopicNameParameters
            .builder()
            .topicNamePrefixParameters(prefix)
            .resourceName("disabled-resource")
            .parameterName("disabled-parameter")
            .build();

        ReplyTopicNameParameters replyTopic = ReplyTopicNameParameters
            .builder()
            .topicNamePrefixParameters(prefix)
            .applicationId("test-application-id")
            .resourceName("disabled-resource")
            .build();

        var listenerConfiguration = ListenerConfiguration
            .stepBuilder()
            .groupIdApplicationDefault()
            .maxPollRecordsKafkaDefault()
            .maxPollIntervalKafkaDefault()
            .continueFromPreviousOffsetOnAssignment()
            .build();

        RequestTemplate<Integer, TestObject> requestTemplate = requestTemplateFactory.createTemplate(
            replyTopic,
            Integer.class,
            TestObject.class,
            Duration.ofSeconds(30),
            listenerConfiguration
        );

        List<String> traceparentSeenByServer = new ArrayList<>();
        var requestListenerContainer = requestListenerContainerFactory
            .createRecordConsumerFactory(
                Integer.class,
                TestObject.class,
                consumerRecord -> {
                    var header = consumerRecord.headers().lastHeader(TRACEPARENT);
                    traceparentSeenByServer.add(
                        header == null ? null : new String(header.value(), StandardCharsets.UTF_8)
                    );
                    return new ReplyProducerRecord<>(new TestObject(2, "reply"));
                },
                RequestListenerConfiguration
                    .stepBuilder(Integer.class)
                    .maxPollRecordsKafkaDefault()
                    .maxPollIntervalKafkaDefault()
                    .build(),
                null
            )
            .createContainer(requestTopic);
        requestListenerContainer.start();

        var reply = requestTemplate.requestAndReceive(
            RequestProducerRecord
                .<Integer>builder()
                .topicNameParameters(requestTopic)
                .value(1)
                .build()
        );

        requestListenerContainer.stop();

        assertThat(reply)
            .as("request/reply skal fungere funksjonelt uavhengig av om sporing er aktivert")
            .isNotNull();
        assertThat(traceparentSeenByServer)
            .as("uten fint.kafka.tracing.enabled skal ikke traceparent følge med forespørselen")
            .containsOnly((String) null);
    }

}

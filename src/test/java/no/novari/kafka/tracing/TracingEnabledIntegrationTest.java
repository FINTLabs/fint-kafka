package no.novari.kafka.tracing;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
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
 * Verifiserer at Spring Kafkas observasjonsstøtte propagerer W3C trace-kontekst gjennom det
 * programmatiske produsent-, konsument- og request/reply-oppsettet i biblioteket når
 * {@code fint.kafka.tracing.enabled=true}.
 */
@SpringBootTest(properties = {
    "management.tracing.sampling.probability=1.0",
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "fint.kafka.tracing.enabled=true"
})
@EmbeddedKafka(partitions = 1, kraft = true)
@AutoConfigureObservability
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TracingEnabledIntegrationTest {

    private static final String TRACEPARENT = "traceparent";

    private final ParameterizedTemplateFactory parameterizedTemplateFactory;
    private final ParameterizedListenerContainerFactoryService listenerContainerFactoryService;
    private final RequestTemplateFactory requestTemplateFactory;
    private final RequestListenerContainerFactory requestListenerContainerFactory;
    private final InMemorySpanExporter spanExporter;

    TracingEnabledIntegrationTest(
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
                .value(new TestObject(1, "enabled"))
                .build()
        );

        assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
        container.stop();
        return consumed.getFirst();
    }

    private List<SpanData> awaitSpans(int minCount) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            if (spanExporter.getFinishedSpanItems().size() >= minCount) {
                break;
            }
            Thread.sleep(100);
        }
        Thread.sleep(200);
        return spanExporter.getFinishedSpanItems();
    }

    @Test
    void producerAddsTraceparentHeaderToMessage() throws InterruptedException {
        ConsumerRecord<String, TestObject> record = produceAndConsume("traceparent-header");

        var traceparent = record.headers().lastHeader(TRACEPARENT);
        assertThat(traceparent)
            .as("produsenten skal legge W3C traceparent-header på meldingen når sporing er aktivert")
            .isNotNull();
        assertThat(new String(traceparent.value(), StandardCharsets.UTF_8))
            .matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");
    }

    @Test
    void consumerSpanIsChildOfProducerSpan() throws InterruptedException {
        ConsumerRecord<String, TestObject> record = produceAndConsume("parent-child-span");

        String traceIdFromHeader = new String(
            record.headers().lastHeader(TRACEPARENT).value(),
            StandardCharsets.UTF_8
        ).split("-")[1];

        List<SpanData> spans = awaitSpans(2).stream()
            .filter(span -> span.getName().contains("parent-child-span"))
            .toList();

        assertThat(spans)
            .as("skal ha både produsent- og konsument-span")
            .hasSizeGreaterThanOrEqualTo(2);
        assertThat(spans)
            .as("alle spans skal ha samme traceId som headeren på meldingen")
            .allSatisfy(span -> assertThat(span.getTraceId()).isEqualTo(traceIdFromHeader));

        SpanData producerSpan = spans.stream()
            .filter(span -> span.getKind().name().equals("PRODUCER"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("fant ikke produsent-span"));
        SpanData consumerSpan = spans.stream()
            .filter(span -> span.getKind().name().equals("CONSUMER"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("fant ikke konsument-span"));

        assertThat(consumerSpan.getParentSpanId())
            .as("konsument-spannet skal ha produsent-spannet som forelder")
            .isEqualTo(producerSpan.getSpanId());
    }

    @Test
    void requestReplyStaysInOneTraceAcrossProducerConsumerAndReply() throws InterruptedException {
        var prefix = TopicNamePrefixParameters
            .stepBuilder()
            .orgId("test-org-id")
            .domainContext("test-domain-context")
            .build();

        RequestTopicNameParameters requestTopic = RequestTopicNameParameters
            .builder()
            .topicNamePrefixParameters(prefix)
            .resourceName("enabled-resource")
            .parameterName("enabled-parameter")
            .build();

        ReplyTopicNameParameters replyTopic = ReplyTopicNameParameters
            .builder()
            .topicNamePrefixParameters(prefix)
            .applicationId("test-application-id")
            .resourceName("enabled-resource")
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

        List<SpanData> spans = awaitSpans(3);
        requestListenerContainer.stop();

        assertThat(reply).isNotNull();
        assertThat(traceparentSeenByServer)
            .as("forespørselen skal bære traceparent til den som svarer")
            .isNotEmpty()
            .doesNotContainNull();
        assertThat(spans.stream().map(SpanData::getTraceId).distinct())
            .as("hele request/reply-kallet skal ligge i ett spor")
            .hasSize(1);
    }

    @Test
    void batchListenerProducesNoConsumerSpan() throws InterruptedException {
        // Kjent begrensning i Spring Kafkas egen observasjonsstøtte - batch-lyttere gir aldri
        // consumer-span, uavhengig av dette biblioteket.
        var topicNameParameters = topic("batch-listener");
        CountDownLatch latch = new CountDownLatch(1);
        List<ConsumerRecord<String, TestObject>> consumed = new ArrayList<>();

        var container = listenerContainerFactoryService
            .createBatchListenerContainerFactory(
                TestObject.class,
                records -> {
                    consumed.addAll(records);
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

        parameterizedTemplateFactory.createTemplate(TestObject.class).send(
            ParameterizedProducerRecord
                .<TestObject>builder()
                .topicNameParameters(topicNameParameters)
                .key("test-key")
                .value(new TestObject(1, "batch"))
                .build()
        );

        assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(1000);
        container.stop();

        ConsumerRecord<String, TestObject> record = consumed.getFirst();
        assertThat(record.headers().lastHeader(TRACEPARENT))
            .as("produsentsiden instrumenteres uavhengig av mottakertype")
            .isNotNull();

        List<SpanData> batchSpans = spanExporter.getFinishedSpanItems().stream()
            .filter(span -> span.getName().contains("batch-listener"))
            .toList();
        long producerSpans = batchSpans.stream()
            .filter(span -> span.getKind().name().equals("PRODUCER"))
            .count();
        long consumerSpans = batchSpans.stream()
            .filter(span -> span.getKind().name().equals("CONSUMER"))
            .count();

        assertThat(producerSpans)
            .as("produsentsiden instrumenteres uavhengig av mottakertype")
            .isGreaterThanOrEqualTo(1);
        assertThat(consumerSpans)
            .as("dokumenterer at batch-lyttere ikke gir consumer-span")
            .isZero();
    }

}

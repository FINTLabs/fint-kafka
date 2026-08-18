package no.novari.kafka.consuming.integration;

import no.novari.kafka.TopicNameGenerator;
import no.novari.kafka.consumertracking.ConsumerTrackingService;
import no.novari.kafka.consumertracking.ConsumerTrackingTools;
import no.novari.kafka.consumertracking.event.PartitionsAssigned;
import no.novari.kafka.consumertracking.event.RecordsPolled;
import no.novari.kafka.consumertracking.event.predicates.OffsetCommittedPredicate;
import no.novari.kafka.consumertracking.event.predicates.PartitionsAssignedPredicate;
import no.novari.kafka.consumertracking.event.report.KeyValueReport;
import no.novari.kafka.consumertracking.event.report.TopicPartitionReport;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import no.novari.kafka.consuming.ErrorHandlerFactory;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.consuming.ListenerContainerFactoryService;
import no.novari.kafka.producing.TemplateFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fastholder de to sidene ved {@code auto.offset.reset=latest} som lett forveksles.
 * <p>
 * Mot {@code seekToEnd}: {@code latest} slår kun inn når gruppen mangler commitet offset, mens
 * {@code seekToEnd} kjører ved hver partisjonstilordning og ville gjort at meldinger produsert
 * mens konsumenten var nede gikk tapt for godt.
 * <p>
 * Men et commitet offset tilhører én bestemt consumer group. En ny gruppe arver ingenting fra en
 * annen, og hopper derfor over alt som ble produsert mellom at den forrige applikasjonen stoppet
 * og den nye startet — se {@code newConsumerGroupIgnoresAnotherGroupsCommittedOffsetAndSkipsTheGap}.
 * Det er grunnen til at {@code latest} ikke kan brukes når en applikasjon overtar en topic fra en
 * annen.
 */
@SpringBootTest(properties = "spring.kafka.consumer.auto-offset-reset=latest")
@EmbeddedKafka(partitions = 1, kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class AutoOffsetResetLatestIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final TopicNameGenerator topicNameGenerator = new TopicNameGenerator(4242);

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

    @Test
    void consumerGroupWithoutCommittedOffsetIsPositionedAtTheEndOfTheTopic() {
        final String topic = topicNameGenerator.generateRandomTopicName();
        final TopicPartitionReport topicPartition = new TopicPartitionReport(topic, 0);

        send(topic, 1, 2);

        ConsumerTrackingTools<String> tracking = consumerTrackingService.createConsumerTrackingTools(
            new PartitionsAssignedPredicate<>(topicPartition)
        );
        ConcurrentMessageListenerContainer<String, String> container = createContainer(topic, tracking);
        container.start();

        assertThat(tracking.waitForEventCondition(TIMEOUT)).isTrue();
        assertThat(tracking.getFilteredEvents(PartitionsAssigned.class))
            .isEqualTo(List.of(new PartitionsAssigned<>(Map.of(topicPartition, 2L))));

        container.stop();
    }

    @Test
    void restartResumesFromCommittedOffsetWithoutSkippingRecordsProducedWhileDown() {
        final String topic = topicNameGenerator.generateRandomTopicName();
        final TopicPartitionReport topicPartition = new TopicPartitionReport(topic, 0);

        send(topic, 1, 2);

        ConsumerTrackingTools<String> assignmentTracking =
            consumerTrackingService.createConsumerTrackingTools(
                new PartitionsAssignedPredicate<>(topicPartition)
            );
        ConcurrentMessageListenerContainer<String, String> container =
            createContainer(topic, assignmentTracking);
        container.start();

        assertThat(assignmentTracking.waitForEventCondition(TIMEOUT)).isTrue();
        assertThat(assignmentTracking.getFilteredEvents(PartitionsAssigned.class))
            .isEqualTo(List.of(new PartitionsAssigned<>(Map.of(topicPartition, 2L))));

        ConsumerTrackingTools<String> firstRunTracking =
            consumerTrackingService.createConsumerTrackingTools(
                new OffsetCommittedPredicate<>(topicPartition, 4L)
            );
        send(topic, 3, 4);

        assertThat(firstRunTracking.waitForEventCondition(TIMEOUT)).isTrue();
        assertThat(polledKeys(firstRunTracking)).containsExactly("key3", "key4");

        container.stop();

        send(topic, 5, 6);

        ConsumerTrackingTools<String> restartTracking =
            consumerTrackingService.createConsumerTrackingTools(
                new OffsetCommittedPredicate<>(topicPartition, 6L)
            );
        ConcurrentMessageListenerContainer<String, String> restartedContainer =
            createContainer(topic, restartTracking);
        restartedContainer.start();

        assertThat(restartTracking.waitForEventCondition(TIMEOUT)).isTrue();
        assertThat(restartTracking.getFilteredEvents(PartitionsAssigned.class))
            .isEqualTo(List.of(new PartitionsAssigned<>(Map.of(topicPartition, 4L))));
        assertThat(polledKeys(restartTracking)).containsExactly("key5", "key6");

        restartedContainer.stop();
    }

    /**
     * Cutover-scenariet: en annen applikasjons commitede offset tilhører den applikasjonens
     * consumer group, og har ingen virkning på en ny gruppe. Meldinger produsert i gapet mellom
     * at den gamle tjenesten stoppes og den nye starter, blir derfor hoppet over.
     */
    @Test
    void newConsumerGroupIgnoresAnotherGroupsCommittedOffsetAndSkipsTheGap() {
        final String topic = topicNameGenerator.generateRandomTopicName();
        final TopicPartitionReport topicPartition = new TopicPartitionReport(topic, 0);

        ConsumerTrackingTools<String> oldApplicationAssignment =
            consumerTrackingService.createConsumerTrackingTools(
                new PartitionsAssignedPredicate<>(topicPartition)
            );
        ConcurrentMessageListenerContainer<String, String> oldApplication =
            createContainer(
                topic,
                oldApplicationAssignment,
                continueFromPreviousOffset("-old-application")
            );
        oldApplication.start();

        assertThat(oldApplicationAssignment.waitForEventCondition(TIMEOUT)).isTrue();

        ConsumerTrackingTools<String> oldApplicationRun =
            consumerTrackingService.createConsumerTrackingTools(
                new OffsetCommittedPredicate<>(topicPartition, 2L)
            );
        send(topic, 1, 2);

        assertThat(oldApplicationRun.waitForEventCondition(TIMEOUT)).isTrue();
        assertThat(polledKeys(oldApplicationRun)).containsExactly("key1", "key2");

        oldApplication.stop();

        send(topic, 3, 4);

        ConsumerTrackingTools<String> newApplicationTracking =
            consumerTrackingService.createConsumerTrackingTools(
                new PartitionsAssignedPredicate<>(topicPartition)
            );
        ConcurrentMessageListenerContainer<String, String> newApplication =
            createContainer(
                topic,
                newApplicationTracking,
                continueFromPreviousOffset("-new-application")
            );
        newApplication.start();

        assertThat(newApplicationTracking.waitForEventCondition(TIMEOUT)).isTrue();
        assertThat(newApplicationTracking.getFilteredEvents(PartitionsAssigned.class))
            .isEqualTo(List.of(new PartitionsAssigned<>(Map.of(topicPartition, 4L))));

        newApplication.stop();
    }

    private static ListenerConfiguration continueFromPreviousOffset() {
        return ListenerConfiguration
            .stepBuilder()
            .groupIdApplicationDefault()
            .maxPollRecordsKafkaDefault()
            .maxPollIntervalKafkaDefault()
            .continueFromPreviousOffsetOnAssignment()
            .build();
    }

    private static ListenerConfiguration continueFromPreviousOffset(String groupIdSuffix) {
        return ListenerConfiguration
            .stepBuilder()
            .groupIdApplicationDefaultWithSuffix(groupIdSuffix)
            .maxPollRecordsKafkaDefault()
            .maxPollIntervalKafkaDefault()
            .continueFromPreviousOffsetOnAssignment()
            .build();
    }

    private ConcurrentMessageListenerContainer<String, String> createContainer(
        String topic,
        ConsumerTrackingTools<String> tracking
    ) {
        return createContainer(topic, tracking, continueFromPreviousOffset());
    }

    private ConcurrentMessageListenerContainer<String, String> createContainer(
        String topic,
        ConsumerTrackingTools<String> tracking,
        ListenerConfiguration listenerConfiguration
    ) {
        return listenerContainerFactoryService
            .createRecordListenerContainerFactory(
                String.class,
                _ -> {
                },
                tracking.wrapListenerConfigurationWithAssignmentTracking(listenerConfiguration),
                errorHandlerFactory.createErrorHandler(
                    tracking.wrapErrorHandlerConfigWithCustomRecovererTracking(
                        ErrorHandlerConfiguration
                            .<String>stepBuilder()
                            .noRetries()
                            .skipFailedRecords()
                            .build()
                    )
                ),
                tracking::registerContainerTracking
            )
            .createContainer(topic);
    }

    private void send(String topic, int fromInclusive, int toInclusive) {
        IntStream
            .rangeClosed(fromInclusive, toInclusive)
            .forEach(i -> {
                try {
                    template
                        .send(topic, "key" + i, "value" + i)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread
                        .currentThread()
                        .interrupt();
                    throw new IllegalStateException(e);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
    }

    @SuppressWarnings("unchecked")
    private static List<String> polledKeys(ConsumerTrackingTools<String> tracking) {
        return tracking
            .getFilteredEvents(RecordsPolled.class)
            .stream()
            .map(event -> ((RecordsPolled<String>) event).getRecords())
            .flatMap(records -> records
                .values()
                .stream())
            .flatMap(List::stream)
            .map(KeyValueReport::getKey)
            .toList();
    }

}

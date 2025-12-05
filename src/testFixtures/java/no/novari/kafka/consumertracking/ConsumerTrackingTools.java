package no.novari.kafka.consumertracking;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.novari.kafka.consumertracking.event.Event;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import no.novari.kafka.consuming.ListenerConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.util.TriConsumer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ConsumerTrackingTools<VALUE> {
    @Getter
    private final List<Event<VALUE>> events;
    @Getter
    private final Consumer<ConcurrentMessageListenerContainer<String, VALUE>> registerContainerTracking;
    private final Consumer<ConsumerRecord<String, VALUE>> customRecovererCalledCallback;
    private final Consumer<Map<TopicPartition, Long>> onAssignmentCallback;
    private final Consumer<Collection<TopicPartition>> onRevokeCallback;
    private final Function<Duration, Boolean> waitForEventCondition;

    public void registerContainerTracking(ConcurrentMessageListenerContainer<String, VALUE> container) {
        registerContainerTracking.accept(container);
    }

    public ListenerConfiguration wrapListenerConfigurationWithAssignmentTracking(
            ListenerConfiguration listenerConfiguration
    ) {
        return listenerConfiguration
                .toBuilder()
                .onPartitionsAssignedConsumer(
                        listenerConfiguration
                                .getOnPartitionsAssignedConsumer()
                                .<BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback>>map(
                                        onPartitionsAssignedConsumer ->
                                                (assignments, callback) -> {
                                                    onAssignmentCallback.accept(assignments);
                                                    onPartitionsAssignedConsumer.accept(assignments, callback);
                                                })
                                .orElse(
                                        (assignments, callback) ->
                                                onAssignmentCallback.accept(assignments)
                                )
                )
                .onPartitionsRevokedConsumer(
                        listenerConfiguration
                                .getOnPartitionsRevokedConsumer()
                                .<Consumer<Collection<TopicPartition>>>map(
                                        onPartitionsRevokedConsumer ->
                                                partitions -> {
                                                    onRevokeCallback.accept(partitions);
                                                    onPartitionsRevokedConsumer.accept(partitions);
                                                })
                                .orElse(onRevokeCallback)
                )
                .build();
    }

    public ErrorHandlerConfiguration<VALUE> wrapRecovererWithTracking(
            ErrorHandlerConfiguration<VALUE> errorHandlerConfiguration
    ) {
        return errorHandlerConfiguration
                .getCustomRecoverer()
                .<TriConsumer<
                        ConsumerRecord<String, VALUE>,
                        org.apache.kafka.clients.consumer.Consumer<String, VALUE>,
                        Exception>>map(
                        recoverer ->
                                (consumerRecord, consumer, e) -> {
                                    customRecovererCalledCallback.accept(consumerRecord);
                                    recoverer.accept(consumerRecord, consumer, e);
                                })
                .map(recoverer ->
                        errorHandlerConfiguration
                                .toBuilder()
                                .customRecoverer(recoverer)
                                .build()
                )
                .orElse(errorHandlerConfiguration);
    }

    public boolean waitForEventCondition(Duration timeout) {
        return waitForEventCondition.apply(timeout);
    }

    @SuppressWarnings("rawtypes")
    @SafeVarargs
    public final List<Event<VALUE>> getFilteredEvents(Class<? extends Event>... typeFilter) {
        Set<Class<? extends Event>> typeFilterSet = Set.of(typeFilter);
        return events
                .stream()
                .filter(event -> typeFilterSet.contains(event.getClass()))
                .collect(Collectors.toList());
    }
}

package no.novari.kafka.consumertracking;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.novari.kafka.consumertracking.events.Event;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.util.TriConsumer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ConsumerTrackingTools<VALUE> {
    @Getter
    private final String topic;
    @Getter
    private final List<Event<VALUE>> events;
    private final Consumer<ConcurrentMessageListenerContainer<String, VALUE>> registerContainerTracking;
    private final Consumer<ConsumerRecord<String, VALUE>> customRecovererCalledCallback;
    private final Function<Duration, Boolean> waitForFinalCommit;

    public void registerContainerTracking(ConcurrentMessageListenerContainer<String, VALUE> container) {
        registerContainerTracking.accept(container);
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

    public boolean waitForFinalCommit(Duration timeout) {
        return waitForFinalCommit.apply(timeout);
    }

    public List<Event<VALUE>> getFilteredEvents(Event.Type... typeFilter) {
        return events
                .stream()
                .filter(event ->
                        Arrays
                                .stream(typeFilter)
                                .collect(Collectors.toSet())
                                .contains(event.getType())
                )
                .collect(Collectors.toList());
    }
}

package no.novari.kafka.consumertracking;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.novari.kafka.consumertracking.events.Event;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ConsumerTrackingTools<V> {
    @Getter
    private final String topic;
    @Getter
    private final List<Event<V>> events;
    private final Consumer<ConcurrentMessageListenerContainer<String, V>> registerTracking;
    private final Function<Duration, Boolean> waitForFinalCommit;

    public void registerTracking(ConcurrentMessageListenerContainer<String, V> container) {
        registerTracking.accept(container);
    }

    public boolean waitForFinalCommit(Duration timeout) {
        return waitForFinalCommit.apply(timeout);
    }

    public List<Event<V>> getFilteredEvents(Event.Type... typeFilter) {
        return events.stream()
                .filter(event ->
                        Arrays.stream(typeFilter).collect(Collectors.toSet())
                                .contains(event.getType())
                )
                .collect(Collectors.toList());
    }
}

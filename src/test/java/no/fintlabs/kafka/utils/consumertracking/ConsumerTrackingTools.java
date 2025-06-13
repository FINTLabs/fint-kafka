package no.fintlabs.kafka.utils.consumertracking;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.fintlabs.kafka.utils.consumertracking.events.Event;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

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

}

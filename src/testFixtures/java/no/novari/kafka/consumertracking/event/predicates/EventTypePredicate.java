package no.novari.kafka.consumertracking.event.predicates;

import lombok.AllArgsConstructor;
import no.novari.kafka.consumertracking.event.Event;

@AllArgsConstructor
public class EventTypePredicate<VALUE> implements EventPredicate<VALUE> {
    @SuppressWarnings("rawtypes")
    private final Class<? extends Event> eventType;

    @Override
    public boolean test(Event<VALUE> event) {
        return eventType.isAssignableFrom(event.getClass());
    }
}

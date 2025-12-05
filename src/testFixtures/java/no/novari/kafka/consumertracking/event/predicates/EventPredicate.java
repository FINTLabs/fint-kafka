package no.novari.kafka.consumertracking.event.predicates;

import no.novari.kafka.consumertracking.event.Event;

import java.util.function.Predicate;

public interface EventPredicate<VALUE> extends Predicate<Event<VALUE>> {
}

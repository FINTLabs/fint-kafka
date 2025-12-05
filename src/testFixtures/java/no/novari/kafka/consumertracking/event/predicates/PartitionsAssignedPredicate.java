package no.novari.kafka.consumertracking.event.predicates;

import no.novari.kafka.consumertracking.event.Event;
import no.novari.kafka.consumertracking.event.PartitionsAssigned;
import no.novari.kafka.consumertracking.event.reports.TopicPartitionReport;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PartitionsAssignedPredicate<VALUE> implements EventPredicate<VALUE> {

    private final Set<TopicPartitionReport> requiredAssignments;

    public PartitionsAssignedPredicate(TopicPartitionReport... topicPartitions) {
        this.requiredAssignments = new HashSet<>(Arrays.asList(topicPartitions));
    }

    @Override
    public boolean test(Event<VALUE> event) {
        return event instanceof PartitionsAssigned<VALUE> &&
               ((PartitionsAssigned<VALUE>) event)
                       .getAssignments()
                       .keySet()
                       .containsAll(requiredAssignments);
    }

}

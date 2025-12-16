package no.novari.kafka.consumertracking.event.predicates;

import no.novari.kafka.consumertracking.event.Event;
import no.novari.kafka.consumertracking.event.PartitionsAssigned;
import no.novari.kafka.consumertracking.event.report.TopicPartitionReport;

import java.util.Map;
import java.util.Objects;

public class PartitionsAssignedWithOffsetPredicate<VALUE> implements EventPredicate<VALUE> {

    private final Map<TopicPartitionReport, Long> requiredAssignments;

    public PartitionsAssignedWithOffsetPredicate(Map<TopicPartitionReport, Long> assignments) {
        this.requiredAssignments = assignments;
    }

    @Override
    public boolean test(Event<VALUE> event) {
        if (!(event instanceof PartitionsAssigned<VALUE>)) {
            return false;
        }
        Map<TopicPartitionReport, Long> assignments = ((PartitionsAssigned<VALUE>) event).getAssignments();
        return assignments
                       .keySet()
                       .equals(requiredAssignments.keySet())
               &&
               assignments
                       .entrySet()
                       .stream()
                       .allMatch(entry ->
                               Objects.equals(entry.getValue(), requiredAssignments.get(entry.getKey()))
                       );
    }

}

package no.novari.kafka.consumertracking.event.predicates;

import lombok.AllArgsConstructor;
import no.novari.kafka.consumertracking.event.Event;
import no.novari.kafka.consumertracking.event.OffsetsCommitted;
import no.novari.kafka.consumertracking.event.reports.TopicPartitionReport;

@AllArgsConstructor
public class OffsetCommittedPredicate<VALUE> implements EventPredicate<VALUE> {

    private final TopicPartitionReport topicPartition;
    private final long offset;

    @Override
    public boolean test(Event<VALUE> event) {
        return event instanceof OffsetsCommitted<VALUE> &&
               ((OffsetsCommitted<VALUE>) event)
                       .getOffsets()
                       .getOrDefault(topicPartition, -1L) >= offset;
    }

}

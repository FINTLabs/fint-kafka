package no.novari.kafka.consumertracking.event;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.consumertracking.event.report.KeyValueReport;
import no.novari.kafka.consumertracking.event.report.TopicPartitionReport;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class ListenerSuccessfullyProcessedRecord<VALUE> implements Event<VALUE> {
    private final TopicPartitionReport topicPartition;
    private final KeyValueReport<VALUE> record;
}

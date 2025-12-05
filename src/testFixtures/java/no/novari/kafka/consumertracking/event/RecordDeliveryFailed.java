package no.novari.kafka.consumertracking.event;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.consumertracking.event.reports.ExceptionReport;
import no.novari.kafka.consumertracking.event.reports.KeyValueReport;
import no.novari.kafka.consumertracking.event.reports.TopicPartitionReport;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class RecordDeliveryFailed<VALUE> implements Event<VALUE> {
    private final ExceptionReport exception;
    private final TopicPartitionReport topicPartition;
    private final KeyValueReport<VALUE> record;
    private final Integer attempt;
}

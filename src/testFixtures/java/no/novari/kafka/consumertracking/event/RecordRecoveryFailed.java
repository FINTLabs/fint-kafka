package no.novari.kafka.consumertracking.event;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.consumertracking.event.report.ExceptionReport;
import no.novari.kafka.consumertracking.event.report.KeyValueReport;
import no.novari.kafka.consumertracking.event.report.TopicPartitionReport;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class RecordRecoveryFailed<VALUE> implements Event<VALUE> {
    private final ExceptionReport exception;
    private final TopicPartitionReport topicPartition;
    private final KeyValueReport<VALUE> keyValue;
}

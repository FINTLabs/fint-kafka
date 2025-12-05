package no.novari.kafka.consumertracking.event;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.consumertracking.event.report.ExceptionReport;
import no.novari.kafka.consumertracking.event.report.TopicPartitionReport;

import java.util.Set;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class OffsetCommitFailed<VALUE> implements Event<VALUE> {
    private final ExceptionReport exception;
    private final Set<TopicPartitionReport> topicPartitions;
}

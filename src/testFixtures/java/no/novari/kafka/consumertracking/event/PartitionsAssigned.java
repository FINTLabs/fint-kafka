package no.novari.kafka.consumertracking.event;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.consumertracking.event.report.TopicPartitionReport;

import java.util.Map;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class PartitionsAssigned<VALUE> implements Event<VALUE> {
    private final Map<TopicPartitionReport, Long> assignments;
}

package no.novari.kafka.consumertracking.event.report;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class TopicPartitionReport {
    String topic;
    int partition;
}

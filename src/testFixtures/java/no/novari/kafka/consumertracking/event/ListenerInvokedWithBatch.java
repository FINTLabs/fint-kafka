package no.novari.kafka.consumertracking.event;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.consumertracking.event.report.KeyValueReport;
import no.novari.kafka.consumertracking.event.report.TopicPartitionReport;

import java.util.List;
import java.util.Map;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public final class ListenerInvokedWithBatch<VALUE> implements Event<VALUE> {
    private final Map<TopicPartitionReport, List<KeyValueReport<VALUE>>> records;
}

package no.fintlabs.kafka.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class RecordsReport<V> implements EventReport<V> {
    private final List<RecordReport<V>> records;
}

package no.fintlabs.kafka.utils.consumertracking.reports;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class RecordReport<V> {
    private String key;
    private V value;
}

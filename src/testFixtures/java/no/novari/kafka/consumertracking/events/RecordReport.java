package no.novari.kafka.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class RecordReport<V> implements EventReport<V> {
    private String key;
    private V value;
}

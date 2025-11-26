package no.novari.kafka.consumertracking;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class RecordReport<VALUE> {
    private final String key;
    private final VALUE value;
}

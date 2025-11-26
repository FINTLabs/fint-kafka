package no.novari.kafka.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class OffsetsCommitted<VALUE> implements Event<VALUE> {
    private final List<Long> offsets;

    public OffsetsCommitted(Long... offsets) {
        this(Arrays.asList(offsets));
    }
}

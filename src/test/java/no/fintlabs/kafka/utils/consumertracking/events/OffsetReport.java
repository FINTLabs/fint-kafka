package no.fintlabs.kafka.utils.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class OffsetReport<V> implements EventReport<V> {
    private final List<Long> offsets;

    public OffsetReport(Long offset) {
        this.offsets = List.of(offset);
    }

}

package no.novari.kafka.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.consumertracking.RecordReport;

import java.util.List;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class BatchRecovered<VALUE> implements Event<VALUE> {
    private final List<RecordReport<VALUE>> records;

    @SafeVarargs
    public BatchRecovered(RecordReport<VALUE>... records) {
        this(List.of(records));
    }
}

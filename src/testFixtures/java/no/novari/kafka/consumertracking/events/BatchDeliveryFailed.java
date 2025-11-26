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
public class BatchDeliveryFailed<VALUE> implements Event<VALUE> {
    private final Class<? extends Exception> type;
    private final String message;
    private final Integer attempt;
    private final List<RecordReport<VALUE>> records;

    @SafeVarargs
    public BatchDeliveryFailed(
            Class<? extends Exception> type,
            String message,
            Integer attempt,
            RecordReport<VALUE>... records
    ) {
        this(type, message, attempt, List.of(records));
    }
}

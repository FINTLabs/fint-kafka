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
public class BatchRecoveryFailed<VALUE> implements Event<VALUE> {
    private final Class<? extends Exception> type;
    private final String message;
    private final List<RecordReport<VALUE>> records;

    @SafeVarargs
    public BatchRecoveryFailed(
            Class<? extends Exception> type,
            String message,
            RecordReport<VALUE>... records
    ) {
        this(type, message, List.of(records));
    }
}

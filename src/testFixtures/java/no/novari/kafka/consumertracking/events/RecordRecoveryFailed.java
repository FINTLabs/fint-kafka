package no.novari.kafka.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.consumertracking.RecordReport;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class RecordRecoveryFailed<VALUE> implements Event<VALUE> {
    private final Class<? extends Exception> type;
    private final String message;
    private final RecordReport<VALUE> record;
}

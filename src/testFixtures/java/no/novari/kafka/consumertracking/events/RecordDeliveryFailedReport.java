package no.novari.kafka.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
@EqualsAndHashCode
public class RecordDeliveryFailedReport<V> implements EventReport<V> {
    private final RecordReport<V> record;
    private final ExceptionReport<V> cause;
    private final int attempt;
}

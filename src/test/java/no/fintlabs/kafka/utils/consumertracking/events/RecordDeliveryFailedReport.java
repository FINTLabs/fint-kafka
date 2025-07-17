package no.fintlabs.kafka.utils.consumertracking.events;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@Builder
@EqualsAndHashCode
public class RecordDeliveryFailedReport<V> implements EventReport<V> {
    private final RecordReport<V> record;
    private final ExceptionReport<V> cause;
    private final int attempt;
}

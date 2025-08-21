package no.fintlabs.kafka.consumertracking.events;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@Builder
@EqualsAndHashCode
public class BatchDeliveryFailedReport<V> implements EventReport<V> {
    private final RecordsReport<V> records;
    private final ExceptionReport<V> cause;
    private final int attempt;
}

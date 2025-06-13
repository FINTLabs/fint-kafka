package no.fintlabs.kafka.utils.consumertracking.events;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class BatchDeliveryFailedReport<V> implements EventReport<V> {
    private final RecordsReport<V> records;
    private final ExceptionReport<V> cause;
    private final int attempt;
}

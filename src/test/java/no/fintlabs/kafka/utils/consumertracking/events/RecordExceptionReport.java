package no.fintlabs.kafka.utils.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class RecordExceptionReport<V> implements EventReport<V> {
    private RecordReport<V> record;
    private ExceptionReport<V> exception;
}

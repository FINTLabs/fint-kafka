package no.fintlabs.kafka.utils.consumertracking.reports;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class RecordExceptionReport<V> {
    private RecordReport<V> record;
    private ExceptionReport exception;
}

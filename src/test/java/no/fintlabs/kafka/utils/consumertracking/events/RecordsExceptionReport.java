package no.fintlabs.kafka.utils.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class RecordsExceptionReport<V> implements EventReport<V> {
    private List<RecordReport<V>> records;
    @Getter
    private ExceptionReport<V> cause;

}

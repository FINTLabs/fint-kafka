package no.fintlabs.kafka.utils.consumertracking.reports;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

@AllArgsConstructor
@EqualsAndHashCode
public class RecordsExceptionReport<V> {
    private List<RecordReport<V>> records;
    @Getter
    private ExceptionReport exception;

    public List<RecordReport<V>> getRecords() {
        return Collections.unmodifiableList(records);
    }
}

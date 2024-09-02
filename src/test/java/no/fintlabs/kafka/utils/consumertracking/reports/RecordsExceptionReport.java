package no.fintlabs.kafka.utils.consumertracking.reports;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

@AllArgsConstructor
@EqualsAndHashCode
public class RecordsExceptionReport {
    private List<RecordReport> records;
    @Getter
    private ExceptionReport exception;

    public List<RecordReport> getRecords() {
        return Collections.unmodifiableList(records);
    }
}

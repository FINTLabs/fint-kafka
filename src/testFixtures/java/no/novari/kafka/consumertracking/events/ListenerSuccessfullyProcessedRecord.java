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
public class ListenerSuccessfullyProcessedRecord<VALUE> implements Event<VALUE> {
    private final RecordReport<VALUE> record;
}

package no.novari.kafka.consumertracking;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class ExceptionReport {
    private final Class<? extends Exception> type;
    private final String message;
}

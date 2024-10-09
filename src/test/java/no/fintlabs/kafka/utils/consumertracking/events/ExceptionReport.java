package no.fintlabs.kafka.utils.consumertracking.events;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class ExceptionReport<V> implements EventReport<V> {
    private Class<? extends Exception> className;
    private String message;
}

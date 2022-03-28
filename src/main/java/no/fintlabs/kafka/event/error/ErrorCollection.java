package no.fintlabs.kafka.event.error;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collection;

@Data
@AllArgsConstructor
public class ErrorCollection {

    public ErrorCollection() {
        errors = new ArrayList<>();
    }

    private Collection<Error> errors;

}

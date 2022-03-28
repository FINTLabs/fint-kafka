package no.fintlabs.kafka.event.error;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

@Data
@AllArgsConstructor
public class ErrorCollection {

    public ErrorCollection() {
        errors = new ArrayList<>();
    }

    public ErrorCollection(Error... errors) {
        this.errors = Arrays.stream(errors).toList();
    }

    private Collection<Error> errors;

}

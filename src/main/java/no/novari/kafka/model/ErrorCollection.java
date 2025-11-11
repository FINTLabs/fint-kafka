package no.novari.kafka.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Collection;

@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class ErrorCollection {

    public ErrorCollection(Error... errors) {
        this(Arrays.stream(errors).toList());
    }

    private Collection<Error> errors;

}

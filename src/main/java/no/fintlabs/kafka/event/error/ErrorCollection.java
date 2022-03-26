package no.fintlabs.kafka.event.error;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorCollection {
    private Collection<Error> errors;
}


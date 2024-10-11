package no.fintlabs.kafka.model;

import lombok.*;

import java.util.Map;

@EqualsAndHashCode
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Error {
    private String errorCode;
    private Map<String, String> args;
}

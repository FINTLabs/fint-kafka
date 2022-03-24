package no.fintlabs.kafka.event.error;

import lombok.Builder;

import java.time.LocalDateTime;
import java.util.Map;

@Builder
public class ErrorEvent {

    private String errorCode;
    private String description;
    private LocalDateTime timestamp;
    private Map<String, String> args;

}

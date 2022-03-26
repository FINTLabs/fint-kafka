package no.fintlabs.kafka.event.error;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Error {
    private String errorCode;
    private LocalDateTime timestamp;
    private Map<String, String> args;
}

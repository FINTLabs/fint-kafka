package no.fintlabs.kafka.logging;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.boot.logging.LogLevel;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

@AllArgsConstructor
public class LogEntry {

    @Getter
    String origin;

    @Getter
    LocalDateTime timestamp;

    @Getter
    Duration timeToLive;

    @Getter
    LogLevel logLevel;

    @Getter
    String message;

    Throwable throwable;

    public Optional<Throwable> getThrowable() {
        return throwable != null
                ? Optional.of(throwable)
                : Optional.empty();
    }

}

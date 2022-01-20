package no.fintlabs.kafka.logging;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogEvent {

    public enum Level {
        TRACE, DEBUG, INFO, WARN, ERROR
    }

    private long timestamp;
    private String threadName;
    private String loggerName;
    private Level level;
    private String message;
    private Throwable throwable;

}

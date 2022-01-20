package no.fintlabs.kafka.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import org.springframework.stereotype.Service;

@Service
public class LogbackLogEventMappingService {

    public LogEvent map(ILoggingEvent loggingEvent) {
        return LogEvent.builder()
                .timestamp(loggingEvent.getTimeStamp())
                .loggerName(loggingEvent.getLoggerName())
                .threadName(loggingEvent.getThreadName())
                .level(this.map(loggingEvent.getLevel()))
                .message(loggingEvent.getFormattedMessage())
                .throwable(loggingEvent.getThrowableProxy() != null
                        ? ((ThrowableProxy) loggingEvent.getThrowableProxy()).getThrowable()
                        : null)
                .build();
    }

    private LogEvent.Level map(ch.qos.logback.classic.Level level) {
        switch (level.levelInt) {
            case ch.qos.logback.classic.Level.TRACE_INT:
                return LogEvent.Level.TRACE;
            case ch.qos.logback.classic.Level.DEBUG_INT:
                return LogEvent.Level.DEBUG;
            case ch.qos.logback.classic.Level.INFO_INT:
                return LogEvent.Level.INFO;
            case ch.qos.logback.classic.Level.WARN_INT:
                return LogEvent.Level.WARN;
            case ch.qos.logback.classic.Level.ERROR_INT:
                return LogEvent.Level.ERROR;
            default:
                return null;
        }
    }

}

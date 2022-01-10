package no.fintlabs.kafka.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;

// Avoids recursive logging in KafkaAppender
public class KafkaLoggerFilter extends AbstractFilter {

    private Result filterKafkaLogger(String loggerName) {
        return loggerName.startsWith("org.apache.kafka")
                ? Result.DENY
                : Result.NEUTRAL;
    }

    @Override
    public Result filter(LogEvent event) {
        return filterKafkaLogger(event.getLoggerName());
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
        return filterKafkaLogger(logger.getName());
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Object msg, Throwable t) {
        return filterKafkaLogger(logger.getName());
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, String msg, Object... params) {
        return filterKafkaLogger(logger.getName());
    }
}

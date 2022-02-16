package no.fintlabs.kafka.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import spock.lang.Specification

class LogbackLogEventMappingServiceSpec extends Specification {

    def 'should map logging event without throwable'() {
        given:
        ILoggingEvent loggingEvent = new LoggingEvent()
        loggingEvent.setTimeStamp(1000L)
        loggingEvent.setThreadName("test.thread.name")
        loggingEvent.setLoggerName("test.logger.name")
        loggingEvent.setLevel(Level.INFO)
        loggingEvent.setMessage("test message")
        loggingEvent.setThrowableProxy(null)
        when:
        def result = new LogbackLogEventMappingService().map(loggingEvent)
        then:
        result.getTimestamp() == 1000L
        result.getThreadName() == "test.thread.name"
        result.getLoggerName() == "test.logger.name"
        result.getLevel() == LogEvent.Level.INFO
        result.getMessage() == "test message"
        result.getThrowable() == null
    }

    def 'should map logging event with throwable'() {
        given:
        ILoggingEvent loggingEvent = new LoggingEvent()
        loggingEvent.setTimeStamp(1000L)
        loggingEvent.setThreadName("test.thread.name")
        loggingEvent.setLoggerName("test.logger.name")
        loggingEvent.setLevel(Level.INFO)
        loggingEvent.setMessage("test message")
        loggingEvent.setThrowableProxy(new ThrowableProxy(new RuntimeException("test exception")))
        when:
        def result = new LogbackLogEventMappingService().map(loggingEvent)
        then:
        result.getTimestamp() == 1000L
        result.getThreadName() == "test.thread.name"
        result.getLoggerName() == "test.logger.name"
        result.getLevel() == LogEvent.Level.INFO
        result.getMessage() == "test message"
        result.getThrowable() != null
        result.getThrowable().getMessage() == "test exception"
    }
}

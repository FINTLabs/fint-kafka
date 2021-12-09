package no.fintlabs.kafka.logging;

import no.fintlabs.kafka.topic.TopicNameService;
import org.springframework.boot.logging.LogLevel;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;

@Service
public class LoggingService {

    // TODO: 09/12/2021 Replace placeholder
    private static final String origin = "originPlaceholder";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public LoggingService(KafkaTemplate<String, Object> kafkaTemplate, TopicNameService topicNameService) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplate.setDefaultTopic(topicNameService.getLogTopicName());
    }

    public void info(TimeToLiveLevel timeToLiveLevel, String message) {
        this.info(timeToLiveLevel.getTimeToLive(), message);
    }

    public void info(Duration timeToLive, String message) {
        this.kafkaTemplate.sendDefault(
                new LogEntry(
                        origin,
                        LocalDateTime.now(),
                        timeToLive,
                        LogLevel.INFO,
                        message,
                        null
                )
        );
    }

    public void warn(TimeToLiveLevel timeToLiveLevel, String message) {
        this.warn(timeToLiveLevel.getTimeToLive(), message);
    }

    public void warn(Duration timeToLive, String message) {
        this.warn(timeToLive, message, null);
    }

    public void warn(TimeToLiveLevel timeToLiveLevel, String message, Throwable throwable) {
        this.warn(timeToLiveLevel.getTimeToLive(), message, throwable);
    }

    public void warn(Duration timeToLive, String message, Throwable throwable) {
        this.kafkaTemplate.sendDefault(
                new LogEntry(
                        origin,
                        LocalDateTime.now(),
                        timeToLive,
                        LogLevel.WARN,
                        message,
                        throwable
                )
        );
    }

    public void error(TimeToLiveLevel timeToLiveLevel, String message) {
        this.error(timeToLiveLevel.getTimeToLive(), message);
    }

    public void error(Duration timeToLive, String message) {
        this.error(timeToLive, message, null);
    }

    public void error(TimeToLiveLevel timeToLiveLevel, String message, Throwable throwable) {
        this.error(timeToLiveLevel.getTimeToLive(), message, throwable);
    }

    public void error(Duration timeToLive, String message, Throwable throwable) {
        this.kafkaTemplate.sendDefault(
                new LogEntry(
                        origin,
                        LocalDateTime.now(),
                        timeToLive,
                        LogLevel.ERROR,
                        message,
                        throwable
                )
        );
    }

}

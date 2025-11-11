package no.novari.kafka.requestreply.topic.configuration;

import lombok.Builder;
import lombok.Getter;

import java.time.Duration;

@Getter
@Builder
public class RequestTopicConfiguration {
    private final Duration retentionTime;
}

package no.novari.kafka.requestreply.topic.configuration;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.Duration;

@Getter
@Builder
@EqualsAndHashCode
@ToString
public class RequestTopicConfiguration {
    private final Duration retentionTime;
}

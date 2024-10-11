package no.fintlabs.kafka.topic.configuration;

import lombok.*;

import java.time.Duration;

@ToString
@EqualsAndHashCode
@Getter
@Builder
public class TopicDeleteCleanupPolicyConfiguration {
    @NonNull
    private final Duration retentionTime;
}

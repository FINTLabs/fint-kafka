package no.fintlabs.kafka.common.topic.configuration;

import lombok.Builder;

import java.time.Duration;
import java.util.Optional;

@Builder
public class TopicDeleteCleanupPolicyConfiguration {

    private final Duration retentionTime;

    public Optional<Duration> getRetentionTime() {
        return Optional.ofNullable(retentionTime);
    }

}

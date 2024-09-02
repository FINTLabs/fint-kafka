package no.fintlabs.kafka.common.topic.configuration;

import lombok.Builder;

import java.time.Duration;
import java.util.Optional;

@Builder
public class TopicCompactCleanupPolicyConfiguration {

    private final Duration maxCompactionLag;
    private final Duration tombstoneRetentionTime;

    public Optional<Duration> getMaxCompactionLag() {
        return Optional.ofNullable(maxCompactionLag);
    }

    public Optional<Duration> getTombstoneRetentionTime() {
        return Optional.ofNullable(tombstoneRetentionTime);
    }
}

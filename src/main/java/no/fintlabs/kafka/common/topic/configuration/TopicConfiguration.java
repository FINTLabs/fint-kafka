package no.fintlabs.kafka.common.topic.configuration;

import lombok.Builder;

import java.util.Optional;

@Builder
public class TopicConfiguration {
    private final TopicDeleteCleanupPolicyConfiguration deleteCleanupPolicy;
    private final TopicCompactCleanupPolicyConfiguration compactCleanupPolicy;
    private final TopicSegmentConfiguration segment;

    public Optional<TopicDeleteCleanupPolicyConfiguration> getDeleteCleanupPolicyConfiguration() {
        return Optional.ofNullable(deleteCleanupPolicy);
    }

    public Optional<TopicCompactCleanupPolicyConfiguration> getCompactCleanupPolicyConfiguration() {
        return Optional.ofNullable(compactCleanupPolicy);
    }

    public Optional<TopicSegmentConfiguration> getSegmentConfiguration() {
        return Optional.ofNullable(segment);
    }

}

package no.fintlabs.kafka.topic.configuration;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

import java.util.Optional;

@ToString
@EqualsAndHashCode
@Builder
public class TopicConfiguration {

    private final TopicDeleteCleanupPolicyConfiguration deleteCleanupPolicy;

    private final TopicCompactCleanupPolicyConfiguration compactCleanupPolicy;

    @NonNull
    private final TopicSegmentConfiguration segment;

    public Optional<TopicDeleteCleanupPolicyConfiguration> getDeleteCleanupPolicyConfiguration() {
        return Optional.ofNullable(deleteCleanupPolicy);
    }

    public Optional<TopicCompactCleanupPolicyConfiguration> getCompactCleanupPolicyConfiguration() {
        return Optional.ofNullable(compactCleanupPolicy);
    }

    public TopicSegmentConfiguration getSegmentConfiguration() {
        return segment;
    }

}

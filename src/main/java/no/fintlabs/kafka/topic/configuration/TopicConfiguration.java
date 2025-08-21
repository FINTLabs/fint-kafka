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

    // TODO 21/08/2025 eivindmorch: Add to specialized topic classes (move req/reply code to new lib)
    private final Integer partitions;

    private final TopicDeleteCleanupPolicyConfiguration deleteCleanupPolicy;

    private final TopicCompactCleanupPolicyConfiguration compactCleanupPolicy;

    @NonNull
    private final TopicSegmentConfiguration segment;

    public Optional<Integer> getPartitions() {
        return Optional.ofNullable(partitions);
    }

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

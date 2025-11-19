package no.novari.kafka.topic.configuration;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.Optional;

@ToString
@EqualsAndHashCode
@Builder
public class TopicConfiguration {

    @Getter
    @NonNull
    private final Integer partitions;

    private final TopicDeleteCleanupPolicyConfiguration deleteCleanupPolicy;

    private final TopicCompactCleanupPolicyConfiguration compactCleanupPolicy;

    @Getter
    @NonNull
    private final TopicSegmentConfiguration segmentConfiguration;

    public Optional<TopicDeleteCleanupPolicyConfiguration> getDeleteCleanupPolicyConfiguration() {
        return Optional.ofNullable(deleteCleanupPolicy);
    }

    public Optional<TopicCompactCleanupPolicyConfiguration> getCompactCleanupPolicyConfiguration() {
        return Optional.ofNullable(compactCleanupPolicy);
    }

}

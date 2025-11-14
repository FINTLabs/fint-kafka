package no.novari.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.time.Duration;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class EventTopicConfiguration {

    // TODO 12/11/2025 eivindmorch: Rename for this and others
    public static EventTopicConfigurationStepBuilder.PartitionStepBuilder builder() {
        return EventTopicConfigurationStepBuilder.builder();
    }

    @NonNull
    private final Integer partitions;

    @NonNull
    private Duration retentionTime;

    @NonNull
    private EventCleanupFrequency cleanupFrequency;

}

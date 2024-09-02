package no.fintlabs.kafka.entity.topic;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import no.fintlabs.kafka.common.topic.configuration.values.CleanupFrequency;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.Optional;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class EntityTopicConfiguration {

    public static EntityTopicConfigurationBuilder.LastValueRetentionTimeStepBuilder builder() {
        return EntityTopicConfigurationBuilder.builder();
    }

    private Duration lastValueRetentionTime;
    private Duration nullValueRetentionTime;
    private CleanupFrequency cleanupFrequency;
    private DataSize maxSegmentSize;

    public Optional<Duration> getLastValueRetentionTime() {
        return Optional.ofNullable(lastValueRetentionTime);
    }

    public Optional<Duration> getNullValueRetentionTime() {
        return Optional.ofNullable(nullValueRetentionTime);
    }

    public Optional<CleanupFrequency> getCleanupFrequency() {
        return Optional.ofNullable(cleanupFrequency);
    }

    public Optional<DataSize> getMaxSegmentSize() {
        return Optional.ofNullable(maxSegmentSize);
    }

}

package no.fintlabs.kafka.event.topic;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import no.fintlabs.kafka.common.topic.configuration.values.CleanupFrequency;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.Optional;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class EventTopicConfiguration {

    public static EventTopicConfigurationBuilder.RetentionTimeStepBuilder builder() {
        return EventTopicConfigurationBuilder.builder();
    }

    private Duration retentionTime;
    private CleanupFrequency cleanupFrequency;
    private DataSize maxSegmentSize;

    public Optional<Duration> getRetentionTime() {
        return Optional.ofNullable(retentionTime);
    }

    public Optional<CleanupFrequency> getCleanupFrequency() {
        return Optional.ofNullable(cleanupFrequency);
    }

    public Optional<DataSize> getMaxSegmentSize() {
        return Optional.ofNullable(maxSegmentSize);
    }

}

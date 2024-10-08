package no.fintlabs.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.time.Duration;
import java.util.Optional;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class EntityTopicConfiguration {

    public static EntityTopicConfigurationBuilder.LastValueRetentionTimeStepBuilder builder() {
        return EntityTopicConfigurationBuilder.builder();
    }

    private Duration lastValueRetentionTime;

    @Getter
    @NonNull
    private Duration nullValueRetentionTime;

    @Getter
    @NonNull
    private CleanupFrequency cleanupFrequency;

    public Optional<Duration> getLastValueRetentionTime() {
        return Optional.ofNullable(lastValueRetentionTime);
    }

}

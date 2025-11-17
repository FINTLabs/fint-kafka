package no.novari.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.lang.NonNull;

import java.time.Duration;
import java.util.Optional;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class EntityTopicConfiguration {

    public static EntityTopicConfigurationStepBuilder.PartitionStep stepBuilder() {
        return EntityTopicConfigurationStepBuilder.firstStep();
    }

    @Getter
    @NonNull
    private final Integer partitions;

    private Duration lastValueRetentionTime;

    @Getter
    @NonNull
    private Duration nullValueRetentionTime;

    @Getter
    @NonNull
    private EntityCleanupFrequency cleanupFrequency;

    public Optional<Duration> getLastValueRetentionTime() {
        return Optional.ofNullable(lastValueRetentionTime);
    }

}

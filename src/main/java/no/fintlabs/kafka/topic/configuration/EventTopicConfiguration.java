package no.fintlabs.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.time.Duration;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class EventTopicConfiguration {

    public static EventTopicConfigurationBuilder.RetentionTimeStepBuilder builder() {
        return EventTopicConfigurationBuilder.builder();
    }

    @NonNull
    private Duration retentionTime;

    @NonNull
    private CleanupFrequency cleanupFrequency;

}

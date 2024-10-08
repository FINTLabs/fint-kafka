package no.fintlabs.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.time.Duration;

import static no.fintlabs.kafka.topic.configuration.ErrorEventTopicConfigurationBuilder.RetentionTimeStepBuilder;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ErrorEventTopicConfiguration {

    public static RetentionTimeStepBuilder builder() {
        return ErrorEventTopicConfigurationBuilder.builder();
    }

    private @NonNull Duration retentionTime;
    private @NonNull CleanupFrequency cleanupFrequency;

}

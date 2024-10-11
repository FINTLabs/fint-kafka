package no.fintlabs.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.time.Duration;

public class EventTopicConfigurationBuilder {

    private EventTopicConfigurationBuilder() {
    }

    public static RetentionTimeStepBuilder builder() {
        return new Steps();
    }

    public interface RetentionTimeStepBuilder {
        CleanupFrequencyStepBuilder retentionTime(@NonNull Duration duration);
    }

    public interface CleanupFrequencyStepBuilder {
        FinalStepBuilder cleanupFrequency(@NonNull CleanupFrequency cleanupFrequency);
    }

    public interface FinalStepBuilder {
        EventTopicConfiguration build();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class Steps implements
            RetentionTimeStepBuilder,
            CleanupFrequencyStepBuilder,
            FinalStepBuilder {

        private Duration retentionTime;
        private CleanupFrequency cleanupFrequency;

        @Override
        public CleanupFrequencyStepBuilder retentionTime(@NonNull Duration duration) {
            retentionTime = duration;
            return this;
        }

        @Override
        public FinalStepBuilder cleanupFrequency(@NonNull CleanupFrequency cleanupFrequency) {
            this.cleanupFrequency = cleanupFrequency;
            return this;
        }

        @Override
        public EventTopicConfiguration build() {
            return new EventTopicConfiguration(
                    retentionTime,
                    cleanupFrequency
            );
        }
    }

}

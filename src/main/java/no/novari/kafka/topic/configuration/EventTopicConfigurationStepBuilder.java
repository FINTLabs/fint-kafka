package no.novari.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.time.Duration;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class EventTopicConfigurationStepBuilder {


    public static PartitionStep firstStep() {
        return new Steps();
    }


    public interface PartitionStep {
        RetentionTimeStep partitions(int partitions);
    }


    public interface RetentionTimeStep {
        CleanupFrequencyStep retentionTime(@NonNull Duration duration);
    }


    public interface CleanupFrequencyStep {
        BuildStep cleanupFrequency(@NonNull EventCleanupFrequency cleanupFrequency);
    }


    public interface BuildStep {
        EventTopicConfiguration build();
    }


    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class Steps implements
            PartitionStep,
            RetentionTimeStep,
            CleanupFrequencyStep,
            BuildStep {

        private int partitions;
        private Duration retentionTime;
        private EventCleanupFrequency cleanupFrequency;


        @Override
        public RetentionTimeStep partitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        @Override
        public CleanupFrequencyStep retentionTime(@NonNull Duration duration) {
            retentionTime = duration;
            return this;
        }

        @Override
        public BuildStep cleanupFrequency(@NonNull EventCleanupFrequency cleanupFrequency) {
            this.cleanupFrequency = cleanupFrequency;
            return this;
        }

        @Override
        public EventTopicConfiguration build() {
            return new EventTopicConfiguration(
                    partitions,
                    retentionTime,
                    cleanupFrequency
            );
        }
    }

}

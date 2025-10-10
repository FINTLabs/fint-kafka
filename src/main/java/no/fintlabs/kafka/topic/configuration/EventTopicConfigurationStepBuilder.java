package no.fintlabs.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.time.Duration;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class EventTopicConfigurationStepBuilder {


    public static PartitionStepBuilder builder() {
        return new Steps();
    }


    public interface PartitionStepBuilder {
        EventTopicConfigurationStepBuilder.RetentionTimeStepBuilder partitions(int partitions);
    }


    public interface RetentionTimeStepBuilder {
        CleanupFrequencyStepBuilder retentionTime(@NonNull Duration duration);
    }


    public interface CleanupFrequencyStepBuilder {
        FinalStepBuilder cleanupFrequency(@NonNull EventCleanupFrequency cleanupFrequency);
    }


    public interface FinalStepBuilder {
        EventTopicConfiguration build();
    }


    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class Steps implements
            PartitionStepBuilder,
            RetentionTimeStepBuilder,
            CleanupFrequencyStepBuilder,
            FinalStepBuilder {

        private int partitions;
        private Duration retentionTime;
        private EventCleanupFrequency cleanupFrequency;


        @Override
        public RetentionTimeStepBuilder partitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        @Override
        public CleanupFrequencyStepBuilder retentionTime(@NonNull Duration duration) {
            retentionTime = duration;
            return this;
        }

        @Override
        public FinalStepBuilder cleanupFrequency(@NonNull EventCleanupFrequency cleanupFrequency) {
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

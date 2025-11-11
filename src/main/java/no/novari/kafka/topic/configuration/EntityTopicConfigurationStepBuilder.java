package no.novari.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.time.Duration;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class EntityTopicConfigurationStepBuilder {


    public static PartitionStepBuilder builder() {
        return new Steps();
    }


    public interface PartitionStepBuilder {
        LastValueRetentionTimeStepBuilder partitions(int partitions);
    }


    public interface LastValueRetentionTimeStepBuilder {

        NullValueRetentionTimeStepBuilder lastValueRetainedForever();

        NullValueRetentionTimeStepBuilder lastValueRetentionTime(@NonNull Duration duration);
    }


    public interface NullValueRetentionTimeStepBuilder {
        CleanupFrequencyStepBuilder nullValueRetentionTime(@NonNull Duration duration);
    }


    public interface CleanupFrequencyStepBuilder {
        FinalStepBuilder cleanupFrequency(@NonNull EntityCleanupFrequency entityCleanupFrequency);
    }

    public interface FinalStepBuilder {
        EntityTopicConfiguration build();
    }


    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class Steps implements
            PartitionStepBuilder,
            LastValueRetentionTimeStepBuilder,
            NullValueRetentionTimeStepBuilder,
            CleanupFrequencyStepBuilder,
            FinalStepBuilder {

        private int partitions;
        private Duration lastValueRetentionTime;
        private Duration nullValueRetentionTime;
        private EntityCleanupFrequency cleanupFrequency;

        @Override
        public LastValueRetentionTimeStepBuilder partitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        @Override
        public NullValueRetentionTimeStepBuilder lastValueRetainedForever() {
            return this;
        }

        @Override
        public NullValueRetentionTimeStepBuilder lastValueRetentionTime(@NonNull Duration duration) {
            lastValueRetentionTime = duration;
            return this;
        }

        @Override
        public CleanupFrequencyStepBuilder nullValueRetentionTime(@NonNull Duration duration) {
            nullValueRetentionTime = duration;
            return this;
        }

        @Override
        public FinalStepBuilder cleanupFrequency(@NonNull EntityCleanupFrequency cleanupFrequency) {
            this.cleanupFrequency = cleanupFrequency;
            return this;
        }

        @Override
        public EntityTopicConfiguration build() {
            return new EntityTopicConfiguration(
                    partitions,
                    lastValueRetentionTime,
                    nullValueRetentionTime,
                    cleanupFrequency
            );
        }

    }
}

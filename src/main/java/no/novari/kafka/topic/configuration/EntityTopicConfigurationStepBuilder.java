package no.novari.kafka.topic.configuration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.lang.NonNull;

import java.time.Duration;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class EntityTopicConfigurationStepBuilder {


    public static PartitionStep firstStep() {
        return new Steps();
    }


    public interface PartitionStep {
        LastValueRetentionTimeStep partitions(int partitions);
    }


    public interface LastValueRetentionTimeStep {

        NullValueRetentionTimeStep lastValueRetainedForever();

        NullValueRetentionTimeStep lastValueRetentionTime(@NonNull Duration duration);
    }


    public interface NullValueRetentionTimeStep {
        CleanupFrequencyStep nullValueRetentionTime(@NonNull Duration duration);
    }


    public interface CleanupFrequencyStep {
        BuildStep cleanupFrequency(@NonNull EntityCleanupFrequency entityCleanupFrequency);
    }

    public interface BuildStep {
        EntityTopicConfiguration build();
    }


    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class Steps implements
            PartitionStep,
            LastValueRetentionTimeStep,
            NullValueRetentionTimeStep,
            CleanupFrequencyStep,
            BuildStep {

        private int partitions;
        private Duration lastValueRetentionTime;
        private Duration nullValueRetentionTime;
        private EntityCleanupFrequency cleanupFrequency;

        @Override
        public LastValueRetentionTimeStep partitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        @Override
        public NullValueRetentionTimeStep lastValueRetainedForever() {
            return this;
        }

        @Override
        public NullValueRetentionTimeStep lastValueRetentionTime(@NonNull Duration duration) {
            lastValueRetentionTime = duration;
            return this;
        }

        @Override
        public CleanupFrequencyStep nullValueRetentionTime(@NonNull Duration duration) {
            nullValueRetentionTime = duration;
            return this;
        }

        @Override
        public BuildStep cleanupFrequency(@NonNull EntityCleanupFrequency cleanupFrequency) {
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

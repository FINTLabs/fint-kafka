package no.fintlabs.kafka.entity.topic;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import no.fintlabs.kafka.common.topic.configuration.values.CleanupFrequency;
import no.fintlabs.kafka.common.topic.configuration.values.NumberOfMessages;
import org.springframework.util.unit.DataSize;

import java.time.Duration;

public final class EntityTopicConfigurationBuilder {

    private EntityTopicConfigurationBuilder() {
    }

    public static LastValueRetentionTimeStepBuilder builder() {
        return new Steps();
    }

    public interface LastValueRetentionTimeStepBuilder {
        NullValueRetentionTimeStepBuilder lastValueRetentionTimeDefault();

        NullValueRetentionTimeStepBuilder lastValueRetainedForever();

        NullValueRetentionTimeStepBuilder lastValueRetentionTime(Duration duration);
    }

    public interface NullValueRetentionTimeStepBuilder {

        CleanupFrequencyStepBuilder nullValueRetentionTimeDefault();

        CleanupFrequencyStepBuilder nullValueRetentionTime(Duration duration);
    }

    public interface CleanupFrequencyStepBuilder {
        MaxSegmentSizeStepBuilder cleanupFrequencyDefault();

        MaxSegmentSizeStepBuilder cleanupFrequency(CleanupFrequency cleanupFrequency);
    }

    public interface MaxSegmentSizeStepBuilder {

        FinalStepBuilder segmentSizeDefault();

        FinalStepBuilder contentDependentMaxSegmentSize(
                NumberOfMessages numberOfActiveEntities,
                DataSize averageEntitySize
        );
    }

    public interface FinalStepBuilder {
        EntityTopicConfiguration build();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class Steps implements
            LastValueRetentionTimeStepBuilder,
            NullValueRetentionTimeStepBuilder,
            CleanupFrequencyStepBuilder,
            MaxSegmentSizeStepBuilder,
            FinalStepBuilder {
        private Duration lastValueRetentionTime;
        private Duration nullValueRetentionTime;
        private CleanupFrequency cleanupFrequency;
        private DataSize maxSegmentSize;


        public NullValueRetentionTimeStepBuilder lastValueRetentionTimeDefault() {
            return this;
        }

        public NullValueRetentionTimeStepBuilder lastValueRetainedForever() {
            return this;
        }

        public NullValueRetentionTimeStepBuilder lastValueRetentionTime(Duration duration) {
            lastValueRetentionTime = duration;
            return this;
        }

        public CleanupFrequencyStepBuilder nullValueRetentionTimeDefault() {
            return this;
        }

        public CleanupFrequencyStepBuilder nullValueRetentionTime(Duration duration) {
            nullValueRetentionTime = duration;
            return this;
        }

        public MaxSegmentSizeStepBuilder cleanupFrequencyDefault() {
            return this;
        }

        public MaxSegmentSizeStepBuilder cleanupFrequency(CleanupFrequency cleanupFrequency) {
            this.cleanupFrequency = cleanupFrequency;
            return this;
        }

        public FinalStepBuilder segmentSizeDefault() {
            return this;
        }

        public FinalStepBuilder contentDependentMaxSegmentSize(
                NumberOfMessages numberOfActiveEntities,
                DataSize averageEntitySize
        ) {
            maxSegmentSize = DataSize.ofBytes(
                    averageEntitySize.toBytes() * numberOfActiveEntities.getMaxMessages()
            );
            return this;
        }

        public EntityTopicConfiguration build() {
            return new EntityTopicConfiguration(
                    lastValueRetentionTime,
                    nullValueRetentionTime,
                    cleanupFrequency,
                    maxSegmentSize
            );
        }
    }
}

package no.fintlabs.kafka.event.error.topic;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import no.fintlabs.kafka.common.topic.configuration.values.CleanupFrequency;
import no.fintlabs.kafka.common.topic.configuration.values.NumberOfMessages;
import org.springframework.util.unit.DataSize;

import java.time.Duration;

public class ErrorEventTopicConfigurationBuilder {

    private ErrorEventTopicConfigurationBuilder() {
    }

    public static RetentionTimeStepBuilder builder() {
        return new Steps();
    }

    public interface RetentionTimeStepBuilder {
        CleanupFrequencyStepBuilder retentionTime(Duration duration);
    }

    public interface CleanupFrequencyStepBuilder {
        MaxSegmentSizeStepBuilder cleanupFrequency(CleanupFrequency cleanupFrequency);
    }

    // TODO eivindmorch 23/07/2024 : Depends on cleanup frequency?
    public interface MaxSegmentSizeStepBuilder {
        FinalStepBuilder contentDependentMaxSegmentSize(NumberOfMessages averageDailyNumberOfEvents);
    }

    public interface FinalStepBuilder {
        ErrorEventTopicConfiguration build();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class Steps implements
            RetentionTimeStepBuilder,
            CleanupFrequencyStepBuilder,
            MaxSegmentSizeStepBuilder,
            FinalStepBuilder {

        private Duration retentionTime;
        private CleanupFrequency cleanupFrequency;
        private DataSize maxSegmentSize;


        @Override
        public CleanupFrequencyStepBuilder retentionTime(Duration duration) {
            retentionTime = duration;
            return this;
        }

        @Override
        public MaxSegmentSizeStepBuilder cleanupFrequency(CleanupFrequency cleanupFrequency) {
            this.cleanupFrequency = cleanupFrequency;
            return this;
        }

        // TODO eivindmorch 23/07/2024 : Depends on cleanup frequency?
        @Override
        public FinalStepBuilder contentDependentMaxSegmentSize(NumberOfMessages averageDailyNumberOfEvents) {
            maxSegmentSize = DataSize.ofKilobytes(averageDailyNumberOfEvents.getMaxMessages() * 10);
            return this;
        }

        @Override
        public ErrorEventTopicConfiguration build() {
            return new ErrorEventTopicConfiguration(
                    retentionTime,
                    cleanupFrequency,
                    maxSegmentSize
            );
        }
    }

}

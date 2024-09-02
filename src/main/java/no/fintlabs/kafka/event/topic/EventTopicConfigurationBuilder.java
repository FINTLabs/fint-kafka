package no.fintlabs.kafka.event.topic;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import no.fintlabs.kafka.common.topic.configuration.values.CleanupFrequency;
import no.fintlabs.kafka.common.topic.configuration.values.NumberOfMessages;
import org.springframework.util.unit.DataSize;

import java.time.Duration;

public class EventTopicConfigurationBuilder {

    private EventTopicConfigurationBuilder() {
    }

    public static RetentionTimeStepBuilder builder() {
        return new Steps();
    }

    public interface RetentionTimeStepBuilder {
        CleanupFrequencyStepBuilder retentionTime(Duration duration);
    }

    public interface CleanupFrequencyStepBuilder {
        FinalStepBuilder cleanupFrequency(CleanupFrequency cleanupFrequency);
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
        private DataSize maxSegmentSize;

        public CleanupFrequencyStepBuilder retentionTime(Duration duration) {
            retentionTime = duration;
            return this;
        }

        public FinalStepBuilder cleanupFrequency(CleanupFrequency cleanupFrequency) {
            this.cleanupFrequency = cleanupFrequency;
            return this;
        }

        public FinalStepBuilder contentDependentMaxSegmentSize(
                NumberOfMessages averageDailyNumberOfEvents,
                DataSize averageMessageSize
        ) {
            maxSegmentSize = DataSize.ofBytes(
                    averageDailyNumberOfEvents.getMaxMessages() * averageMessageSize.toBytes()
            );
            return this;
        }

        public EventTopicConfiguration build() {
            return new EventTopicConfiguration(
                    retentionTime,
                    cleanupFrequency,
                    maxSegmentSize
            );
        }
    }

}

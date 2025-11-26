package no.novari.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.UUID;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ListenerConfigurationStepBuilder {

    static GroupIdSuffixStep firstStep() {
        return new Steps();
    }

    public interface GroupIdSuffixStep {
        MaxPollRecordsStep groupIdApplicationDefault();

        MaxPollRecordsStep groupIdApplicationDefaultWithUniqueSuffix();

        MaxPollRecordsStep groupIdApplicationDefaultWithSuffix(String suffix);
    }

    public interface MaxPollRecordsStep {
        MaxPollIntervalStep maxPollRecordsKafkaDefault();

        MaxPollIntervalStep maxPollRecords(int numberOfRecords);
    }

    public interface MaxPollIntervalStep {
        OffsetSeekingOnAssignmentStep maxPollIntervalKafkaDefault();

        OffsetSeekingOnAssignmentStep maxPollInterval(Duration maxPollInterval);
    }

    public interface OffsetSeekingOnAssignmentStep {
        OptionalConfigsAndBuildStep seekToBeginningOnAssignment();

        OptionalConfigsAndBuildStep continueFromPreviousOffsetOnAssignment();
    }

    public interface OptionalConfigsAndBuildStep extends OffsetSeekingTriggerStep, BuildStep {
    }

    public interface OffsetSeekingTriggerStep {
        BuildStep offsetSeekingTrigger(OffsetSeekingTrigger trigger);
    }

    public interface BuildStep {
        ListenerConfiguration build();
    }


    @NoArgsConstructor
    private static class Steps implements
            GroupIdSuffixStep,
            MaxPollRecordsStep,
            MaxPollIntervalStep,
            OffsetSeekingOnAssignmentStep,
            OptionalConfigsAndBuildStep {

        private String groupIdSuffix;
        private Integer maxPollRecords;
        private Duration maxPollInterval;
        private boolean seekingOffsetOnAssignment;
        private OffsetSeekingTrigger offsetSeekingTrigger;

        @Override
        public MaxPollRecordsStep groupIdApplicationDefault() {
            return this;
        }

        @Override
        public MaxPollRecordsStep groupIdApplicationDefaultWithUniqueSuffix() {
            groupIdSuffix = UUID.randomUUID().toString();
            return this;
        }

        @Override
        public MaxPollRecordsStep groupIdApplicationDefaultWithSuffix(String suffix) {
            groupIdSuffix = suffix;
            return this;
        }

        @Override
        public MaxPollIntervalStep maxPollRecordsKafkaDefault() {
            return this;
        }

        @Override
        public MaxPollIntervalStep maxPollRecords(int numberOfRecords) {
            maxPollRecords = numberOfRecords;
            return this;
        }

        @Override
        public OffsetSeekingOnAssignmentStep maxPollIntervalKafkaDefault() {
            return this;
        }

        @Override
        public OffsetSeekingOnAssignmentStep maxPollInterval(Duration maxPollInterval) {
            this.maxPollInterval = maxPollInterval;
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep seekToBeginningOnAssignment() {
            seekingOffsetOnAssignment = true;
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep continueFromPreviousOffsetOnAssignment() {
            seekingOffsetOnAssignment = false;
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep offsetSeekingTrigger(OffsetSeekingTrigger trigger) {
            offsetSeekingTrigger = trigger;
            return this;
        }

        @Override
        public ListenerConfiguration build() {
            return new ListenerConfiguration(
                    groupIdSuffix,
                    maxPollRecords,
                    maxPollInterval,
                    seekingOffsetOnAssignment,
                    offsetSeekingTrigger
            );
        }

    }
}

package no.fintlabs.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.springframework.kafka.listener.CommonErrorHandler;

import java.time.Duration;
import java.util.UUID;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ListenerConfigurationStepBuilder {

    static <VALUE> GroupIdSuffixStep<VALUE> firstStep(Class<VALUE> consumerRecordValueClass) {
        return new Steps<>(consumerRecordValueClass);
    }

    public interface GroupIdSuffixStep<VALUE> {
        MaxPollRecordsStep<VALUE> groupIdApplicationDefault();

        MaxPollRecordsStep<VALUE> groupIdApplicationDefaultWithUniqueSuffix();

        MaxPollRecordsStep<VALUE> groupIdApplicationDefaultWithSuffix(String suffix);
    }

    public interface MaxPollRecordsStep<VALUE> {
        MaxPollIntervalStep<VALUE> maxPollRecordsKafkaDefault();

        MaxPollIntervalStep<VALUE> maxPollRecords(int numberOfRecords);
    }

    public interface MaxPollIntervalStep<VALUE> {
        ErrorHandlerStep<VALUE> maxPollIntervalKafkaDefault();

        ErrorHandlerStep<VALUE> maxPollInterval(Duration maxPollInterval);
    }

    public interface ErrorHandlerStep<VALUE> {
        OffsetSeekingOnAssignmentStep<VALUE> errorHandler(
                ErrorHandlerConfiguration<? super VALUE> errorHandlerConfiguration
        );

        OffsetSeekingOnAssignmentStep<VALUE> errorHandler(CommonErrorHandler errorHandlerConfiguration);
    }

    public interface OffsetSeekingOnAssignmentStep<VALUE> {
        OptionalConfigsAndBuildStep<VALUE> seekToBeginningOnAssignment();

        OptionalConfigsAndBuildStep<VALUE> continueFromPreviousOffsetOnAssignment();
    }

    public interface OptionalConfigsAndBuildStep<VALUE> extends OffsetSeekingTriggerStep<VALUE>, BuildStep<VALUE> {
    }

    public interface OffsetSeekingTriggerStep<VALUE> {
        BuildStep<VALUE> offsetSeekingTrigger(OffsetSeekingTrigger trigger);
    }

    public interface BuildStep<VALUE> {
        ListenerConfiguration<VALUE> build();
    }


    private static class Steps<VALUE> implements
            GroupIdSuffixStep<VALUE>,
            MaxPollRecordsStep<VALUE>,
            MaxPollIntervalStep<VALUE>,
            ErrorHandlerStep<VALUE>,
            OffsetSeekingOnAssignmentStep<VALUE>,
            OptionalConfigsAndBuildStep<VALUE> {

        private final Class<VALUE> consumerRecordValueClass;
        private String groupIdSuffix;
        private Integer maxPollRecords;
        private Duration maxPollInterval;
        private CommonErrorHandler errorHandler;
        private ErrorHandlerConfiguration<? super VALUE> errorHandlerConfiguration;
        private boolean seekingOffsetOnAssignment;
        private OffsetSeekingTrigger offsetSeekingTrigger;

        private Steps(Class<VALUE> consumerRecordValueClass) {
            this.consumerRecordValueClass = consumerRecordValueClass;
        }

        @Override
        public MaxPollRecordsStep<VALUE> groupIdApplicationDefault() {
            return this;
        }

        @Override
        public MaxPollRecordsStep<VALUE> groupIdApplicationDefaultWithUniqueSuffix() {
            groupIdSuffix = UUID.randomUUID().toString();
            return this;
        }

        @Override
        public MaxPollRecordsStep<VALUE> groupIdApplicationDefaultWithSuffix(String suffix) {
            groupIdSuffix = suffix;
            return this;
        }

        @Override
        public MaxPollIntervalStep<VALUE> maxPollRecordsKafkaDefault() {
            return this;
        }

        @Override
        public MaxPollIntervalStep<VALUE> maxPollRecords(int numberOfRecords) {
            maxPollRecords = numberOfRecords;
            return this;
        }

        @Override
        public ErrorHandlerStep<VALUE> maxPollIntervalKafkaDefault() {
            return this;
        }

        @Override
        public ErrorHandlerStep<VALUE> maxPollInterval(Duration maxPollInterval) {
            this.maxPollInterval = maxPollInterval;
            return this;
        }

        @Override
        public OffsetSeekingOnAssignmentStep<VALUE> errorHandler(ErrorHandlerConfiguration<? super VALUE> errorHandlerConfiguration) {
            this.errorHandlerConfiguration = errorHandlerConfiguration;
            return this;
        }

        @Override
        public OffsetSeekingOnAssignmentStep<VALUE> errorHandler(CommonErrorHandler errorHandler) {
            this.errorHandler = errorHandler;
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep<VALUE> seekToBeginningOnAssignment() {
            seekingOffsetOnAssignment = true;
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep<VALUE> continueFromPreviousOffsetOnAssignment() {
            seekingOffsetOnAssignment = false;
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep<VALUE> offsetSeekingTrigger(OffsetSeekingTrigger trigger) {
            offsetSeekingTrigger = trigger;
            return this;
        }

        @Override
        public ListenerConfiguration<VALUE> build() {
            return new ListenerConfiguration<>(
                    consumerRecordValueClass,
                    groupIdSuffix,
                    maxPollRecords,
                    maxPollInterval,
                    errorHandler,
                    errorHandlerConfiguration,
                    seekingOffsetOnAssignment,
                    offsetSeekingTrigger
            );
        }

    }
}

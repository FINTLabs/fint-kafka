package no.novari.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
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
        OnAssignmentStep maxPollIntervalKafkaDefault();

        OnAssignmentStep maxPollInterval(Duration maxPollInterval);
    }

    public interface OnAssignmentStep {
        OptionalConfigsAndBuildStep seekToBeginningOnAssignment();

        // TODO 05/12/2025 eivindmorch: Rename
        OptionalConfigsAndBuildStep seekToBeginningAndDoOnAssignment(
                Consumer<Map<TopicPartition, Long>> onAssignmentConsumer
        );

        OptionalConfigsAndBuildStep continueFromPreviousOffsetOnAssignment();

        // TODO 05/12/2025 eivindmorch: Rename
        OptionalConfigsAndBuildStep continueFromPreviousOffsetAndDoOnAssignment(
                Consumer<Map<TopicPartition, Long>> onAssignmentConsumer
        );

        OptionalConfigsAndBuildStep onAssignment(
                BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onAssignmentConsumer
        );
    }


    public interface OptionalConfigsAndBuildStep extends OffsetSeekingTriggerStep, BuildStep {
    }

    public interface OffsetSeekingTriggerStep {
        OptionalConfigsAndBuildStep offsetSeekingTrigger(OffsetSeekingTrigger trigger);

        OptionalConfigsAndBuildStep onRevoke(Consumer<Collection<TopicPartition>> onRevokeConsumer);
    }

    public interface BuildStep {
        ListenerConfiguration build();
    }


    @NoArgsConstructor
    private static class Steps implements
            GroupIdSuffixStep,
            MaxPollRecordsStep,
            MaxPollIntervalStep,
            OnAssignmentStep,
            OptionalConfigsAndBuildStep {

        private String groupIdSuffix;
        private Integer maxPollRecords;
        private Duration maxPollInterval;
        private BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onPartitionsAssignedConsumer;
        private Consumer<Collection<TopicPartition>> onPartitionsRevokedConsumer;
        private OffsetSeekingTrigger offsetSeekingTrigger;

        @Override
        public MaxPollRecordsStep groupIdApplicationDefault() {
            return this;
        }

        @Override
        public MaxPollRecordsStep groupIdApplicationDefaultWithUniqueSuffix() {
            groupIdSuffix = UUID
                    .randomUUID()
                    .toString();
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
        public OnAssignmentStep maxPollIntervalKafkaDefault() {
            return this;
        }

        @Override
        public OnAssignmentStep maxPollInterval(Duration maxPollInterval) {
            this.maxPollInterval = maxPollInterval;
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep seekToBeginningOnAssignment() {
            onPartitionsAssignedConsumer = this::seekToBeginning;
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep seekToBeginningAndDoOnAssignment(
                Consumer<Map<TopicPartition, Long>> onAssignmentConsumer
        ) {
            onPartitionsAssignedConsumer = (assignments, callback) -> {
                seekToBeginning(assignments, callback);
                onAssignmentConsumer.accept(assignments);
            };
            return this;
        }

        private void seekToBeginning(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
            log.debug("Seeking offset to beginning on assignments: {}", assignments);
            callback.seekToBeginning(assignments.keySet());
        }

        @Override
        public OptionalConfigsAndBuildStep continueFromPreviousOffsetOnAssignment() {
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep continueFromPreviousOffsetAndDoOnAssignment(
                Consumer<Map<TopicPartition, Long>> onAssignmentConsumer
        ) {
            onPartitionsAssignedConsumer = (assignments, callback) ->
                    onAssignmentConsumer.accept(assignments);
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep onAssignment(
                BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onAssignmentCallbackConsumer
        ) {
            onPartitionsAssignedConsumer = onAssignmentCallbackConsumer;
            return this;
        }


        @Override
        public OptionalConfigsAndBuildStep offsetSeekingTrigger(OffsetSeekingTrigger trigger) {
            offsetSeekingTrigger = trigger;
            return this;
        }

        @Override
        public OptionalConfigsAndBuildStep onRevoke(Consumer<Collection<TopicPartition>> onRevokeConsumer) {
            this.onPartitionsRevokedConsumer = onRevokeConsumer;
            return this;
        }

        @Override
        public ListenerConfiguration build() {
            return new ListenerConfiguration(
                    groupIdSuffix,
                    maxPollRecords,
                    maxPollInterval,
                    onPartitionsAssignedConsumer,
                    onPartitionsRevokedConsumer,
                    offsetSeekingTrigger
            );
        }

    }
}

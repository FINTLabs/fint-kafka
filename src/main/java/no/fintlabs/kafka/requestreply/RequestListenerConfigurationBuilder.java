package no.fintlabs.kafka.requestreply;

import java.time.Duration;

public class RequestListenerConfigurationBuilder {


    static <VALUE> MaxPollRecordsStep<VALUE> firstStep(Class<VALUE> consumerRecordValueClass) {
        return new Steps<>(consumerRecordValueClass);
    }

    public interface MaxPollRecordsStep<VALUE> {
        MaxPollIntervalStep<VALUE> maxPollRecordsKafkaDefault();

        MaxPollIntervalStep<VALUE> maxPollRecords(int numberOfRecords);
    }

    public interface MaxPollIntervalStep<VALUE> {
        BuildStep<VALUE> maxPollIntervalKafkaDefault();

        BuildStep<VALUE> maxPollInterval(Duration maxPollInterval);
    }

    public interface BuildStep<VALUE> {
        RequestListenerConfiguration<VALUE> build();
    }


    private static class Steps<VALUE> implements
            MaxPollRecordsStep<VALUE>,
            MaxPollIntervalStep<VALUE>,
            BuildStep<VALUE> {

        private final Class<VALUE> consumerRecordValueClass;
        private Integer maxPollRecords;
        private Duration maxPollInterval;

        private Steps(Class<VALUE> consumerRecordValueClass) {
            this.consumerRecordValueClass = consumerRecordValueClass;
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
        public BuildStep<VALUE> maxPollIntervalKafkaDefault() {
            return this;
        }

        @Override
        public BuildStep<VALUE> maxPollInterval(Duration maxPollInterval) {
            this.maxPollInterval = maxPollInterval;
            return this;
        }

        @Override
        public RequestListenerConfiguration<VALUE> build() {
            return new RequestListenerConfiguration<>(
                    consumerRecordValueClass,
                    maxPollRecords,
                    maxPollInterval
            );
        }

    }
}

package no.fintlabs.kafka.requestreply;

import no.fintlabs.kafka.consuming.ErrorHandlerConfiguration;
import org.springframework.kafka.listener.CommonErrorHandler;

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
        ErrorHandlerStep<VALUE> maxPollIntervalKafkaDefault();

        ErrorHandlerStep<VALUE> maxPollInterval(Duration maxPollInterval);
    }

    public interface ErrorHandlerStep<VALUE> {
        BuildStep<VALUE> errorHandler(
                ErrorHandlerConfiguration<? super VALUE> errorHandlerConfiguration
        );

        BuildStep<VALUE> errorHandler(CommonErrorHandler errorHandlerConfiguration);
    }

    public interface BuildStep<VALUE> {
        RequestListenerConfiguration<VALUE> build();
    }


    private static class Steps<VALUE> implements
            MaxPollRecordsStep<VALUE>,
            MaxPollIntervalStep<VALUE>,
            ErrorHandlerStep<VALUE>,
            BuildStep<VALUE> {

        private final Class<VALUE> consumerRecordValueClass;
        private Integer maxPollRecords;
        private Duration maxPollInterval;
        private CommonErrorHandler errorHandler;
        private ErrorHandlerConfiguration<? super VALUE> errorHandlerConfiguration;

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
        public ErrorHandlerStep<VALUE> maxPollIntervalKafkaDefault() {
            return this;
        }

        @Override
        public ErrorHandlerStep<VALUE> maxPollInterval(Duration maxPollInterval) {
            this.maxPollInterval = maxPollInterval;
            return this;
        }

        @Override
        public BuildStep<VALUE> errorHandler(ErrorHandlerConfiguration<? super VALUE> errorHandlerConfiguration) {
            this.errorHandlerConfiguration = errorHandlerConfiguration;
            return this;
        }

        @Override
        public BuildStep<VALUE> errorHandler(CommonErrorHandler errorHandler) {
            this.errorHandler = errorHandler;
            return this;
        }

        @Override
        public RequestListenerConfiguration<VALUE> build() {
            return new RequestListenerConfiguration<>(
                    consumerRecordValueClass,
                    maxPollRecords,
                    maxPollInterval,
                    errorHandler,
                    errorHandlerConfiguration
            );
        }

    }
}

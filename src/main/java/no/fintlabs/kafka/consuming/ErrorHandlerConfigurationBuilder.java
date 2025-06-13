package no.fintlabs.kafka.consuming;


import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiFunction;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
class ErrorHandlerConfigurationBuilder {

    static <VALUE> RetryStep<VALUE> builder() {
        return new Steps<>();
    }

    public interface RetryStep<VALUE> extends DefaultRetryStep<VALUE>, RetryFunctionStep<VALUE> {
    }

    public interface DefaultRetryStep<VALUE> {
        RecoveryStep<VALUE> noRetries();

        RecoveryStep<VALUE> retryWithFixedInterval(
                Duration interval,
                int maxRetries
        );

        RecoveryStep<VALUE> retryWithExponentialInterval(
                Duration initialInterval,
                double intervalMultiplier,
                Duration maxInterval,
                Duration maxElapsedTime
        );

        RecoveryStep<VALUE> retryWithExponentialInterval(
                Duration initialInterval,
                double intervalMultiplier,
                Duration maxInterval,
                int maxRetries
        );

    }

    public interface RetryFunctionStep<VALUE> {
        RetryFunctionDefaultStep<VALUE> retryWithBackoffFunction(
                BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backoffFunction
        );
    }

    public interface RetryFunctionDefaultStep<VALUE> {
        DefaultRetryStep<VALUE> orElse();
    }

    public interface RecoveryStep<VALUE> {

        BuilderStep<VALUE> logFailedRecords();

        BuilderStep<VALUE> publishFailedRecordsToDeadLetterTopic();

        BuilderStep<VALUE> stopListenerContainerOnFailedRecord();
    }

    public interface BuilderStep<VALUE> {
        ErrorHandlerConfiguration<VALUE> build();
    }

    private static class Steps<VALUE> implements
            RetryStep<VALUE>,
            DefaultRetryStep<VALUE>,
            RetryFunctionDefaultStep<VALUE>,
            RecoveryStep<VALUE>,
            BuilderStep<VALUE> {

        private BackOff defaultBackOff;
        private BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backOffFunction;
        private ErrorHandlerConfiguration.RecoveryType recoveryType;
        private ConsumerAwareRecordRecoverer customRecoverer;

        @Override
        public RecoveryStep<VALUE> noRetries() {
            return this;
        }

        @Override
        public RecoveryStep<VALUE> retryWithFixedInterval(Duration interval, int maxRetries) {
            defaultBackOff = new FixedBackOff(interval.toMillis(), maxRetries);
            return this;
        }

        @Override
        public RecoveryStep<VALUE> retryWithExponentialInterval(
                Duration initialInterval,
                double intervalMultiplier,
                Duration maxInterval,
                Duration maxElapsedTime
        ) {
            ExponentialBackOff exponentialBackOff = new ExponentialBackOff(
                    initialInterval.toMillis(),
                    intervalMultiplier
            );
            exponentialBackOff.setMaxInterval(maxInterval.toMillis());
            exponentialBackOff.setMaxElapsedTime(maxElapsedTime.toMillis());
            defaultBackOff = exponentialBackOff;
            return this;
        }

        @Override
        public RecoveryStep<VALUE> retryWithExponentialInterval(
                Duration initialInterval,
                double intervalMultiplier,
                Duration maxInterval,
                int maxRetries
        ) {
            ExponentialBackOffWithMaxRetries exponentialBackOff = new ExponentialBackOffWithMaxRetries(
                    maxRetries
            );
            exponentialBackOff.setInitialInterval(initialInterval.toMillis());
            exponentialBackOff.setMultiplier(intervalMultiplier);
            exponentialBackOff.setMaxInterval(maxInterval.toMillis());
            return this;
        }

        @Override
        public RetryFunctionDefaultStep<VALUE> retryWithBackoffFunction(
                BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backoffFunction
        ) {
            backOffFunction = backoffFunction;
            return this;
        }

        @Override
        public DefaultRetryStep<VALUE> orElse() {
            return this;
        }

        @Override
        public BuilderStep<VALUE> logFailedRecords() {
            recoveryType = ErrorHandlerConfiguration.RecoveryType.LOG;
            return this;
        }

        @Override
        public BuilderStep<VALUE> publishFailedRecordsToDeadLetterTopic() {
            recoveryType = ErrorHandlerConfiguration.RecoveryType.DEAD_LETTER;
            return this;
        }

        @Override
        public BuilderStep<VALUE> stopListenerContainerOnFailedRecord() {
            recoveryType = ErrorHandlerConfiguration.RecoveryType.STOP_LISTENER;
            return this;
        }

        @Override
        public ErrorHandlerConfiguration<VALUE> build() {
            return new ErrorHandlerConfiguration<>(
                    defaultBackOff,
                    backOffFunction,
                    recoveryType
            );
        }
    }

}

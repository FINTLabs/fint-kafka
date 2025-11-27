package no.novari.kafka.consuming;


import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.logging.log4j.util.TriConsumer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ErrorHandlerConfigurationStepBuilder {

    static <CONSUMER_RECORD> RetryStep<CONSUMER_RECORD> firstStep() {
        return new Steps<>();
    }

    public interface RetryStep<CONSUMER_RECORD> extends
            DefaultRetryStep<CONSUMER_RECORD>,
            RetryFunctionStep<CONSUMER_RECORD> {
    }


    public interface RetryFunctionStep<CONSUMER_RECORD> {
        RetryFunctionDefaultStep<CONSUMER_RECORD> retryWithBackoffFunction(
                BiFunction<CONSUMER_RECORD, Exception, Optional<BackOff>> backoffFunction
        );
    }


    public interface RetryFunctionDefaultStep<CONSUMER_RECORD> {
        DefaultRetryStep<CONSUMER_RECORD> orElse();
    }


    public interface DefaultRetryStep<CONSUMER_RECORD> {
        RecoveryStep<CONSUMER_RECORD> noRetries();

        RetryClassificationStep<CONSUMER_RECORD> retryWithFixedInterval(
                Duration interval,
                int maxRetries
        );

        RetryClassificationStep<CONSUMER_RECORD> retryWithExponentialInterval(
                Duration initialInterval,
                double intervalMultiplier,
                Duration maxInterval,
                Duration maxElapsedTime
        );

        RetryClassificationStep<CONSUMER_RECORD> retryWithExponentialInterval(
                Duration initialInterval,
                double intervalMultiplier,
                Duration maxInterval,
                int maxRetries
        );
    }


    public interface RetryClassificationStep<CONSUMER_RECORD> {
        RetryFailureChangeStep<CONSUMER_RECORD> retryOnly(Collection<Class<? extends Exception>> exceptions);

        RetryFailureChangeStep<CONSUMER_RECORD> excludeExceptionsFromRetry(
                Collection<Class<?
                        extends Exception>> exceptions
        );

        RetryFailureChangeStep<CONSUMER_RECORD> useDefaultRetryClassification();
    }


    public interface RetryFailureChangeStep<CONSUMER_RECORD> {
        RecoveryStep<CONSUMER_RECORD> restartRetryOnExceptionChange();

        RecoveryStep<CONSUMER_RECORD> continueRetryOnExceptionChange();
    }


    public interface RecoveryStep<CONSUMER_RECORD> {

        BuilderStep<CONSUMER_RECORD> skipFailedRecords();

        RecoveryFailureStep<CONSUMER_RECORD> recoverFailedRecords(
                BiConsumer<CONSUMER_RECORD, Exception> customRecoverer
        );

        RecoveryFailureStep<CONSUMER_RECORD> recoverFailedRecords(
                TriConsumer<CONSUMER_RECORD, Consumer<String, ?>, Exception> customRecoverer
        );
    }


    public interface RecoveryFailureStep<VALUE> {
        BuilderStep<VALUE> skipRecordOnRecoveryFailure();

        BuilderStep<VALUE> reprocessAndRetryRecordOnRecoveryFailure();

        BuilderStep<VALUE> reprocessRecordOnRecoveryFailure();
    }


    public interface BuilderStep<VALUE> {
        ErrorHandlerConfiguration<VALUE> build();
    }


    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class Steps<CONSUMER_RECORD> implements
            RetryStep<CONSUMER_RECORD>,
            RetryFunctionDefaultStep<CONSUMER_RECORD>,
            DefaultRetryStep<CONSUMER_RECORD>,
            RetryClassificationStep<CONSUMER_RECORD>,
            RetryFailureChangeStep<CONSUMER_RECORD>,
            RecoveryStep<CONSUMER_RECORD>,
            RecoveryFailureStep<CONSUMER_RECORD>,
            BuilderStep<CONSUMER_RECORD> {

        private BackOff defaultBackOff;
        private BiFunction<CONSUMER_RECORD, Exception, Optional<BackOff>> backOffFunction;
        private boolean restartRetryOnExceptionChange;
        private TriConsumer<CONSUMER_RECORD, Consumer<String, ?>, Exception> customRecoverer;
        private boolean skipRecordOnRecoveryFailure;
        private boolean restartRetryOnRecoveryFailure;

        private ErrorHandlerConfiguration.ClassificationType classificationType;
        private Collection<Class<? extends Exception>> classificationExceptions;


        @Override
        public RetryFunctionDefaultStep<CONSUMER_RECORD> retryWithBackoffFunction(
                BiFunction<CONSUMER_RECORD, Exception, Optional<BackOff>> backoffFunction
        ) {
            backOffFunction = backoffFunction;
            return this;
        }

        @Override
        public DefaultRetryStep<CONSUMER_RECORD> orElse() {
            return this;
        }

        @Override
        public RecoveryStep<CONSUMER_RECORD> noRetries() {
            return this;
        }

        @Override
        public RetryClassificationStep<CONSUMER_RECORD> retryWithFixedInterval(
                Duration interval,
                int maxRetries
        ) {
            defaultBackOff = new FixedBackOff(interval.toMillis(), maxRetries);
            return this;
        }

        @Override
        public RetryClassificationStep<CONSUMER_RECORD> retryWithExponentialInterval(
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
        public RetryClassificationStep<CONSUMER_RECORD> retryWithExponentialInterval(
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
            defaultBackOff = exponentialBackOff;
            return this;
        }

        @Override
        public RetryFailureChangeStep<CONSUMER_RECORD> retryOnly(Collection<Class<? extends Exception>> exceptions) {
            classificationType = ErrorHandlerConfiguration.ClassificationType.ONLY;
            classificationExceptions = exceptions;
            return this;
        }

        @Override
        public RetryFailureChangeStep<CONSUMER_RECORD> excludeExceptionsFromRetry(
                Collection<Class<?
                        extends Exception>> exceptions
        ) {
            classificationType = ErrorHandlerConfiguration.ClassificationType.EXCLUDE;
            classificationExceptions = exceptions;
            return this;
        }

        @Override
        public RetryFailureChangeStep<CONSUMER_RECORD> useDefaultRetryClassification() {
            classificationType = ErrorHandlerConfiguration.ClassificationType.DEFAULT;
            return this;
        }

        @Override
        public RecoveryStep<CONSUMER_RECORD> restartRetryOnExceptionChange() {
            this.restartRetryOnExceptionChange = true;
            return this;
        }

        @Override
        public RecoveryStep<CONSUMER_RECORD> continueRetryOnExceptionChange() {
            this.restartRetryOnExceptionChange = false;
            return this;
        }

        @Override
        public BuilderStep<CONSUMER_RECORD> skipFailedRecords() {
            return this;
        }

        @Override
        public RecoveryFailureStep<CONSUMER_RECORD> recoverFailedRecords(
                BiConsumer<CONSUMER_RECORD, Exception> customRecoverer
        ) {
            return recoverFailedRecords(
                    (consumerRecord, consumer, exception)
                            -> customRecoverer.accept(consumerRecord, exception)
            );
        }

        @Override
        public RecoveryFailureStep<CONSUMER_RECORD> recoverFailedRecords(
                TriConsumer<CONSUMER_RECORD, Consumer<String, ?>, Exception> customRecoverer
        ) {
            this.customRecoverer = customRecoverer;
            return this;
        }

        @Override
        public BuilderStep<CONSUMER_RECORD> skipRecordOnRecoveryFailure() {
            this.skipRecordOnRecoveryFailure = true;
            return this;
        }

        @Override
        public BuilderStep<CONSUMER_RECORD> reprocessAndRetryRecordOnRecoveryFailure() {
            this.restartRetryOnRecoveryFailure = true;
            return this;
        }

        @Override
        public BuilderStep<CONSUMER_RECORD> reprocessRecordOnRecoveryFailure() {
            this.restartRetryOnRecoveryFailure = false;
            return this;
        }

        @Override
        public ErrorHandlerConfiguration<CONSUMER_RECORD> build() {
            return ErrorHandlerConfiguration
                    .<CONSUMER_RECORD>builder()
                    .backOffFunction(backOffFunction)
                    .defaultBackoff(defaultBackOff)
                    .customRecoverer(customRecoverer)
                    .classificationType(classificationType)
                    .classificationExceptions(classificationExceptions)
                    .skipRecordOnRecoveryFailure(skipRecordOnRecoveryFailure)
                    .restartRetryOnExceptionChange(restartRetryOnExceptionChange)
                    .restartRetryOnRecoveryFailure(restartRetryOnRecoveryFailure)
                    .build();
        }
    }

}

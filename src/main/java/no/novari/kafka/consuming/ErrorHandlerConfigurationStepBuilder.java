package no.novari.kafka.consuming;


import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

    static <VALUE> RetryStep<VALUE> firstStep() {
        return new Steps<>();
    }


    public interface RetryStep<VALUE> extends DefaultRetryStep<VALUE>, RetryFunctionStep<VALUE> {
    }


    public interface RetryFunctionStep<VALUE> {
        RetryFunctionDefaultStep<VALUE> retryWithBackoffFunction(
                BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backoffFunction
        );
    }


    public interface RetryFunctionDefaultStep<VALUE> {
        DefaultRetryStep<VALUE> orElse();
    }


    public interface DefaultRetryStep<VALUE> {
        RecoveryStep<VALUE> noRetries();

        RetryClassificationStep<VALUE> retryWithFixedInterval(
                Duration interval,
                int maxRetries
        );

        RetryClassificationStep<VALUE> retryWithExponentialInterval(
                Duration initialInterval,
                double intervalMultiplier,
                Duration maxInterval,
                Duration maxElapsedTime
        );

        RetryClassificationStep<VALUE> retryWithExponentialInterval(
                Duration initialInterval,
                double intervalMultiplier,
                Duration maxInterval,
                int maxRetries
        );
    }


    public interface RetryClassificationStep<VALUE> {
        RetryFailureChangeStep<VALUE> retryOnly(Collection<Class<? extends Exception>> exceptions);

        RetryFailureChangeStep<VALUE> excludeExceptionsFromRetry(Collection<Class<? extends Exception>> exceptions);

        RetryFailureChangeStep<VALUE> useDefaultRetryClassification();
    }


    public interface RetryFailureChangeStep<VALUE> {
        RecoveryStep<VALUE> restartRetryOnExceptionChange();

        RecoveryStep<VALUE> continueRetryOnExceptionChange();
    }


    public interface RecoveryStep<VALUE> {

        BuilderStep<VALUE> skipFailedRecords();

        RecoveryFailureStep<VALUE> recoverFailedRecords(
                BiConsumer<ConsumerRecord<String, VALUE>, Exception> customRecoverer
        );

        RecoveryFailureStep<VALUE> recoverFailedRecords(
                TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception> customRecoverer
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


    @NoArgsConstructor
    private static class Steps<VALUE> implements
            RetryStep<VALUE>,
            RetryFunctionDefaultStep<VALUE>,
            DefaultRetryStep<VALUE>,
            RetryClassificationStep<VALUE>,
            RetryFailureChangeStep<VALUE>,
            RecoveryStep<VALUE>,
            RecoveryFailureStep<VALUE>,
            BuilderStep<VALUE> {

        private BackOff defaultBackOff;
        private BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backOffFunction;
        private boolean restartRetryOnExceptionChange;
        private TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception> customRecoverer;
        private boolean skipRecordOnRecoveryFailure;
        private boolean restartRetryOnRecoveryFailure;

        private ErrorHandlerConfiguration.ClassificationType classificationType;
        private Collection<Class<? extends Exception>> classificationExceptions;


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
        public RecoveryStep<VALUE> noRetries() {
            return this;
        }

        @Override
        public RetryClassificationStep<VALUE> retryWithFixedInterval(Duration interval, int maxRetries) {
            defaultBackOff = new FixedBackOff(interval.toMillis(), maxRetries);
            return this;
        }

        @Override
        public RetryClassificationStep<VALUE> retryWithExponentialInterval(
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
        public RetryClassificationStep<VALUE> retryWithExponentialInterval(
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
        public RetryFailureChangeStep<VALUE> retryOnly(Collection<Class<? extends Exception>> exceptions) {
            classificationType = ErrorHandlerConfiguration.ClassificationType.ONLY;
            classificationExceptions = exceptions;
            return this;
        }

        @Override
        public RetryFailureChangeStep<VALUE> excludeExceptionsFromRetry(Collection<Class<? extends Exception>> exceptions) {
            classificationType = ErrorHandlerConfiguration.ClassificationType.EXCLUDE;
            classificationExceptions = exceptions;
            return this;
        }

        @Override
        public RetryFailureChangeStep<VALUE> useDefaultRetryClassification() {
            classificationType = ErrorHandlerConfiguration.ClassificationType.DEFAULT;
            return this;
        }

        @Override
        public RecoveryStep<VALUE> restartRetryOnExceptionChange() {
            this.restartRetryOnExceptionChange = true;
            return this;
        }

        @Override
        public RecoveryStep<VALUE> continueRetryOnExceptionChange() {
            this.restartRetryOnExceptionChange = false;
            return this;
        }

        @Override
        public BuilderStep<VALUE> skipFailedRecords() {
            return this;
        }

        @Override
        public RecoveryFailureStep<VALUE> recoverFailedRecords(
                BiConsumer<ConsumerRecord<String, VALUE>, Exception> customRecoverer
        ) {
            return recoverFailedRecords(
                    (consumerRecord, consumer, exception)
                            -> customRecoverer.accept(consumerRecord, exception)
            );
        }

        @Override
        public RecoveryFailureStep<VALUE> recoverFailedRecords(
                TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception> customRecoverer
        ) {
            this.customRecoverer = customRecoverer;
            return this;
        }

        @Override
        public BuilderStep<VALUE> skipRecordOnRecoveryFailure() {
            this.skipRecordOnRecoveryFailure = true;
            return this;
        }

        @Override
        public BuilderStep<VALUE> reprocessAndRetryRecordOnRecoveryFailure() {
            this.restartRetryOnRecoveryFailure = true;
            return this;
        }

        @Override
        public BuilderStep<VALUE> reprocessRecordOnRecoveryFailure() {
            this.restartRetryOnRecoveryFailure = false;
            return this;
        }

        @Override
        public ErrorHandlerConfiguration<VALUE> build() {
            return ErrorHandlerConfiguration
                    .<VALUE>builder()
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

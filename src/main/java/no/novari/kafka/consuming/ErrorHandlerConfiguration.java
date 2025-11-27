package no.novari.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.util.TriConsumer;
import org.springframework.util.backoff.BackOff;

import java.util.Collection;
import java.util.Optional;
import java.util.function.BiFunction;

@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ErrorHandlerConfiguration<CONSUMER_RECORD> {

    public static <VALUE>
    ErrorHandlerConfigurationStepBuilder.RetryStep<ConsumerRecord<String, VALUE>> stepBuilder() {
        return ErrorHandlerConfigurationStepBuilder.firstStep();
    }

    public static <CONSUMER_RECORD>
    ErrorHandlerConfigurationStepBuilder.RetryStep<CONSUMER_RECORD> stepBuilderWithCustomRecord() {
        return ErrorHandlerConfigurationStepBuilder.firstStep();
    }

    public enum ClassificationType {
        ONLY,
        EXCLUDE,
        DEFAULT
    }

    private final BiFunction<CONSUMER_RECORD, Exception, Optional<BackOff>> backOffFunction;

    private final BackOff defaultBackoff;

    private final TriConsumer<CONSUMER_RECORD, Consumer<String, ?>, Exception> customRecoverer;

    @Getter
    @Builder.Default
    private final ClassificationType classificationType = ClassificationType.DEFAULT;

    @Getter
    private final Collection<Class<? extends Exception>> classificationExceptions;

    @Getter
    @Builder.Default
    private final boolean skipRecordOnRecoveryFailure = false;

    @Getter
    @Builder.Default
    private final boolean restartRetryOnExceptionChange = true;

    @Getter
    @Builder.Default
    private final boolean restartRetryOnRecoveryFailure = true;

    public Optional<BackOff> getDefaultBackoff() {
        return Optional.ofNullable(defaultBackoff);
    }

    public Optional<BiFunction<CONSUMER_RECORD, Exception, Optional<BackOff>>> getBackOffFunction() {
        return Optional.ofNullable(backOffFunction);
    }

    public Optional<TriConsumer<CONSUMER_RECORD, Consumer<String, ?>, Exception>> getCustomRecoverer() {
        return Optional.ofNullable(customRecoverer);
    }
}

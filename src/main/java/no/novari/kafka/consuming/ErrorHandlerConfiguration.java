package no.novari.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.util.TriConsumer;
import org.springframework.util.backoff.BackOff;

import java.util.Optional;
import java.util.function.BiFunction;

@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ErrorHandlerConfiguration<VALUE> {
    // TODO 21/11/2025 eivindmorch: Add retry classification
    public static <VALUE> ErrorHandlerConfigurationBuilder<VALUE> builder() {
        return new ErrorHandlerConfigurationBuilder<VALUE>();
    }

    public static <VALUE> ErrorHandlerConfigurationStepBuilder.RetryStep<VALUE> stepBuilder() {
        return ErrorHandlerConfigurationStepBuilder.firstStep();
    }

    private final BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backOffFunction;

    private final BackOff defaultBackoff;

    private final TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception> customRecoverer;

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

    public Optional<BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>>> getBackOffFunction() {
        return Optional.ofNullable(backOffFunction);
    }

    public Optional<TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception>> getCustomRecoverer() {
        return Optional.ofNullable(customRecoverer);
    }
}

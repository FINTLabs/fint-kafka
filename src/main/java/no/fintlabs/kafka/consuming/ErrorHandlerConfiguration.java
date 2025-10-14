package no.fintlabs.kafka.consuming;

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

@Builder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ErrorHandlerConfiguration<VALUE> {

    public static <VALUE> ErrorHandlerConfigurationBuilder<VALUE> builder(Class<VALUE> consumerRecordValueClass) {
        return new ErrorHandlerConfigurationBuilder<VALUE>().consumerRecordValueClass(consumerRecordValueClass);
    }

    public static <VALUE> ErrorHandlerConfigurationStepBuilder.RetryStep<VALUE> stepBuilder(
            Class<VALUE> consumerRecordValueClass
    ) {
        return ErrorHandlerConfigurationStepBuilder.firstStep(consumerRecordValueClass);
    }

    @Getter
    private final Class<VALUE> consumerRecordValueClass;

    private final BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backOffFunction;

    private final BackOff defaultBackoff;

    private final TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception> recoverer;

    public Optional<BackOff> getDefaultBackoff() {
        return Optional.ofNullable(defaultBackoff);
    }

    public Optional<BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>>> getBackOffFunction() {
        return Optional.ofNullable(backOffFunction);
    }

    public Optional<TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception>> getRecoverer() {
        return Optional.ofNullable(recoverer);
    }
}

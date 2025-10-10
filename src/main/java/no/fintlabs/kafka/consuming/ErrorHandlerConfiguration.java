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

    // TODO 29/09/2025 eivindmorch: Add value class as builder method input


    public static <VALUE> no.fintlabs.kafka.consuming.ErrorHandlerConfigurationBuilder.RetryStep<VALUE> stepBuilder(
            Class<VALUE> consumerRecordValueClass
    ) {
        return no.fintlabs.kafka.consuming.ErrorHandlerConfigurationBuilder.firstStep(consumerRecordValueClass);
    }

    @Getter
    private final Class<VALUE> consumerRecordValueClass;

    private final BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backOffFunction;

    @Getter
    private final BackOff defaultBackoff;

    private final TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception> recoverer;

    public Optional<BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>>> getBackOffFunction() {
        return Optional.ofNullable(backOffFunction);
    }

    public Optional<TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception>> getRecoverer() {
        return Optional.ofNullable(recoverer);
    }
}

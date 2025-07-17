package no.fintlabs.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.util.TriConsumer;
import org.springframework.util.backoff.BackOff;

import java.util.Optional;
import java.util.function.BiFunction;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ErrorHandlerConfiguration<VALUE> {

    public enum RecoveryType {
        SKIP,
        DEAD_LETTER,
        STOP_LISTENER,
        CUSTOM
    }

    public static <VALUE> ErrorHandlerConfigurationBuilder.RetryStep<VALUE> builder() {
        return ErrorHandlerConfigurationBuilder.firstStep();
    }

    private final BackOff defaultBackoff;
    private final BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backOffFunction;
    private final RecoveryType recoveryType;
    private final TriConsumer<ConsumerRecord<String, VALUE>, Consumer<String, VALUE>, Exception> customRecoverer;
}

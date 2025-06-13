package no.fintlabs.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.util.backoff.BackOff;

import java.util.Optional;
import java.util.function.BiFunction;

// TODO 16/10/2024 eivindmorch: Oppgradere til spring boot 3.3.4, java 22 og gradle >7.5 eller 8.*
// TODO 16/10/2024 eivindmorch: DLT topic må ha med consumer grp i topic navn
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ErrorHandlerConfiguration<VALUE> {

    public enum RecoveryType {
        LOG,
        DEAD_LETTER,
        STOP_LISTENER
    }

    public static <VALUE> ErrorHandlerConfigurationBuilder.RetryStep<VALUE> builder() {
        return ErrorHandlerConfigurationBuilder.builder();
    }

    private final BackOff defaultBackoff;
    private final BiFunction<ConsumerRecord<String, VALUE>, Exception, Optional<BackOff>> backOffFunction;
    private final RecoveryType recoveryType;
}

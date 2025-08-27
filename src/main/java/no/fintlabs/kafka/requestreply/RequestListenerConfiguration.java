package no.fintlabs.kafka.requestreply;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import no.fintlabs.kafka.consuming.ErrorHandlerConfiguration;
import org.springframework.kafka.listener.CommonErrorHandler;

import java.time.Duration;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class RequestListenerConfiguration<VALUE> {
    private final Class<VALUE> consumerRecordValueClass;
    private final Integer maxPollRecords;
    private final Duration maxPollInterval;
    private final CommonErrorHandler errorHandler;
    private final ErrorHandlerConfiguration<? super VALUE> errorHandlerConfiguration;

    public static <VALUE> RequestListenerConfigurationBuilder.MaxPollRecordsStep<VALUE> builder(
            Class<VALUE> consumerRecordValueClass
    ) {
        return RequestListenerConfigurationBuilder.firstStep(consumerRecordValueClass);
    }

}

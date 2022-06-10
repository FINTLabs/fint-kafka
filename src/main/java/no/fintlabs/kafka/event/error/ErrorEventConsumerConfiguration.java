package no.fintlabs.kafka.event.error;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.ListenerConfiguration;
import org.springframework.kafka.listener.CommonErrorHandler;

@Data
@Builder
public class ErrorEventConsumerConfiguration extends ListenerConfiguration {

    private final String groupIdSuffix;
    private final CommonErrorHandler errorHandler;
    private final boolean seekingOffsetResetOnAssignment;

    public static ErrorEventConsumerConfiguration empty() {
        return ErrorEventConsumerConfiguration.builder().build();
    }

}

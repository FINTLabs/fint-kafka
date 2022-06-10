package no.fintlabs.kafka.event;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.ListenerConfiguration;
import org.springframework.kafka.listener.CommonErrorHandler;

@Data
@Builder
public class EventConsumerConfiguration implements ListenerConfiguration {

    private final String groupIdSuffix;
    private final CommonErrorHandler errorHandler;
    private final boolean seekingOffsetResetOnAssignment;

    public static EventConsumerConfiguration empty() {
        return EventConsumerConfiguration.builder().build();
    }

}

package no.fintlabs.kafka.event;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.ListenerConfiguration;
import no.fintlabs.kafka.common.OffsetSeekingTrigger;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Data
@Builder
public class EventConsumerConfiguration implements ListenerConfiguration {

    private final String groupIdSuffix;
    private final DefaultErrorHandler errorHandler;
    private final boolean seekingOffsetResetOnAssignment;
    private final OffsetSeekingTrigger offsetSeekingTrigger;
    private final Integer maxPollIntervalMs;
    private final ContainerProperties.AckMode ackMode;

    public static EventConsumerConfiguration empty() {
        return EventConsumerConfiguration.builder().build();
    }

}

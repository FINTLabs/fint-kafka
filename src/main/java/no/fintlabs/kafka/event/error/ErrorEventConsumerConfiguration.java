package no.fintlabs.kafka.event.error;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.ListenerConfiguration;
import no.fintlabs.kafka.common.OffsetSeekingTrigger;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Data
@Builder
public class ErrorEventConsumerConfiguration implements ListenerConfiguration {

    private final String groupIdSuffix;
    private final DefaultErrorHandler errorHandler;
    private final boolean seekingOffsetResetOnAssignment;
    private final OffsetSeekingTrigger offsetSeekingTrigger;
    private final Integer maxPollIntervalMs;
    private final ContainerProperties.AckMode ackMode;

    public static ErrorEventConsumerConfiguration empty() {
        return ErrorEventConsumerConfiguration.builder().build();
    }

}

package no.fintlabs.kafka.requestreply;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.ListenerConfiguration;
import no.fintlabs.kafka.common.OffsetSeekingTrigger;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Data
@Builder
public class RequestConsumerConfiguration implements ListenerConfiguration {

    private final Integer maxPollIntervalMs;
    private final DefaultErrorHandler errorHandler;
    private final ContainerProperties.AckMode ackMode;
    private final Integer maxPollRecords;

    public static RequestConsumerConfiguration empty() {
        return RequestConsumerConfiguration.builder().build();
    }

    @Override
    public String getGroupIdSuffix() {
        return null;
    }

    @Override
    public boolean isSeekingOffsetResetOnAssignment() {
        return false;
    }

    @Override
    public OffsetSeekingTrigger getOffsetSeekingTrigger() {
        return null;
    }

}

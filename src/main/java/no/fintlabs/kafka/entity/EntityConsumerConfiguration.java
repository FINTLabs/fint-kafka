package no.fintlabs.kafka.entity;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.ListenerConfiguration;
import no.fintlabs.kafka.common.OffsetSeekingTrigger;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Data
@Builder
public class EntityConsumerConfiguration implements ListenerConfiguration {

    private final String groupIdSuffix;
    private final DefaultErrorHandler errorHandler;
    private final OffsetSeekingTrigger offsetSeekingTrigger;
    private final ContainerProperties.AckMode ackMode;
    private final Integer maxPollIntervalMs;
    private final Integer maxPollRecords;

    public static EntityConsumerConfiguration empty() {
        return EntityConsumerConfiguration.builder().build();
    }

    @Override
    public boolean isSeekingOffsetResetOnAssignment() {
        return true;
    }

}

package no.fintlabs.kafka.entity;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.ListenerConfiguration;
import no.fintlabs.kafka.common.OffsetSeekingTrigger;
import org.springframework.kafka.listener.CommonErrorHandler;

@Data
@Builder
public class EntityConsumerConfiguration implements ListenerConfiguration {

    private final String groupIdSuffix;
    private final CommonErrorHandler errorHandler;
    private final OffsetSeekingTrigger offsetSeekingTrigger;

    public static EntityConsumerConfiguration empty() {
        return EntityConsumerConfiguration.builder().build();
    }

    @Override
    public boolean isSeekingOffsetResetOnAssignment() {
        return true;
    }

}

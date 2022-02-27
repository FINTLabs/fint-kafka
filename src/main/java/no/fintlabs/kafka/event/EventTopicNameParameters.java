package no.fintlabs.kafka.event;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventTopicNameParameters {
    private final String orgId;
    private final String domainContext;
    private final String eventName;
}

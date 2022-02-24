package no.fintlabs.kafka.event;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventTopicNameParameters {
    public final String orgId;
    public final String domainContext;
    public final String eventName;
}

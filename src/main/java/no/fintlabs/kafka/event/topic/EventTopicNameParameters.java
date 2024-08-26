package no.fintlabs.kafka.event.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

@Data
@Builder
public class EventTopicNameParameters implements TopicNameParameters {
    private final String orgId;
    private final String domainContext;
    private final String eventName;

    @Override
    public String getTopicName() {
        return "%s.%s.event.%s".formatted(orgId, domainContext, eventName);
    }

}

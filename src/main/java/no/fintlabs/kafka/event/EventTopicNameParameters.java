package no.fintlabs.kafka.event;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.TopicNameParameters;

@Data
@Builder
public class EventTopicNameParameters implements TopicNameParameters {

    private final String orgId;
    private final String domainContext;
    private final String eventName;

    @Override
    public String toTopicName() {
        validateRequiredParameter("orgId", orgId);
        validateRequiredParameter("domainContext", domainContext);
        validateRequiredParameter("resource", eventName);
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(orgId))
                .add(formatTopicNameComponent(domainContext))
                .add("event")
                .add(validateTopicNameComponent(eventName))
                .toString();
    }

}

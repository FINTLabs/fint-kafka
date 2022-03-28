package no.fintlabs.kafka.event;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.*;

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
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("event")
                .add(validateTopicComponent(eventName))
                .toString();
    }

}

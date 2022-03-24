package no.fintlabs.kafka.event.error;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.TopicNameParameters;

@Data
@Builder
public class ErrorEventTopicNameParameters implements TopicNameParameters {

    private final String orgId;
    private final String domainContext;
    private final String errorEventName;

    @Override
    public String toTopicName() {
        validateRequiredParameter("orgId", orgId);
        validateRequiredParameter("domainContext", domainContext);
        validateRequiredParameter("resource", errorEventName);
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(orgId))
                .add(formatTopicNameComponent(domainContext))
                .add("event")
                .add("error")
                .add(validateTopicNameComponent(errorEventName))
                .toString();
    }

}

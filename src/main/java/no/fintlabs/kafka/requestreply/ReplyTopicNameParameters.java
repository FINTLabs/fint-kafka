package no.fintlabs.kafka.requestreply;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.TopicNameParameters;

@Data
@Builder
public class ReplyTopicNameParameters implements TopicNameParameters {

    private final String orgId;
    private final String domainContext;
    private final String applicationId;
    private final String resource;

    public String toTopicName() {
        validateRequiredParameter("orgId", orgId);
        validateRequiredParameter("domainContext", domainContext);
        validateRequiredParameter("applicationId", applicationId);
        validateRequiredParameter("resource", resource);
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(orgId))
                .add(formatTopicNameComponent(domainContext))
                .add("reply")
                .add(validateTopicNameComponent(applicationId))
                .add(formatTopicNameComponent(resource))
                .toString();
    }

}

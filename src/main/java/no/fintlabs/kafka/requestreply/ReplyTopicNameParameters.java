package no.fintlabs.kafka.requestreply;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.*;

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
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("reply")
                .add(validateTopicComponent(applicationId))
                .add(formatTopicComponent(resource))
                .toString();
    }

}

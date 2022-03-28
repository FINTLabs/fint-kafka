package no.fintlabs.kafka.requestreply;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

import java.util.StringJoiner;

import static no.fintlabs.kafka.common.topic.TopicComponentUtils.*;

@Data
@Builder
public class RequestTopicNameParameters implements TopicNameParameters {

    private final String orgId;
    private final String domainContext;
    private final String resource;
    private final boolean isCollection;
    private final String parameterName;

    public String toTopicName() {
        validateRequiredParameter("orgId", orgId);
        validateRequiredParameter("domainContext", domainContext);
        validateRequiredParameter("resource", resource);

        StringJoiner stringJoiner = createTopicNameJoiner()
                .add(formatTopicComponent(orgId))
                .add(formatTopicComponent(domainContext))
                .add("request")
                .add(formatTopicComponent(resource));
        if (isCollection) {
            stringJoiner.add("collection");
        }
        if (parameterName != null) {
            stringJoiner.add("by")
                    .add(validateTopicComponent(parameterName));
        }
        return stringJoiner.toString();
    }

}

package no.fintlabs.kafka.requestreply.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

@Data
@Builder
public class RequestTopicNameParameters implements TopicNameParameters {
    private final String orgId;
    private final String domainContext;
    private final String resource;
    private final boolean isCollection;
    private final String parameterName;

    @Override
    public String getTopicName() {
        return "%s.%s.request.%s.%s".formatted(orgId, domainContext, resource, parameterName);
    }

}

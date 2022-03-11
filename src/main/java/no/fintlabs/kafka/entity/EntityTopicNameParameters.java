package no.fintlabs.kafka.entity;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.TopicNameParameters;

@Data
@Builder
public class EntityTopicNameParameters implements TopicNameParameters {

    private final String orgId;
    private final String domainContext;
    private final String resource;

    @Override
    public String toTopicName() {
        validateRequiredParameter("orgId", orgId);
        validateRequiredParameter("domainContext", domainContext);
        validateRequiredParameter("resource", resource);
        return createTopicNameJoiner()
                .add(formatTopicNameComponent(orgId))
                .add(formatTopicNameComponent(domainContext))
                .add("entity")
                .add(formatTopicNameComponent(resource))
                .toString();
    }

}

package no.fintlabs.kafka.entity.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNameParameters;

@Data
@Builder
public class EntityTopicNameParameters implements TopicNameParameters {
    private final String orgId;
    private final String domainContext;
    private final String resource;
}

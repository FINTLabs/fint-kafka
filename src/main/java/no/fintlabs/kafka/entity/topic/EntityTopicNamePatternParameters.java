package no.fintlabs.kafka.entity.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNamePatternParameters;
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern;
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern;

@Data
@Builder
public class EntityTopicNamePatternParameters implements TopicNamePatternParameters {
    private final FormattedTopicComponentPattern orgId;
    private final FormattedTopicComponentPattern domainContext;
    private final FormattedTopicComponentPattern resource;
}

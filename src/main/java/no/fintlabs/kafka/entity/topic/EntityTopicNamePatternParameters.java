package no.fintlabs.kafka.entity.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNamePatternParameters;
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern;

@Data
@Builder
public class EntityTopicNamePatternParameters implements TopicNamePatternParameters {
    private final FormattedTopicComponentPattern resource;
}

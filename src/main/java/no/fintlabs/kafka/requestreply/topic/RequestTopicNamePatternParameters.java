package no.fintlabs.kafka.requestreply.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNamePatternParameters;
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern;
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern;

@Data
@Builder
public class RequestTopicNamePatternParameters implements TopicNamePatternParameters {
    private final FormattedTopicComponentPattern orgId;
    private final FormattedTopicComponentPattern domainContext;
    private final FormattedTopicComponentPattern resource;
    private final boolean isCollection;
    private final ValidatedTopicComponentPattern parameterName;
}

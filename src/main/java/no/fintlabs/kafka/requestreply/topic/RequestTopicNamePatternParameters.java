package no.fintlabs.kafka.requestreply.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern;
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern;

@Data
@Builder
public class RequestTopicNamePatternParameters {
    private final FormattedTopicComponentPattern resource;
    private final boolean isCollection;
    private final ValidatedTopicComponentPattern parameterName;
}

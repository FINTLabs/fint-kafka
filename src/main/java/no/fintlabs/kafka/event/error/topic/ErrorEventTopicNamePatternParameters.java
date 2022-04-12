package no.fintlabs.kafka.event.error.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.TopicNamePatternParameters;
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern;

@Data
@Builder
public class ErrorEventTopicNamePatternParameters implements TopicNamePatternParameters {
    private final ValidatedTopicComponentPattern errorEventName;
}

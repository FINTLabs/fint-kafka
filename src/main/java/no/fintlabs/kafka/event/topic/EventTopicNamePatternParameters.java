package no.fintlabs.kafka.event.topic;

import lombok.Builder;
import lombok.Data;
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern;

@Data
@Builder
public class EventTopicNamePatternParameters {
    private final ValidatedTopicComponentPattern eventName;
}

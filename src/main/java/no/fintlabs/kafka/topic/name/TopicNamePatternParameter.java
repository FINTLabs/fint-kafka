package no.fintlabs.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class TopicNamePatternParameter {
    private final String name;
    private final TopicNamePatternParameterPattern pattern;
}

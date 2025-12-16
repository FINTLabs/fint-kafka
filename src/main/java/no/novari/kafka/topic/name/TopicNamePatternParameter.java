package no.novari.kafka.topic.name;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@EqualsAndHashCode
@ToString
public class TopicNamePatternParameter {
    private final String name;
    private final TopicNamePatternParameterPattern pattern;
}

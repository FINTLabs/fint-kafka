package no.novari.kafka.topic.name;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@EqualsAndHashCode
@ToString
public class TopicNameParameter {
    private final String name;
    private final String value;
    private final boolean required;
}

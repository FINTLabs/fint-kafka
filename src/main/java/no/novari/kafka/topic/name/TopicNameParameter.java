package no.novari.kafka.topic.name;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@Builder
public class TopicNameParameter {
    private final String name;
    private final String value;
    private final boolean required;
}

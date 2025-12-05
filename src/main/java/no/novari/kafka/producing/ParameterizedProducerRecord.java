package no.novari.kafka.producing;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import no.novari.kafka.topic.name.TopicNameParameters;
import org.apache.kafka.common.header.Headers;

@Builder
@Getter
@EqualsAndHashCode
@ToString
public class ParameterizedProducerRecord<VALUE> {
    private final TopicNameParameters topicNameParameters;
    private final Headers headers;
    private final String key;
    private final VALUE value;
}

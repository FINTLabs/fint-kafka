package no.fintlabs.kafka.model;

import lombok.Builder;
import lombok.Getter;
import no.fintlabs.kafka.topic.name.TopicNameParameters;
import org.apache.kafka.common.header.Headers;

@Builder
@Getter
public class ParameterizedProducerRecord<V> {
    private final TopicNameParameters topicNameParameters;
    private final Headers headers;
    private final String key;
    private final V value;
}

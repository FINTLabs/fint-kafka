package no.fintlabs.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.fintlabs.kafka.topic.name.TopicNameParameters;
import org.apache.kafka.common.header.Headers;

@AllArgsConstructor
@Getter
public class ParameterizedProducerRecord<V> {
    private final TopicNameParameters topicNameParameters;
    private final Headers headers;
    private final String key;
    private final V value;
}

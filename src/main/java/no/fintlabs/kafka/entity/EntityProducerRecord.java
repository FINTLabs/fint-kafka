package no.fintlabs.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.header.Headers;

@Data
@AllArgsConstructor
public class EntityProducerRecord<V> {
    private final EntityTopicNameParameters topicNameParameters;
    private final Headers headers;
    private final String key;
    private final V value;
}

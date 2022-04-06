package no.fintlabs.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import org.apache.kafka.common.header.Headers;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EntityProducerRecord<V> {
    private EntityTopicNameParameters topicNameParameters;
    private Headers headers;
    private String key;
    private V value;
}

package no.fintlabs.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import no.fintlabs.kafka.event.topic.EventTopicNameParameters;
import org.apache.kafka.common.header.Headers;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EventProducerRecord<T> {
    private EventTopicNameParameters topicNameParameters;
    private Headers headers;
    private String key;
    private T value;
}

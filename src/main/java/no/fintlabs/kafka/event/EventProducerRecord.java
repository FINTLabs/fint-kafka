package no.fintlabs.kafka.event;

import lombok.Data;
import org.apache.kafka.common.header.internals.RecordHeaders;

@Data
public class EventProducerRecord<T> {
    private final EventTopicNameParameters topicNameParameters;
    private final RecordHeaders headers;
    private final String key;
    private final T value;

    public EventProducerRecord(EventTopicNameParameters topicNameParameters, RecordHeaders headers, T value) {
        this(topicNameParameters, headers, null, value);
    }

    public EventProducerRecord(EventTopicNameParameters topicNameParameters, RecordHeaders headers, String key, T value) {
        this.topicNameParameters = topicNameParameters;
        this.headers = headers;
        this.key = key;
        this.value = value;
    }

}

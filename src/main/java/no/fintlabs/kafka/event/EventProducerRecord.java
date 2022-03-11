package no.fintlabs.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.header.internals.RecordHeaders;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EventProducerRecord<T> {
    private EventTopicNameParameters topicNameParameters;
    private RecordHeaders headers;
    private String key;
    private T value;

//    public EventProducerRecord(EventTopicNameParameters topicNameParameters, RecordHeaders headers, T value) {
//        this(topicNameParameters, headers, null, value);
//    }
//
//    public EventProducerRecord(EventTopicNameParameters topicNameParameters, RecordHeaders headers, String key, T value) {
//        this.topicNameParameters = topicNameParameters;
//        this.headers = headers;
//        this.key = key;
//        this.value = value;
//    }

}
